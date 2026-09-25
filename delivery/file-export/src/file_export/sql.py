"""Turn a validated config plus resolved window bounds into one SELECT.

Pure functions, no I/O. Identifiers were validated by the config models; every
value is rendered as a literal here (strings quoted and escaped, booleans and
numbers bare).
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from file_export.config import Column, ExportConfig, Filter, Recipient, Scalar


@dataclass(frozen=True)
class ResolvedWindow:
    """Window bounds after keyword resolution. None means unbounded."""

    start: str | None
    end: str | None


def literal(value: Scalar | dt.date | None) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):  # before int: bool is an int subclass
        return "TRUE" if value else "FALSE"
    if isinstance(value, int | float):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def render_filter(f: Filter) -> str:
    if f.in_ is not None:
        return f"{f.column} IN ({', '.join(literal(v) for v in f.in_)})"
    if f.not_in is not None:
        # NULL NOT IN (...) is NULL, so NULLs would silently vanish; keep them
        # explicit instead of letting three-valued logic decide.
        values = ", ".join(literal(v) for v in f.not_in)
        return f"({f.column} NOT IN ({values}) OR {f.column} IS NULL)"
    if f.ilike is not None:
        return f"{f.column} ILIKE {literal(f.ilike)}"
    return f"{f.column} IS NOT NULL"


def render_window(config: ExportConfig, window: ResolvedWindow) -> list[str]:
    if config.window is None:
        return []
    column = config.window.column
    clauses = []
    if window.start is not None:
        op = ">" if config.window.start_exclusive else ">="
        clauses.append(f"{column} {op} {literal(window.start)}")
    if window.end is not None:
        clauses.append(f"{column} <= {literal(window.end)}")
    return clauses


def render_column(column: Column, window: ResolvedWindow) -> str:
    if column.expr is not None:
        expr = column.expr.replace("{range_start}", literal(window.start)).replace(
            "{range_end}", literal(window.end)
        )
        return f"{expr} AS {column.alias}"
    if column.alias:
        return f"{column.name} AS {column.alias}"
    return column.name


def recipient_filters(config: ExportConfig, recipient: Recipient) -> list[Filter]:
    """A shared export's recipient with no filters of its own is selected by
    its tenant keys. The tenant assertion runs on the result either way."""
    if recipient.filters:
        return list(recipient.filters)
    if recipient.tenant_keys and config.tenant_column:
        return [Filter(column=config.tenant_column, in_=recipient.tenant_keys)]
    return []


def build_predicates(
    config: ExportConfig, recipient: Recipient, window: ResolvedWindow
) -> list[str]:
    return [
        *(render_filter(f) for f in config.filters),
        *(render_filter(f) for f in recipient_filters(config, recipient)),
        *render_window(config, window),
    ]


def build_projection(config: ExportConfig, window: ResolvedWindow) -> list[str]:
    if not config.columns:
        return ["*"]
    return [render_column(c, window) for c in config.columns]


def build_rows_sql(
    config: ExportConfig, recipient: Recipient, window: ResolvedWindow
) -> str:
    """Every column of the rows this recipient receives. The engine
    materialises this once, checks tenants against it, and projects each
    output from it -- one snapshot for the check, the count, and every file."""
    sql = f"SELECT *\nFROM {config.source_for(recipient)}"
    predicates = build_predicates(config, recipient, window)
    if predicates:
        sql += "\nWHERE " + "\n    AND ".join(predicates)
    return sql


def build_select(
    config: ExportConfig,
    recipient: Recipient,
    window: ResolvedWindow,
    from_relation: str | None = None,
) -> str:
    """The export query: projection over `from_relation` (the materialised
    rows) or, for display, over the filtered source directly."""
    columns = ",\n    ".join(build_projection(config, window))
    if from_relation is not None:
        return f"SELECT\n    {columns}\nFROM {from_relation}"
    sql = f"SELECT\n    {columns}\nFROM {config.source_for(recipient)}"
    predicates = build_predicates(config, recipient, window)
    if predicates:
        sql += "\nWHERE " + "\n    AND ".join(predicates)
    return sql


def tenant_violation_count_sql(
    result_relation: str, tenant_column: str, tenant_keys: list[Scalar]
) -> str:
    """Rows whose tenant is not one of the recipient's keys, or is NULL."""
    keys = ", ".join(literal(k) for k in tenant_keys)
    return (
        f"SELECT COUNT(*) FROM {result_relation} "
        f"WHERE {tenant_column} IS NULL OR {tenant_column} NOT IN ({keys})"
    )


def last_sunday(today: dt.date) -> dt.date:
    """The most recent Sunday strictly before `today`."""
    days_back = (today.weekday() + 1) % 7 or 7
    return today - dt.timedelta(days=days_back)
