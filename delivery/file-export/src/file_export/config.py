"""The export config contract: one YAML file per export, validated at load time.

Everything that can be wrong with a config is caught here, before any SQL runs,
on every path (dry-run, CLI, orchestrator). In particular a shared export
without tenant keys raises at parse time.

Trust model: identifiers (relations, columns, aliases) are checked against a
strict pattern because they are interpolated into SQL. Computed-column `expr`
values are raw SQL and are trusted *because the config is committed and
reviewed* -- never build a config from user input.
"""

from __future__ import annotations

import re
import warnings
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

IDENTIFIER = r"^[A-Za-z_][A-Za-z0-9_]*$"
RELATION = r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$"
NAME = r"^[a-z0-9][a-z0-9_-]*$"
DATE_KEYWORDS = {"today", "last_sunday", "max_value", "last_run_end"}

Scalar = str | int | float | bool


class ConfigError(ValueError):
    """A config file is invalid. The message names the file and the problem."""


class Kind(StrEnum):
    SHARED = "shared"  # one model, filtered per recipient; tenant keys asserted
    DEDICATED = "dedicated"  # one model per recipient, filters baked in
    AD_HOC = "ad_hoc"  # any relation, never scheduled


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Column(_Strict):
    """A selected column (`name`) or a computed one (`expr`), optionally aliased.

    `expr` may use `{range_start}` / `{range_end}`, replaced with the resolved
    window bounds as SQL literals.
    """

    name: Annotated[str, Field(pattern=IDENTIFIER)] | None = None
    expr: str | None = None
    alias: Annotated[str, Field(pattern=IDENTIFIER)] | None = None

    @model_validator(mode="after")
    def _one_source(self) -> Column:
        if (self.name is None) == (self.expr is None):
            raise ValueError("a column needs exactly one of `name` or `expr`")
        if self.expr is not None and self.alias is None:
            raise ValueError("a computed column (`expr`) needs an `alias`")
        return self


class Filter(_Strict):
    """One row filter. Exactly one operator per filter."""

    column: Annotated[str, Field(pattern=IDENTIFIER)]
    in_: list[Scalar] | None = Field(default=None, alias="in")
    not_in: list[Scalar] | None = None
    ilike: str | None = None
    not_null: Literal[True] | None = None

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    @model_validator(mode="after")
    def _one_operator(self) -> Filter:
        ops = [self.in_, self.not_in, self.ilike, self.not_null]
        if sum(op is not None for op in ops) != 1:
            raise ValueError(
                f"filter on {self.column}: exactly one of in / not_in / ilike / not_null"
            )
        if self.in_ == [] or self.not_in == []:
            raise ValueError(f"filter on {self.column}: empty value list")
        return self


class Bounds(_Strict):
    """A window bound pair. Each is a literal date/timestamp string, a date
    keyword (today, last_sunday, max_value, last_run_end), or omitted
    (unbounded on that side)."""

    start: str | None = None
    end: str | None = None


class Window(_Strict):
    """A range filter whose boundary semantics are in the key name.

    start_inclusive_end_inclusive:  [start, end]
    start_exclusive_end_inclusive:  (start, end]  -- the incremental form: the
                                    row *at* the watermark was sent last time.
    """

    column: Annotated[str, Field(pattern=IDENTIFIER)]
    start_inclusive_end_inclusive: Bounds | None = None
    start_exclusive_end_inclusive: Bounds | None = None

    @model_validator(mode="after")
    def _one_semantics(self) -> Window:
        if (self.start_inclusive_end_inclusive is None) == (
            self.start_exclusive_end_inclusive is None
        ):
            raise ValueError(
                "window: exactly one of start_inclusive_end_inclusive / "
                "start_exclusive_end_inclusive"
            )
        for value in (self.bounds.start, self.bounds.end):
            if (
                value is not None
                and value.isidentifier()
                and value not in DATE_KEYWORDS
            ):
                raise ValueError(
                    f"window: unknown date keyword {value!r}; "
                    f"expected one of {sorted(DATE_KEYWORDS)} or a literal date"
                )
        if self.bounds.end == "last_run_end":
            raise ValueError("window: last_run_end is a start bound, not an end bound")
        return self

    @property
    def bounds(self) -> Bounds:
        return self.start_inclusive_end_inclusive or self.start_exclusive_end_inclusive

    @property
    def start_exclusive(self) -> bool:
        return self.start_exclusive_end_inclusive is not None

    @property
    def incremental(self) -> bool:
        return self.bounds.start == "last_run_end"


class Output(_Strict):
    """One destination for the query's result. Several outputs share one query.

    `path` placeholders: {export}, {recipient}, {run_date}.
    `header_record` / `trailer_record` placeholders additionally: {row_count}.
    `control_file` writes `<path>.ctl` next to the data file.
    """

    path: str
    format: Literal["csv", "parquet"] = "csv"
    header: bool = True
    delimiter: str = ","
    header_record: str | None = None
    trailer_record: str | None = None
    control_file: bool = False

    @model_validator(mode="after")
    def _records_need_csv(self) -> Output:
        if self.format != "csv" and (self.header_record or self.trailer_record):
            raise ValueError("header_record / trailer_record apply to csv outputs only")
        if len(self.delimiter) != 1:
            raise ValueError("delimiter must be a single character")
        return self


class Schedule(_Strict):
    cron: str
    # Generated schedules always run in UTC; there is deliberately no timezone key.


class Recipient(_Strict):
    name: Annotated[str, Field(pattern=NAME)]
    source: Annotated[str, Field(pattern=RELATION)] | None = None  # dedicated only
    tenant_keys: list[Scalar] | None = None  # shared only
    filters: list[Filter] = []
    schedule: Schedule | None = None


class ExportConfig(_Strict):
    name: Annotated[str, Field(pattern=NAME)]
    kind: Kind
    source: Annotated[str, Field(pattern=RELATION)] | None = None
    tenant_column: Annotated[str, Field(pattern=IDENTIFIER)] | None = None
    columns: list[Column] = []
    filters: list[Filter] = []
    window: Window | None = None
    outputs: Annotated[list[Output], Field(min_length=1)]
    recipients: Annotated[list[Recipient], Field(min_length=1)]

    @model_validator(mode="after")
    def _kind_rules(self) -> ExportConfig:
        names = [r.name for r in self.recipients]
        if len(names) != len(set(names)):
            raise ValueError("recipient names must be unique")

        if self.kind is Kind.SHARED:
            if not self.source:
                raise ValueError("shared export needs a top-level `source`")
            if not self.tenant_column:
                raise ValueError(
                    "shared export needs `tenant_column`: without it a recipient "
                    "could receive another recipient's rows"
                )
            for r in self.recipients:
                if not r.tenant_keys:
                    raise ValueError(
                        f"shared export: recipient {r.name} needs `tenant_keys` "
                        "(the only tenant values it may receive)"
                    )
                if r.source:
                    raise ValueError(
                        f"shared export: recipient {r.name} cannot set `source`"
                    )

        if self.kind is Kind.DEDICATED:
            if self.source or self.tenant_column or self.filters:
                raise ValueError(
                    "dedicated export: the YAML carries delivery settings only -- "
                    "no top-level source, tenant_column, or filters"
                )
            for r in self.recipients:
                if not r.source:
                    raise ValueError(
                        f"dedicated export: recipient {r.name} needs `source`"
                    )
                if r.tenant_keys or r.filters:
                    raise ValueError(
                        f"dedicated export: recipient {r.name} has filters baked "
                        "into its model; no tenant_keys or filters"
                    )

        if self.kind is Kind.AD_HOC:
            if not self.source:
                raise ValueError("ad_hoc export needs a top-level `source`")
            if any(r.schedule for r in self.recipients):
                raise ValueError("ad_hoc exports are never scheduled")

        return self

    def source_for(self, recipient: Recipient) -> str:
        return recipient.source or self.source

    def recipient(self, name: str) -> Recipient:
        for r in self.recipients:
            if r.name == name:
                return r
        raise ConfigError(f"export {self.name}: no recipient {name!r}")

    def warnings(self) -> list[str]:
        """Legal but risky configurations, reported rather than rejected."""
        found = []
        if (
            self.window
            and self.window.incremental
            and self.window.bounds.end == "max_value"
            and (self.filters or any(r.filters for r in self.recipients))
        ):
            found.append(
                f"export {self.name}: incremental window ends at max_value, which "
                "ignores row filters -- the watermark can advance past rows a filter "
                "excluded, and they will never be sent"
            )
        return found


def load_config(path: Path) -> ExportConfig:
    """Load and validate one config. Raises ConfigError naming the file."""
    try:
        raw: Any = yaml.safe_load(path.read_text())
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path}: not valid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError(f"{path}: expected a mapping at the top level")
    try:
        config = ExportConfig.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(f"{path}: {exc}") from exc
    for message in config.warnings():
        warnings.warn(message, stacklevel=2)
    return config


def discover_configs(directory: Path) -> list[Path]:
    return sorted([*directory.rglob("*.yml"), *directory.rglob("*.yaml")])


def load_configs(directory: Path) -> list[ExportConfig]:
    """Load every config under `directory`; export names must be unique."""
    configs = [load_config(p) for p in discover_configs(directory)]
    seen: dict[str, int] = {}
    for c in configs:
        seen[c.name] = seen.get(c.name, 0) + 1
    duplicates = sorted(n for n, count in seen.items() if count > 1)
    if duplicates:
        raise ConfigError(f"{directory}: duplicate export names {duplicates}")
    return configs


def safe_identifier(value: str) -> str:
    if not re.match(IDENTIFIER, value):
        raise ConfigError(f"not a safe identifier: {value!r}")
    return value
