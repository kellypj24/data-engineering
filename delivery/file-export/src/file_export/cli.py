"""`file-export` command line.

    file-export validate [CONFIG_DIR]
    file-export exposures (--write | --check) --manifest MANIFEST --out EXPOSURES_YML [--configs DIR]
    file-export run CONFIG (--recipient NAME | --all) --mode dry-run|select-only|execute \
        --duckdb PATH [--output-root DIR]
"""

from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path

import duckdb

from file_export import exposures
from file_export.config import ConfigError, load_config, load_configs
from file_export.engine import ExportEngine, ExportFailed, Mode


def _validate(args) -> int:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            configs = load_configs(Path(args.config_dir))
        except ConfigError as exc:
            print(f"invalid: {exc}", file=sys.stderr)
            return 1
    for w in caught:
        print(f"warning: {w.message}", file=sys.stderr)
    print(f"{len(configs)} export config(s) valid")
    return 0


def _exposures(args) -> int:
    out = Path(args.out)
    try:
        text = exposures.generate(load_configs(Path(args.configs)), Path(args.manifest))
    except (ConfigError, exposures.ExposureError) as exc:
        print(f"exposures: {exc}", file=sys.stderr)
        return 1
    if args.write:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text)
        print(f"wrote {out}")
        return 0
    if not exposures.is_current(out, text):
        print(
            f"{out} is stale: the export configs changed without regenerating it. "
            "Run `file-export exposures --write` and commit the result.",
            file=sys.stderr,
        )
        return 1
    print(f"{out} is current")
    return 0


def _run(args) -> int:
    config = load_config(Path(args.config))
    if args.mode == Mode.DRY_RUN and args.duckdb is None:
        print(
            "dry-run resolves window keywords against the warehouse; pass --duckdb",
            file=sys.stderr,
        )
        return 2
    with duckdb.connect(args.duckdb, read_only=args.mode != Mode.EXECUTE) as connection:
        engine = ExportEngine(connection, Path(args.output_root))
        names = [r.name for r in config.recipients] if args.all else [args.recipient]
        failed = False
        for name in names:
            try:
                result = engine.run(config, name, args.mode)
            except ExportFailed as exc:
                result, failed = exc.result, True
            print(f"-- {result.export} / {result.recipient}: {result.status}")
            print(f"-- window: ({result.window.start}, {result.window.end}]")
            if result.row_count is not None:
                print(f"-- rows: {result.row_count}")
            for path in result.files:
                print(f"-- wrote: {path}")
            if result.error:
                print(f"-- error: {result.error}")
            if args.mode == Mode.DRY_RUN:
                print(result.sql)
    return 1 if failed else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="file-export")
    sub = parser.add_subparsers(dest="command", required=True)

    validate = sub.add_parser("validate", help="validate every config in a directory")
    validate.add_argument("config_dir", nargs="?", default="configs")
    validate.set_defaults(func=_validate)

    exp = sub.add_parser("exposures", help="generate or check the dbt exposures file")
    action = exp.add_mutually_exclusive_group(required=True)
    action.add_argument("--write", action="store_true")
    action.add_argument("--check", action="store_true")
    exp.add_argument(
        "--manifest",
        required=True,
        help="dbt target/manifest.json (run `dbt parse` first)",
    )
    exp.add_argument(
        "--out",
        required=True,
        help="the generated exposures .yml inside the dbt project",
    )
    exp.add_argument("--configs", default="configs")
    exp.set_defaults(func=_exposures)

    run = sub.add_parser("run", help="run one export")
    run.add_argument("config")
    who = run.add_mutually_exclusive_group(required=True)
    who.add_argument("--recipient")
    who.add_argument("--all", action="store_true")
    run.add_argument("--mode", required=True, choices=[m.value for m in Mode])
    run.add_argument("--duckdb", help="path to the duckdb database")
    run.add_argument("--output-root", default="exports")
    run.set_defaults(func=_run)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
