#!/usr/bin/env python3
"""Deployment orchestrator for xiDIS fabric.

This script reads a JSON configuration file that describes the storage
fabric and drives a series of phases to configure storage nodes and
aggregators.  The implementation is intentionally minimal – it focuses on
configuration parsing, validation and a pluggable pipeline structure.  The
actual NVMe-oF and Opus operations are expected to be implemented later.
"""

from __future__ import annotations

import argparse
import json
import logging
import pathlib
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List

from jsonschema import Draft7Validator, ValidationError

# ---------------------------------------------------------------------------
# Configuration schema
# ---------------------------------------------------------------------------

CONFIG_SCHEMA_PATH = pathlib.Path(__file__).with_name("config_schema.json")
with CONFIG_SCHEMA_PATH.open() as _schema_file:
    CONFIG_SCHEMA: Dict[str, Any] = json.load(_schema_file)


def load_config(path: pathlib.Path) -> Dict[str, Any]:
    """Load and validate configuration file."""
    data = json.loads(path.read_text())
    validator = Draft7Validator(CONFIG_SCHEMA)
    errors = sorted(validator.iter_errors(data), key=lambda e: e.path)
    if errors:
        msgs = [f"{'/'.join(map(str, e.path))}: {e.message}" for e in errors]
        raise ValidationError("; ".join(msgs))
    return data


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

def setup_logging(verbose: bool = False) -> None:
    """Configure stdout and JSONL file logging."""
    log_dir = pathlib.Path("logs") / datetime.utcnow().strftime("%Y%m%d")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "deploy.jsonl"

    class JsonlHandler(logging.FileHandler):
        def emit(self, record: logging.LogRecord) -> None:  # type: ignore[override]
            json_record = {
                "time": datetime.utcnow().isoformat() + "Z",
                "level": record.levelname,
                "msg": record.getMessage(),
            }
            self.stream.write(json.dumps(json_record) + "\n")
            self.flush()

    root = logging.getLogger()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    console.setLevel(logging.INFO if not verbose else logging.DEBUG)

    file_handler = JsonlHandler(log_file)
    file_handler.setLevel(logging.DEBUG)

    root.addHandler(console)
    root.addHandler(file_handler)


# ---------------------------------------------------------------------------
# Pipeline skeleton
# ---------------------------------------------------------------------------

@dataclass
class Context:
    config: Dict[str, Any]
    limit: Iterable[str] | None = None
    dry_run: bool = False


def phase_precheck(ctx: Context) -> None:
    logging.info("precheck phase for hosts: %s", ctx.limit or "all")
    # Placeholder for SSH connectivity checks, hugepages, etc.


def phase_storage(ctx: Context) -> None:
    logging.info("storage_export phase")
    # Placeholder for storage node configuration.


def phase_agg(ctx: Context) -> None:
    logging.info("aggregator_connect phase")
    # Placeholder for connecting namespaces on aggregators.


def phase_raid(ctx: Context) -> None:
    logging.info("opus_raid phase")
    # Placeholder for RAID assembly.


def phase_reexport(ctx: Context) -> None:
    logging.info("reexport phase")
    # Placeholder for optional re-export.


def phase_verify(ctx: Context) -> None:
    logging.info("verify phase")
    # Placeholder for final configuration verification.


PHASES = {
    "precheck": phase_precheck,
    "storage": phase_storage,
    "agg": phase_agg,
    "raid": phase_raid,
    "reexport": phase_reexport,
    "verify": phase_verify,
}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="xiDIS deployment tool")
    parser.add_argument("--config", required=True, help="Path to fabric JSON")
    parser.add_argument("--limit", help="Comma separated list of hosts")
    parser.add_argument(
        "--phase",
        choices=list(PHASES.keys()),
        help="Run only a specific phase",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show actions only")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    parser.add_argument(
        "--teardown",
        action="store_true",
        help="Remove configuration described by JSON",
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)
    setup_logging(args.verbose)

    config = load_config(pathlib.Path(args.config))
    ctx = Context(config=config, limit=args.limit.split(",") if args.limit else None, dry_run=args.dry_run)

    if args.phase:
        PHASES[args.phase](ctx)
    else:
        for fn in PHASES.values():
            fn(ctx)

    if args.teardown:
        logging.warning("teardown mode is not yet implemented")


if __name__ == "__main__":
    main()
