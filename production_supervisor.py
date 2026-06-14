"""Continuous production entry point for the Layer-0 through Layer-6A pipeline."""

from test_supervisor import (
    ENGINE_SPECS,
    NONCRITICAL_REQUIRED_OUTPUTS,
    REQUIRED_OUTPUTS,
    SMART_MONEY_STRUCTURE_WARNING_SECONDS,
    collect_supervisor_health,
    run_supervisor,
)


SMART_MONEY_ENGINE = next(
    spec for spec in ENGINE_SPECS if spec.script == "smart_money_engine.py"
)


if __name__ == "__main__":
    run_supervisor(duration=0, clean=False)
