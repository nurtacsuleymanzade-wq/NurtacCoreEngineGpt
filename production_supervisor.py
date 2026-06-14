"""Continuous production entry point through the Layer-8 candidate pipeline."""

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
OBSERVER_ENGINE = next(
    (spec for spec in ENGINE_SPECS if spec.script == "observer_engine.py"),
    None,
)
HISTORICAL_OUTCOME_ENGINE = next(
    (spec for spec in ENGINE_SPECS if spec.script == "historical_outcome_engine.py"),
    None,
)
SETUP_ENGINE = next(
    spec for spec in ENGINE_SPECS if spec.script == "setup_engine.py"
)


if __name__ == "__main__":
    run_supervisor(duration=0, clean=False)
