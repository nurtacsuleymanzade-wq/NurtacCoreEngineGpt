"""Continuous production entry point for the Layer-0 through Layer-5 pipeline."""

from test_supervisor import run_supervisor


if __name__ == "__main__":
    run_supervisor(duration=0, clean=False)
