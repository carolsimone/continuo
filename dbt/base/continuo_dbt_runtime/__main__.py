"""Run the worker: python -m continuo_dbt_runtime."""
import sys

from continuo_dbt_runtime.worker import main

if __name__ == "__main__":
    sys.exit(main())
