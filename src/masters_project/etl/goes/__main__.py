"""Entry point: ``python -m masters_project.etl.goes``."""

from masters_project.etl.goes.pipeline import main

# The guard also matters for the process pool: workers re-import this module as
# ``__mp_main__`` and must not run the ETL again.
if __name__ == "__main__":
    main()
