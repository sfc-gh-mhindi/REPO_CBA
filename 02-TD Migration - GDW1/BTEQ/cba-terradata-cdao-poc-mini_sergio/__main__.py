#!/usr/bin/env python3
"""Allow bteq_dcf to be executed as a module with python -m bteq_dcf."""
from .main import main

if __name__ == "__main__":
    raise SystemExit(main())
