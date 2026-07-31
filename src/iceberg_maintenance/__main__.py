"""Entry point: `python -m iceberg_maintenance`."""

import sys

from .maintenance import main

if __name__ == "__main__":
    sys.exit(main())
