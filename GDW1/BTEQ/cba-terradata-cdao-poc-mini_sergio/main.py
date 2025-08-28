#!/usr/bin/env python3
"""
BTEQ Migration Tool - Clean Architecture Entry Point

Lightweight main entry point that delegates to service layer components.
This demonstrates the target architecture for the refactoring.
"""
import sys
from typing import Optional, List

# Clean service imports (domain-organized)
from services.cli.argument_parser import build_argument_parser
from services.cli.modes import ProcessingMode


def main(argv: Optional[List[str]] = None) -> int:
    """
    Main entry point for BTEQ migration CLI.
    
    This is now lightweight and focused only on:
    1. Parsing arguments
    2. Delegating to the appropriate service
    3. Returning exit code
    """
    
    # Parse command-line arguments
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    
    # Import CLI service (lazy import for faster startup)
    from services.cli.main_cli import BteqMigrationCLI
    
    # Create CLI instance and delegate
    cli = BteqMigrationCLI()
    return cli.run(args)


if __name__ == "__main__":
    sys.exit(main())
