#!/usr/bin/env python3
"""
BTEQ Substitution CLI

Command-line interface for BTEQ variable substitution and Snowflake migration pipeline.
"""

import argparse
import logging
import sys
from pathlib import Path

from .pipeline import SubstitutionPipeline


def setup_logging(level: str = "INFO") -> None:
    """Set up logging configuration."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('bteq_substitution.log')
        ]
    )


def validate_paths(args: argparse.Namespace) -> None:
    """Validate input paths exist."""
    if not Path(args.input_dir).exists():
        raise FileNotFoundError(f"Input directory not found: {args.input_dir}")
    
    if not Path(args.config_path).exists():
        raise FileNotFoundError(f"Config file not found: {args.config_path}")


def run_substitution_pipeline(args: argparse.Namespace) -> None:
    """Run the complete substitution pipeline."""
    logger = logging.getLogger(__name__)
    
    try:
        # Validate inputs
        validate_paths(args)
        
        # Initialize pipeline
        pipeline = SubstitutionPipeline(
            config_path=args.config_path,
            input_dir=args.input_dir,
            output_base_dir=args.output_dir
        )
        
        logger.info("Starting BTEQ substitution and migration pipeline")
        logger.info(f"Input directory: {args.input_dir}")
        logger.info(f"Output directory: {args.output_dir}")
        logger.info(f"Config file: {args.config_path}")
        
        # Run pipeline
        results = pipeline.run_complete_pipeline()
        
        # Display summary
        summary = pipeline.get_pipeline_summary()
        print(summary)
        
        if args.verbose:
            print("\n=== Detailed Results ===")
            print(f"Substitution results: {results['substitution']['summary']}")
            print(f"Parsing results: {results['parsing']['summary']}")
            print(f"Generation results: {results['generation']['summary']}")
        
        logger.info("Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def run_substitution_only(args: argparse.Namespace) -> None:
    """Run only the variable substitution step."""
    logger = logging.getLogger(__name__)
    
    try:
        validate_paths(args)
        
        pipeline = SubstitutionPipeline(
            config_path=args.config_path,
            input_dir=args.input_dir,
            output_base_dir=args.output_dir
        )
        
        # Create output directory and run substitution
        output_dir = pipeline.create_timestamped_output_dir()
        results = pipeline.perform_substitution()
        
        print(f"\n=== Variable Substitution Results ===")
        print(f"Output directory: {output_dir}")
        print(f"Files processed: {results['summary']['successful_files']}/{results['summary']['total_files']}")
        print(f"Total substitutions: {results['summary']['total_substitutions']}")
        
        if results['summary']['failed_files'] > 0:
            print(f"Failed files: {results['summary']['failed_file_names']}")
        
        logger.info("Variable substitution completed")
        
    except Exception as e:
        logger.error(f"Substitution failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def show_config_mappings(args: argparse.Namespace) -> None:
    """Display the current variable mappings from config."""
    try:
        from .substitution_service import SubstitutionService
        
        service = SubstitutionService(args.config_path)
        mappings = service.get_mappings()
        
        print(f"\n=== Variable Substitution Mappings ===")
        print(f"Config file: {args.config_path}")
        print(f"Total mappings: {len(mappings)}")
        print()
        
        for mapping in mappings:
            print(f"  {mapping.old_value} → {mapping.new_value}")
        
        if not mappings:
            print("  No mappings found in config file.")
        
    except Exception as e:
        print(f"Error reading config: {e}")
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="BTEQ Variable Substitution and Snowflake Migration Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run complete pipeline
  python -m bteq_dcf.substitution.cli run-pipeline
  
  # Run only substitution
  python -m bteq_dcf.substitution.cli substitute-only
  
  # Show current mappings
  python -m bteq_dcf.substitution.cli show-mappings
  
  # Use custom paths
  python -m bteq_dcf.substitution.cli run-pipeline \\
    --input-dir /path/to/bteq/files \\
    --output-dir /path/to/output \\
    --config-path /path/to/config.cfg
        """
    )
    
    # Global arguments
    parser.add_argument(
        '--input-dir', 
        default='references/current_state/bteq_sql',
        help='Input directory containing BTEQ files (default: references/current_state/bteq_sql)'
    )
    parser.add_argument(
        '--output-dir',
        default='output', 
        help='Output base directory (default: output)'
    )
    parser.add_argument(
        '--config-path',
        default='config.cfg',
        help='Path to variable mapping config file (default: config.cfg)'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set logging level (default: INFO)'
    )
    
    # Subcommands
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Run complete pipeline
    run_parser = subparsers.add_parser(
        'run-pipeline',
        help='Run complete substitution and migration pipeline'
    )
    
    # Run substitution only
    sub_parser = subparsers.add_parser(
        'substitute-only', 
        help='Run only variable substitution step'
    )
    
    # Show mappings
    show_parser = subparsers.add_parser(
        'show-mappings',
        help='Display current variable mappings from config'
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Set up logging
    setup_logging(args.log_level)
    
    # Route to appropriate command
    if args.command == 'run-pipeline':
        run_substitution_pipeline(args)
    elif args.command == 'substitute-only':
        run_substitution_only(args)
    elif args.command == 'show-mappings':
        show_config_mappings(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
