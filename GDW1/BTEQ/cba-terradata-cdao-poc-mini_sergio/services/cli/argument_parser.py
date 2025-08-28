"""
CLI Argument Parser

Handles command-line argument parsing and validation for the BTEQ migration tool.
"""
import argparse
from .modes import ProcessingMode


def build_argument_parser() -> argparse.ArgumentParser:
    """Build the unified CLI argument parser."""
    
    parser = argparse.ArgumentParser(
        description="BTEQ DCF - Unified BTEQ to Snowflake Migration Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # V1: Pure prescriptive (fastest, no AI)
  python main.py --input my_script.bteq --mode v1_prscrip_sp
  
  # V2: Prescriptive + Claude enhancement (RECOMMENDED)
  python main.py --input my_script.bteq --mode v2_prscrip_claude_sp
  
  # V3: Complete Claude pipeline with DBT
  python main.py --input my_script.bteq --mode v3_prscrip_claude_sp_claude_dbt
  
  # V4: Multi-model comparison (when implemented)
  python main.py --input my_script.bteq --mode v4_prscrip_claude_llama_sp
  
  # V5: Direct BTEQ to DBT (no SP generation)
  python main.py --input my_script.bteq --mode v5_claude_dbt

🎯 PROCESSING MODES:
  v1_prscrip_sp:                Pure prescriptive conversion (fastest, 0 LLM calls)
  v2_prscrip_claude_sp:         Prescriptive + Claude enhancement (RECOMMENDED, 1 LLM call)  
  v3_prscrip_claude_sp_claude_dbt: Full pipeline with DBT models (comprehensive, 2 LLM calls)
  v4_prscrip_claude_llama_sp:   Claude vs Llama comparison (highest quality, 2 LLM calls)
  v5_claude_dbt:                Direct BTEQ → DBT conversion (streamlined, 1 LLM call)
        """
    )
    
    # Required arguments
    parser.add_argument(
        '--input', '-i',
        required=True,
        help='Input BTEQ file or directory containing BTEQ files'
    )
    
    parser.add_argument(
        '--mode', '-m',
        choices=ProcessingMode.all_modes(),
        default=ProcessingMode.V2_PRSCRIP_CLAUDE_SP,
        help='Processing mode - clean pipeline selection [default: v2_prscrip_claude_sp]'
    )
    
    # Optional arguments
    parser.add_argument(
        '--output', '-o',
        default='./output',
        help='Output directory for generated files [default: ./output]'
    )
    
    # AI Model Configuration
    ai_group = parser.add_argument_group('AI Model Configuration (for agentic/hybrid modes)')
    ai_group.add_argument(
        '--model-strategy',
        choices=['quality_first', 'performance_first', 'balanced', 'cost_optimized'],
        default='balanced',
        help='Model selection strategy [default: balanced]'
    )
    
    ai_group.add_argument(
        '--multi-model',
        action='store_true',
        help='Enable multi-model consensus (compare outputs from different models)'
    )
    
    ai_group.add_argument(
        '--validation',
        action='store_true',
        default=True,
        help='Enable validation agents for syntax and performance checking [default: True]'
    )
    
    ai_group.add_argument(
        '--error-correction',
        action='store_true',
        default=True,
        help='Enable error correction agents for iterative improvement [default: True]'
    )
    
    ai_group.add_argument(
        '--complexity-target',
        choices=['low', 'medium', 'high'],
        default='medium',
        help='Target complexity for processing [default: medium]'
    )
    
    # Logging and output
    log_group = parser.add_argument_group('Logging and Output')
    log_group.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output with detailed logging'
    )
    
    log_group.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set logging level [default: INFO]'
    )
    
    log_group.add_argument(
        '--dry-run',
        action='store_true',
        help='Perform analysis only without generating output files'
    )
    
    return parser
