#!/usr/bin/env python3
"""
Test script for BTEQ to DBT Model Converter

Tests the new DBT conversion functionality with the provided example files.
Demonstrates dual LLM comparison and DBT best practices.
"""

import sys
import logging
from pathlib import Path
import json
from typing import Dict, Any

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from services.conversion.dbt.converter import DBTConverter, DBTConversionContext
from utils.logging.llm_logger import get_llm_logger

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_example_files() -> Dict[str, str]:
    """Load the example BTEQ SQL and stored procedure files."""
    files = {}
    
    # Load original BTEQ SQL (input example)
    bteq_sql_path = project_root / "references/current_state/bteq_sql/ACCT_BALN_BKDT_ADJ_RULE_ISRT.sql"
    if bteq_sql_path.exists():
        files["bteq_sql"] = bteq_sql_path.read_text()
        logger.info(f"Loaded BTEQ SQL: {len(files['bteq_sql'])} characters")
    else:
        logger.error(f"BTEQ SQL file not found: {bteq_sql_path}")
        
    # Load DBT exemplar (expected output example)  
    dbt_exemplar_path = project_root / "references/exemplar/ACCT_BALN_BKDT_ADJ_RULE_ISRT.sql"
    if dbt_exemplar_path.exists():
        files["dbt_exemplar"] = dbt_exemplar_path.read_text()
        logger.info(f"Loaded DBT exemplar: {len(files['dbt_exemplar'])} characters")
    else:
        logger.error(f"DBT exemplar file not found: {dbt_exemplar_path}")
    
    # Load a recent stored procedure output for reference
    sp_path = project_root / "output/migration_run_20250821_130217/results/substitution_run_20250821_130222/snowflake_sp/ACCT_BALN_BKDT_ADJ_RULE_ISRT.sql"
    if sp_path.exists():
        files["stored_procedure"] = sp_path.read_text()
        logger.info(f"Loaded stored procedure: {len(files['stored_procedure'])} characters")
    else:
        logger.warning(f"Stored procedure file not found: {sp_path}")
        # Use a placeholder
        files["stored_procedure"] = "-- Stored procedure reference not available"
        
    return files


def test_dbt_conversion():
    """Test the BTEQ to DBT conversion with example files."""
    logger.info("🚀 Starting BTEQ to DBT Conversion Test")
    
    # Setup LLM logging
    llm_logger = get_llm_logger()
    
    # Load example files
    files = load_example_files()
    if not files.get("bteq_sql"):
        logger.error("Cannot proceed without BTEQ SQL input")
        return
    
    # Initialize DBT converter with dual models
    models = ["claude-4-sonnet", "snowflake-llama-3.3-70b"]
    converter = DBTConverter(models=models)
    logger.info(f"Initialized DBT converter with models: {models}")
    
    # Create conversion context
    context = DBTConversionContext(
        original_bteq_sql=files["bteq_sql"],
        chosen_stored_procedure=files.get("stored_procedure", ""),
        procedure_name="ACCT_BALN_BKDT_ADJ_RULE_ISRT",
        analysis_markdown="Complex backdated adjustment logic with business day calculations"
    )
    
    try:
        # Perform DBT conversion
        logger.info("🔄 Starting dual-model DBT conversion...")
        result = converter.convert_to_dbt(context)
        
        # Display results
        print("\n" + "="*80)
        print("🎯 DBT CONVERSION RESULTS")
        print("="*80)
        
        print(f"\n📊 CONVERSION SUMMARY:")
        print(f"   • Models Used: {list(result.model_results.keys())}")
        print(f"   • Preferred Model: {result.preferred_result.model}")
        print(f"   • Quality Score: {result.preferred_result.quality_score:.3f}")
        print(f"   • Processing Time: {result.total_processing_time_ms}ms")
        print(f"   • DBT Features: {', '.join(result.preferred_result.dbt_features)}")
        
        print(f"\n📝 COMPARISON NOTES:")
        for note in result.comparison_notes:
            print(f"   • {note}")
        
        print(f"\n🚀 MIGRATION NOTES:")
        for note in result.preferred_result.migration_notes:
            print(f"   • {note}")
        
        if result.preferred_result.warnings:
            print(f"\n⚠️  WARNINGS:")
            for warning in result.preferred_result.warnings:
                print(f"   • {warning}")
        
        print(f"\n📋 GENERATED DBT MODEL:")
        print("-" * 60)
        print(result.preferred_result.dbt_sql)
        print("-" * 60)
        
        # Save results to file
        output_dir = project_root / "output" / "dbt_conversion_test"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save the preferred DBT model
        dbt_model_path = output_dir / f"{result.preferred_result.name}.sql"
        dbt_model_path.write_text(result.preferred_result.dbt_sql)
        logger.info(f"Saved DBT model to: {dbt_model_path}")
        
        # Save all model results for comparison
        for model_name, model_result in result.model_results.items():
            model_file_path = output_dir / f"{result.preferred_result.name}_{model_name}.sql"
            model_file_path.write_text(model_result.dbt_sql)
            logger.info(f"Saved {model_name} result to: {model_file_path}")
        
        # Save metadata
        metadata = {
            "conversion_summary": {
                "models_used": list(result.model_results.keys()),
                "preferred_model": result.preferred_result.model,
                "quality_score": result.preferred_result.quality_score,
                "total_time_ms": result.total_processing_time_ms,
                "dbt_features": result.preferred_result.dbt_features
            },
            "comparison_notes": result.comparison_notes,
            "migration_notes": result.preferred_result.migration_notes,
            "warnings": result.preferred_result.warnings
        }
        
        metadata_path = output_dir / "conversion_metadata.json"
        metadata_path.write_text(json.dumps(metadata, indent=2))
        logger.info(f"Saved metadata to: {metadata_path}")
        
        # Compare with exemplar if available
        if files.get("dbt_exemplar"):
            print(f"\n🎯 EXEMPLAR COMPARISON:")
            print("-" * 60)
            print("Expected DBT Model (Exemplar):")
            print(files["dbt_exemplar"][:500] + "..." if len(files["dbt_exemplar"]) > 500 else files["dbt_exemplar"])
            print("-" * 60)
            
            # Basic comparison metrics
            generated_lines = len(result.preferred_result.dbt_sql.split('\n'))
            exemplar_lines = len(files["dbt_exemplar"].split('\n'))
            
            print(f"\n📏 SIZE COMPARISON:")
            print(f"   • Generated: {generated_lines} lines, {len(result.preferred_result.dbt_sql)} chars")
            print(f"   • Exemplar:  {exemplar_lines} lines, {len(files['dbt_exemplar'])} chars")
        
        print(f"\n✅ DBT Conversion Test Completed Successfully!")
        print(f"📁 Results saved to: {output_dir}")
        
    except Exception as e:
        logger.error(f"❌ DBT conversion failed: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Main test execution."""
    print("🏗️  BTEQ to DBT Model Converter Test")
    print("="*50)
    print("Testing dual-model LLM approach for BTEQ → DBT conversion")
    print("Emphasizing DBT best practices and no-hallucination policy")
    print("="*50)
    
    test_dbt_conversion()


if __name__ == "__main__":
    main()
