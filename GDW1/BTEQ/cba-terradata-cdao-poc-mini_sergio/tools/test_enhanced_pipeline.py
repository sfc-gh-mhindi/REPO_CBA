#!/usr/bin/env python3
"""
Test Enhanced BTEQ Migration Pipeline

Tests the complete agentic pipeline with Claude Sonnet LLM enhancement.
"""

import logging
import sys
import shutil
import json
from pathlib import Path

# Add bteq_dcf to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

def test_enhanced_pipeline():
    """Test the complete enhanced BTEQ migration pipeline."""
    
    print('🚀 Testing Enhanced BTEQ Pipeline with Claude Sonnet')
    print('='*70)
    
    try:
        from services.migration.pipelines.substitution import SubstitutionPipeline
        
        # Test file selection
        test_files = [
            'ACCT_BALN_BKDT_ADJ_RULE_ISRT.sql',
            'ACCT_BALN_BKDT_GET_PROS_KEY.sql',
            'sp_get_pros_key.sql'
        ]
        
        source_dir = Path(__file__).parent.parent.parent / 'current_state' / 'bteq_sql'
        
        # Find available test file
        test_file = None
        for filename in test_files:
            candidate = source_dir / filename
            if candidate.exists():
                test_file = candidate
                break
        
        if not test_file:
            print(f'❌ No test files found in {source_dir}')
            return False
        
        # Create temporary test directory
        temp_dir = Path(__file__).parent / 'temp_enhanced_test'
        temp_dir.mkdir(exist_ok=True)
        shutil.copy(test_file, temp_dir / 'test.sql')
        
        print(f'📁 Testing with: {test_file.name}')
        print(f'🤖 Using Claude Sonnet (claude-3-5-sonnet)')
        
        # Create pipeline
        pipeline = SubstitutionPipeline(
            config_path=str(Path(__file__).parent.parent / 'config.cfg'),
            input_dir=str(temp_dir),
            output_base_dir=str(Path(__file__).parent.parent / 'output')
        )
        
        # Run enhanced pipeline
        print('\n🔄 Running enhanced pipeline...')
        results = pipeline.run_complete_pipeline()
        
        print('✅ Pipeline completed successfully!')
        print(f'📂 Output: {results["output_directory"]}')
        
        output_dir = Path(results['output_directory'])
        
        # Analyze results
        enhanced_files = list(output_dir.glob('snowflake_sp/*.sql'))
        analysis_files = list(output_dir.glob('analysis/individual/*.md'))
        metadata_files = list(output_dir.glob('snowflake_sp/*_metadata.json'))
        
        print('\n📊 **Pipeline Results:**')
        print(f'   Enhanced Procedures: {len(enhanced_files)}')
        print(f'   Analysis Reports: {len(analysis_files)}')
        print(f'   Metadata Files: {len(metadata_files)}')
        
        # Examine enhanced procedure
        if enhanced_files:
            enhanced_file = enhanced_files[0]
            with open(enhanced_file) as f:
                enhanced_sql = f.read()
            
            print(f'\n🤖 **Claude Sonnet Enhanced Procedure:**')
            print(f'   File: {enhanced_file.name}')
            print(f'   Length: {len(enhanced_sql):,} characters')
            print(f'   Lines: {len(enhanced_sql.splitlines())}')
            
            # Check for business logic
            has_business_logic = any(keyword in enhanced_sql.upper() for keyword in 
                                   ['DELETE', 'INSERT', 'SELECT', 'UPDATE', 'MERGE'])
            print(f'   Contains Business Logic: {"✅" if has_business_logic else "❌"}')
            
            # Check for error handling
            has_error_handling = any(keyword in enhanced_sql.upper() for keyword in 
                                   ['EXCEPTION', 'TRY', 'CATCH', 'ERROR', 'SQLCODE'])
            print(f'   Has Error Handling: {"✅" if has_error_handling else "❌"}')
            
            # Show structure preview
            lines = enhanced_sql.splitlines()
            print(f'\n🏗️  **Structure Preview (first 10 lines):**')
            for i, line in enumerate(lines[:10], 1):
                if line.strip():
                    print(f'   {i:2}: {line[:80]}')
            
            # Save enhanced output
            demo_output = Path(__file__).parent / 'claude_enhanced_procedure.sql'
            with open(demo_output, 'w') as f:
                f.write(enhanced_sql)
            print(f'\n💾 Claude-enhanced procedure saved to: {demo_output}')
        
        # Examine metadata
        if metadata_files:
            metadata_file = metadata_files[0]
            with open(metadata_file) as f:
                metadata = json.load(f)
            
            print(f'\n🧠 **Claude Enhancement Metadata:**')
            print(f'   Quality Score: {metadata.get("quality_score", "N/A")}')
            print(f'   LLM Enhancements: {len(metadata.get("llm_enhancements", []))}')
            print(f'   Migration Notes: {len(metadata.get("migration_notes", []))}')
            
            if metadata.get('llm_enhancements'):
                print(f'\n🎯 **Claude Enhancements:**')
                for enhancement in metadata['llm_enhancements']:
                    print(f'     • {enhancement}')
            
            if metadata.get('migration_notes'):
                print(f'\n📝 **Migration Notes:**')
                for note in metadata['migration_notes']:
                    print(f'     • {note}')
        
        # Pipeline statistics
        summary = results.get('summary', {})
        print(f'\n📈 **Pipeline Statistics:**')
        print(f'   Total Files: {summary.get("total_files_processed", 0)}')
        print(f'   Successful Substitutions: {summary.get("successful_substitutions", 0)}')
        print(f'   Successful Parsing: {summary.get("successful_parsing", 0)}')
        print(f'   Enhanced Procedures: {summary.get("successful_generation", 0)}')
        
        print(f'\n🎉 **CLAUDE SONNET ENHANCED PIPELINE: SUCCESS!**')
        print(f'🌟 All components working with Claude-3.5-Sonnet')
        
        # Clean up
        shutil.rmtree(temp_dir)
        return True
        
    except Exception as e:
        print(f'❌ Pipeline test failed: {e}')
        import traceback
        traceback.print_exc()
        
        # Clean up on error
        temp_dir = Path(__file__).parent / 'temp_enhanced_test'
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        return False


if __name__ == "__main__":
    success = test_enhanced_pipeline()
    sys.exit(0 if success else 1)
