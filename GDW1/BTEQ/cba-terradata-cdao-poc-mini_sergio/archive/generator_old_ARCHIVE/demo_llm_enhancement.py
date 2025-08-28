#!/usr/bin/env python3
"""
Demo script for LLM-enhanced BTEQ to Snowflake conversion

Demonstrates the complete agentic workflow:
1. Load BTEQ file, analysis, and basic generated procedure
2. Use Snowflake Cortex LLM to enhance the procedure
3. Show the improvements made
"""

import logging
from pathlib import Path
import json

from llm_enhanced_generator import LLMEnhancedGenerator, create_llm_generation_context
from llm_integration import create_llm_service
# Use absolute imports to fix packaging issues
try:
    from parser.tokens import ParserResult, ControlStatement, SqlBlock, ControlType
except ImportError:
    # Fallback for when running as part of package
    from parser.tokens import ParserResult, ControlStatement, SqlBlock, ControlType

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def demo_llm_enhancement():
    """Demonstrate LLM-enhanced procedure generation."""
    
    print("🤖 LLM-Enhanced BTEQ to Snowflake Conversion Demo")
    print("="*60)
    
    # Initialize LLM service
    llm_service = create_llm_service()
    enhanced_generator = LLMEnhancedGenerator()
    
    print(f"🔧 LLM Service Available: {'✅' if llm_service.available else '❌'}")
    
    # Demo with latest pipeline output if available
    output_dirs = list(Path("../../output").glob("substitution_run_*"))
    if not output_dirs:
        print("❌ No pipeline output found. Run the pipeline first.")
        return
    
    latest_output = sorted(output_dirs)[-1]
    print(f"📁 Using latest output: {latest_output.name}")
    
    # Find a processed file to demo with
    analysis_files = list((latest_output / "analysis" / "individual").glob("*.md"))
    if not analysis_files:
        print("❌ No analysis files found.")
        return
    
    demo_file = analysis_files[0]
    file_stem = demo_file.stem.replace("_analysis", "")
    
    print(f"📄 Demo file: {file_stem}")
    
    try:
        # Load the required components
        print("\n📋 Loading components...")
        
        # 1. Original BTEQ content
        original_bteq_file = latest_output / "substituted" / f"{file_stem}.sql"
        if original_bteq_file.exists():
            with open(original_bteq_file, 'r') as f:
                original_bteq = f.read()
        else:
            print(f"⚠️  Original BTEQ file not found: {original_bteq_file}")
            original_bteq = "-- Original BTEQ content not available"
        
        # 2. Analysis markdown
        with open(demo_file, 'r') as f:
            analysis_markdown = f.read()
        
        # 3. Basic generated procedure
        basic_sp_file = latest_output / "snowflake_sp" / f"{file_stem}.sql"
        if basic_sp_file.exists():
            with open(basic_sp_file, 'r') as f:
                basic_procedure = f.read()
        else:
            basic_procedure = "-- Basic procedure not available"
        
        # 4. Parser result (recreated from JSON)
        parsed_file = latest_output / "parsed" / f"{file_stem}_parsed.json"
        if parsed_file.exists():
            with open(parsed_file, 'r') as f:
                parsed_data = json.load(f)
            parser_result = recreate_parser_result(parsed_data)
        else:
            # Create minimal parser result
            parser_result = ParserResult(controls=[], sql_blocks=[])
        
        print("✅ All components loaded successfully")
        
        # Create LLM generation context
        print("\n🔗 Creating LLM generation context...")
        
        llm_context = create_llm_generation_context(
            original_bteq=original_bteq,
            analysis_markdown=analysis_markdown,
            basic_procedure=basic_procedure,
            procedure_name=file_stem.upper(),
            parser_result=parser_result
        )
        
        # Generate enhanced procedure
        print("\n🚀 Generating enhanced procedure with LLM...")
        
        enhanced_result = enhanced_generator.generate_enhanced(llm_context)
        
        # Show results
        print("\n📊 Enhancement Results:")
        print(f"   Procedure Name: {enhanced_result.name}")
        print(f"   Quality Score: {enhanced_result.quality_score:.2f}")
        print(f"   Parameters: {len(enhanced_result.parameters)}")
        print(f"   Warnings: {len(enhanced_result.warnings)}")
        
        if enhanced_result.llm_enhancements:
            print("\n✨ LLM Enhancements:")
            for enhancement in enhanced_result.llm_enhancements:
                print(f"   • {enhancement}")
        
        if enhanced_result.migration_notes:
            print("\n📝 Migration Notes:")
            for note in enhanced_result.migration_notes:
                print(f"   • {note}")
        
        # Show size comparison
        basic_lines = len(basic_procedure.splitlines())
        enhanced_lines = len(enhanced_result.sql.splitlines())
        
        print(f"\n📏 Size Comparison:")
        print(f"   Basic Procedure: {basic_lines} lines")
        print(f"   Enhanced Procedure: {enhanced_lines} lines")
        print(f"   Enhancement Ratio: {enhanced_lines/basic_lines:.1f}x")
        
        # Save enhanced demo output
        demo_output_file = Path("demo_enhanced_procedure.sql")
        with open(demo_output_file, 'w') as f:
            f.write(enhanced_result.sql)
        
        print(f"\n💾 Enhanced procedure saved to: {demo_output_file}")
        
        # Test additional LLM functions
        print("\n🔍 Testing additional LLM capabilities...")
        
        if llm_service.available:
            # Test analysis summarization
            analysis_summary = llm_service.summarize_analysis(analysis_markdown[:1000])  # Limit for demo
            print(f"   📄 Analysis Summary: {analysis_summary[:100]}...")
            
            # Test complexity assessment
            complexity = llm_service.assess_migration_complexity(original_bteq[:1000])
            print(f"   🎯 Complexity Assessment: {complexity[:100]}...")
            
            # Test key insights extraction
            insights = llm_service.extract_key_insights(
                analysis_markdown[:1000], 
                "What are the main challenges in migrating this BTEQ script?"
            )
            print(f"   💡 Key Insights: {insights[:100]}...")
        else:
            print("   ⚠️  LLM functions not available (using fallback templates)")
        
        print("\n🎉 Demo completed successfully!")
        
    except Exception as e:
        logger.error(f"Demo failed: {e}")
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()


def recreate_parser_result(parsed_data: dict) -> ParserResult:
    """Recreate ParserResult from JSON data."""
    
    controls = []
    sql_blocks = []
    
    # Recreate controls
    for ctrl_data in parsed_data.get('controls', []):
        if isinstance(ctrl_data, dict):
            try:
                ctrl_type = ControlType[ctrl_data.get('type', 'UNKNOWN')]
                controls.append(ControlStatement(
                    type=ctrl_type,
                    raw=ctrl_data.get('raw', ''),
                    args=ctrl_data.get('args'),
                    line_no=ctrl_data.get('line_no')
                ))
            except (KeyError, ValueError):
                pass
    
    # Recreate SQL blocks
    for block_data in parsed_data.get('sql_blocks', []):
        if isinstance(block_data, dict) and 'sql' in block_data:
            sql_blocks.append(SqlBlock(
                sql=block_data.get('sql', ''),
                start_line=block_data.get('start_line', 0),
                end_line=block_data.get('end_line', 0)
            ))
    
    return ParserResult(
        controls=controls,
        sql_blocks=sql_blocks
    )


if __name__ == "__main__":
    demo_llm_enhancement()
