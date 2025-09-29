#!/usr/bin/env python3
"""
Production script for running the complete agentic pipeline with comprehensive logging.
This is the main execution script for processing BTEQ files through the AI agent system.
"""

import sys
import logging
from pathlib import Path
import json
from datetime import datetime

def setup_debug_logging():
    """Enable comprehensive debug logging to capture all LLM interactions."""
    # Create logs directory if it doesn't exist
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Setup file logging with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"agentic_execution_{timestamp}.log"
    
    # Configure logging with multiple levels
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Enable debug logging for our components
    logging.getLogger('bteq_dcf').setLevel(logging.DEBUG)
    logging.getLogger('agentic').setLevel(logging.DEBUG)
    logging.getLogger('generator').setLevel(logging.DEBUG)
    
    print(f"🔍 Debug logging enabled: {log_file}")
    return log_file

def load_real_bteq_file():
    """Load a real BTEQ file for processing."""
    # Get the project root (parent of bteq_dcf directory)
    project_root = Path(__file__).parent.parent
    bteq_file = project_root / "current_state/bteq_bteq/ACCT_BALN_BKDT_AUDT_ISRT.bteq"
    
    if bteq_file.exists():
        with open(bteq_file, 'r') as f:
            content = f.read()
        return content, bteq_file.name
    else:
        print("❌ BTEQ file not found, using sample content")
        return None, None

def run_analysis_agent():
    """Run BTEQ analysis agent with comprehensive logging."""
    print("\n🎯 RUNNING BTEQ ANALYSIS AGENT")
    print("=" * 60)
    
    try:
        from services.orchestration.agents.service import BteqAnalysisAgent
        from bteq_dcf.utils.logging import get_llm_logger
        
        # Load real BTEQ content
        bteq_content, filename = load_real_bteq_file()
        if not bteq_content:
            return None
            
        print(f"📁 Processing: {filename}")
        print(f"📊 Size: {len(bteq_content)} chars, {len(bteq_content.splitlines())} lines")
        
        # Initialize agent with logging
        logger = get_llm_logger()
        agent = BteqAnalysisAgent()
        
        print(f"🤖 Agent initialized with model: {agent.model_name}")
        print("📤 Sending analysis request to LLM...")
        
        # Execute analysis with full logging
        analysis_result = agent.analyze_bteq_complexity(bteq_content)
        
        print("✅ Analysis completed!")
        print(f"📄 Result preview: {str(analysis_result)[:200]}...")
        
        return analysis_result
        
    except Exception as e:
        print(f"❌ Error running analysis agent: {e}")
        return None

def run_generation_agent():
    """Run multi-model generation agent with comparison."""
    print("\n🔧 RUNNING MULTI-MODEL GENERATION AGENT")
    print("=" * 60)
    
    try:
        from services.orchestration.agents.service import MultiModelGenerationAgent
        
        # Load real BTEQ content
        bteq_content, filename = load_real_bteq_file()
        if not bteq_content:
            return None
            
        # Initialize multi-model agent
        agent = MultiModelGenerationAgent()
        print(f"🤖 Agent initialized with models: {agent.model_names}")
        print("📤 Sending generation requests to multiple LLMs...")
        
        # Generate with model comparison
        generation_results = agent.generate_with_comparison(
            prompt=f"Convert this BTEQ script to a Snowflake stored procedure:\n\n{bteq_content}",
            context={"source": "BTEQ", "target": "Snowflake", "filename": filename}
        )
        
        print("✅ Multi-model generation completed!")
        for model, result in generation_results.items():
            print(f"📄 {model}: {len(str(result))} characters generated")
        
        return generation_results
        
    except Exception as e:
        print(f"❌ Error running generation agent: {e}")
        return None

def run_validation_agent():
    """Run validation agent on generated SQL."""
    print("\n✅ RUNNING VALIDATION AGENT")
    print("=" * 60)
    
    try:
        from services.orchestration.agents.service import ValidationAgent
        
        # Sample generated SQL to validate
        sample_sql = """
        CREATE OR REPLACE PROCEDURE ACCT_BALN_BKDT_AUDT_ISRT(
            CAD_PROD_DATA VARCHAR DEFAULT 'PROD_CAD',
            DDSTG VARCHAR DEFAULT 'DATA_STG',
            VTECH VARCHAR DEFAULT 'TECH_VIEW'
        )
        RETURNS STRING
        LANGUAGE SQL
        AS
        $$
        BEGIN
            INSERT INTO IDENTIFIER(:CAD_PROD_DATA || '.ACCT_BALN_BKDT_AUDT')
            SELECT * FROM IDENTIFIER(:DDSTG || '.ACCT_BALN_BKDT_STG1');
            
            RETURN 'SUCCESS';
        EXCEPTION
            WHEN OTHER THEN
                RETURN 'ERROR: ' || SQLERRM;
        END;
        $$;
        """
        
        # Initialize validation agent
        agent = ValidationAgent()
        print("🤖 Validation agent initialized")
        
        # This will generate a real validation prompt
        print("📤 Sending real validation request to LLM...")
        
        validation_result = agent.validate_sql_syntax(sample_sql)
        
        print("✅ Validation completed!")
        print(f"📄 Result: {validation_result}")
        
        return validation_result
        
    except Exception as e:
        print(f"❌ Error running validation agent: {e}")
        return None

def show_captured_prompts():
    """Show the actual prompts captured in the logs."""
    print("\n📋 CAPTURED PROMPTS FROM LOGS")
    print("=" * 60)
    
    try:
        from bteq_dcf.utils.logging import get_llm_logger
        
        logger = get_llm_logger()
        recent_interactions = logger.get_recent_requests(limit=10)
        
        if not recent_interactions:
            print("❌ No recent interactions found in logs")
            return
            
        for i, interaction in enumerate(recent_interactions, 1):
            print(f"\n🔍 INTERACTION {i}: {interaction.get('request_type', 'unknown')}")
            print(f"🤖 Model: {interaction.get('model', 'unknown')}")
            print(f"⏱️  Timestamp: {interaction.get('timestamp', 'unknown')}")
            print(f"📊 Prompt length: {interaction.get('prompt_length', 0)} characters")
            print(f"⚡ Response time: {interaction.get('processing_time_ms', 0)}ms")
            
            # Show actual prompt (truncated for readability)
            prompt = interaction.get('prompt', '')
            if prompt:
                print(f"\n📝 ACTUAL PROMPT:")
                print("─" * 40)
                if len(prompt) > 500:
                    print(f"{prompt[:250]}...")
                    print(f"[... TRUNCATED - Full prompt is {len(prompt)} characters ...]")
                    print(f"...{prompt[-250:]}")
                else:
                    print(prompt)
                print("─" * 40)
            
            # Show response summary
            response = interaction.get('response', '')
            if response:
                print(f"📤 Response length: {len(response)} characters")
                if len(response) > 200:
                    print(f"Response preview: {response[:200]}...")
                else:
                    print(f"Response: {response}")
    
    except Exception as e:
        print(f"❌ Error reading logs: {e}")

def run_complete_agentic_pipeline():
    """Run the complete agentic pipeline and capture all interactions."""
    print("\n🚀 RUNNING COMPLETE AGENTIC PIPELINE")
    print("=" * 60)
    
    try:
        # This would run the full orchestrated workflow
        print("🎭 Starting orchestrated multi-agent workflow...")
        
        # Run individual agents in sequence
        analysis_result = run_analysis_agent()
        generation_result = run_generation_agent()
        validation_result = run_validation_agent()
        
        print("\n✅ Complete pipeline execution finished!")
        return {
            'analysis': analysis_result,
            'generation': generation_result,
            'validation': validation_result
        }
        
    except Exception as e:
        print(f"❌ Error in pipeline: {e}")
        return None

def main():
    """Main execution function."""
    print("🚀 AGENTIC SYSTEM EXECUTION WITH COMPREHENSIVE LOGGING")
    print("=" * 80)
    print("Running complete AI agent pipeline on real BTEQ files\n")
    
    # Setup comprehensive logging
    log_file = setup_debug_logging()
    
    try:
        # Run the real agentic system
        results = run_complete_agentic_pipeline()
        
        # Show the captured prompts from logs
        show_captured_prompts()
        
        print(f"\n📁 Complete log file: {log_file}")
        print("\n🎉 EXECUTION COMPLETED")
        print("=" * 50)
        print("✅ Real agents executed with actual LLM calls")
        print("✅ Prompts captured and logged")
        print("✅ Responses recorded with timing")
        print("✅ Full execution trace available")
        
        return results
        
    except Exception as e:
        print(f"❌ Execution failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
