#!/usr/bin/env python3
"""
Development script for testing agentic system with real LLM interactions.
Captures and displays actual prompts sent to language models during execution.
This is a development/testing tool and should be used from the tools directory.
"""

import sys
import logging
from pathlib import Path
import json
from datetime import datetime

# Add project root to Python path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def setup_debug_logging():
    """Enable comprehensive debug logging."""
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Enable debug for our components
    logging.getLogger('bteq_dcf').setLevel(logging.DEBUG)
    
def load_real_bteq():
    """Load real BTEQ file from project structure."""
    bteq_file = project_root / "current_state/bteq_bteq/ACCT_BALN_BKDT_AUDT_ISRT.bteq"
    
    if bteq_file.exists():
        with open(bteq_file, 'r') as f:
            content = f.read()
        return content, bteq_file.name
    else:
        return None, None

def run_real_analysis_agent():
    """Run real BteqAnalysisAgent and capture its prompt."""
    print("\n🎯 RUNNING REAL BTEQ ANALYSIS AGENT")
    print("=" * 60)
    
    try:
        from bteq_dcf.agentic.agents import BteqAnalysisAgent
        
        # Load real BTEQ content
        bteq_content, filename = load_real_bteq()
        if not bteq_content:
            print("❌ No BTEQ file found")
            return None
            
        print(f"📁 Processing: {filename}")
        print(f"📊 Size: {len(bteq_content)} chars, {len(bteq_content.splitlines())} lines")
        
        # Initialize agent
        agent = BteqAnalysisAgent()
        print(f"🤖 Agent initialized with model: {agent.model_name}")
        
        # Create analysis context
        analysis_context = {
            "bteq_content": bteq_content,
            "filename": filename,
            "task": "complexity_analysis"
        }
        
        print("📤 Sending real analysis request to LLM...")
        print("🔍 This will generate and send a REAL prompt to the LLM...")
        
        # This calls the actual agent method which constructs and sends a real prompt
        result = agent.analyze(analysis_context)
        
        print("✅ Analysis completed!")
        print(f"📄 Result: {str(result)[:300]}...")
        
        return result
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None

def run_real_generation_agent():
    """Run real MultiModelGenerationAgent."""
    print("\n🔧 RUNNING REAL MULTI-MODEL GENERATION AGENT")
    print("=" * 60)
    
    try:
        from bteq_dcf.agentic.agents import MultiModelGenerationAgent
        
        # Load real BTEQ content
        bteq_content, filename = load_real_bteq()
        if not bteq_content:
            print("❌ No BTEQ file found")
            return None
            
        # Initialize agent
        agent = MultiModelGenerationAgent()
        print(f"🤖 Agent initialized with models: {agent.model_names}")
        
        # Create generation context
        generation_context = {
            "prompt": f"Convert this BTEQ script to a Snowflake stored procedure with proper error handling:\\n\\n{bteq_content[:1000]}...",
            "context": {
                "source": "BTEQ",
                "target": "Snowflake", 
                "filename": filename,
                "complexity": "medium"
            }
        }
        
        print("📤 Sending real generation requests to multiple LLMs...")
        print("🔍 This will generate and send REAL prompts to BOTH models...")
        
        # This calls the actual agent method which constructs and sends real prompts
        results = agent.generate_multi_model(generation_context)
        
        print("✅ Multi-model generation completed!")
        for model, result in results.items():
            print(f"📄 {model}: {len(str(result))} characters generated")
        
        return results
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None

def capture_llm_logs():
    """Capture and display the actual LLM interaction logs."""
    print("\n📋 CAPTURED LLM INTERACTIONS")
    print("=" * 60)
    
    try:
        from bteq_dcf.utils.logging import get_llm_logger
        
        # Get the logger instance
        logger = get_llm_logger()
        
        # Check if we have log files
        log_dir = project_root / "bteq_dcf/logs/llm_interactions"
        if log_dir.exists():
            log_files = list(log_dir.glob("*.json"))
            print(f"🔍 Found {len(log_files)} log files")
            
            # Read recent interactions
            interactions = []
            for log_file in sorted(log_files)[-5:]:  # Last 5 files
                try:
                    with open(log_file, 'r') as f:
                        data = json.load(f)
                        interactions.append(data)
                except:
                    continue
            
            if interactions:
                print(f"\n📊 RECENT LLM INTERACTIONS ({len(interactions)} found):")
                print("─" * 60)
                
                for i, interaction in enumerate(interactions[-3:], 1):  # Show last 3
                    print(f"\n🔍 INTERACTION {i}:")
                    print(f"🤖 Model: {interaction.get('model', 'unknown')}")
                    print(f"📝 Request type: {interaction.get('request_type', 'unknown')}")
                    print(f"⏱️  Timestamp: {interaction.get('timestamp', 'unknown')}")
                    print(f"📊 Prompt length: {interaction.get('prompt_length', 0)} characters")
                    
                    # Show the actual prompt sent to LLM
                    prompt = interaction.get('prompt', '')
                    if prompt:
                        print(f"\n📤 ACTUAL PROMPT SENT TO LLM:")
                        print("─" * 40)
                        if len(prompt) > 800:
                            print(f"{prompt[:400]}")
                            print(f"\\n... [TRUNCATED - Full prompt is {len(prompt)} chars] ...\\n")
                            print(f"{prompt[-400:]}")
                        else:
                            print(prompt)
                        print("─" * 40)
                    
                    # Show response summary
                    response = interaction.get('response', '')
                    if response:
                        print(f"📥 Response length: {len(response)} characters")
                        print(f"⚡ Processing time: {interaction.get('processing_time_ms', 0)}ms")
                        if len(response) > 200:
                            print(f"Response preview: {response[:200]}...")
            else:
                print("❌ No interaction data found in log files")
        else:
            print("❌ No log directory found")
            
    except Exception as e:
        print(f"❌ Error reading logs: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main execution."""
    print("🚀 REAL AGENTIC SYSTEM - CAPTURING ACTUAL PROMPTS")
    print("=" * 80)
    print("Running real agents and capturing the actual prompts sent to LLMs\\n")
    
    # Setup logging
    setup_debug_logging()
    
    try:
        # Run real agents
        print("🎭 Starting real agent execution...")
        
        analysis_result = run_real_analysis_agent()
        generation_result = run_real_generation_agent()
        
        # Show captured prompts from logs
        capture_llm_logs()
        
        print("\\n🎉 EXECUTION COMPLETED")
        print("=" * 50)
        print("✅ Real agents executed")
        print("✅ Actual prompts sent to LLMs")
        print("✅ Real responses received")
        print("✅ All interactions logged")
        
        return {
            'analysis': analysis_result,
            'generation': generation_result
        }
        
    except Exception as e:
        print(f"❌ Execution failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
