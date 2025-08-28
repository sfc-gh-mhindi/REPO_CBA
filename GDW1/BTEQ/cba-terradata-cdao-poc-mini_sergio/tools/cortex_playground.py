#!/usr/bin/env python3
"""
Cortex Playground

Interactive playground for testing Snowflake Cortex functionality
with different models, prompts, and parameters.
"""

import sys
import time
from pathlib import Path
from typing import Dict, Any, List, Optional

# Add bteq_dcf to path
sys.path.insert(0, str(Path(__file__).parent.parent))


class CortexPlayground:
    """Interactive playground for Cortex experimentation."""
    
    def __init__(self):
        self.session = None
        self.current_model = "claude-3-5-sonnet"
        self.available_models = [
            "claude-3-5-sonnet",
            "claude-3-haiku",
            "llama3.1-8b", 
            "llama3.1-70b",
            "mixtral-8x7b",
            "snowflake-arctic"
        ]
        self.setup_session()
    
    def setup_session(self):
        """Setup Snowflake session."""
        try:
            from bteq_dcf.utils.database import get_connection_manager
            connection_manager = get_connection_manager()
            self.session = connection_manager.get_session()
            
            if self.session:
                print("✅ Connected to Snowflake")
            else:
                print("❌ Failed to connect to Snowflake")
                
        except Exception as e:
            print(f"❌ Session setup failed: {e}")
    
    def test_complete(self, prompt: str, model: Optional[str] = None) -> str:
        """Test Cortex Complete function."""
        if not self.session:
            return "❌ No Snowflake session"
        
        model = model or self.current_model
        
        try:
            from snowflake.cortex import Complete
            
            print(f"🤖 Calling {model} with prompt: {prompt[:50]}...")
            start_time = time.time()
            
            response = Complete(model, prompt, session=self.session)
            
            execution_time = (time.time() - start_time) * 1000
            print(f"⏱️  Execution time: {execution_time:.0f}ms")
            
            return response or "Empty response"
            
        except Exception as e:
            return f"❌ Error: {e}"
    
    def test_summarize(self, text: str) -> str:
        """Test Cortex Summarize function."""
        if not self.session:
            return "❌ No Snowflake session"
        
        try:
            from snowflake.cortex import Summarize
            
            print(f"📋 Summarizing text ({len(text)} chars)...")
            start_time = time.time()
            
            summary = Summarize(text, session=self.session)
            
            execution_time = (time.time() - start_time) * 1000
            print(f"⏱️  Execution time: {execution_time:.0f}ms")
            
            return summary or "Empty summary"
            
        except Exception as e:
            return f"❌ Error: {e}"
    
    def test_extract_answer(self, text: str, question: str) -> str:
        """Test Cortex ExtractAnswer function."""
        if not self.session:
            return "❌ No Snowflake session"
        
        try:
            from snowflake.cortex import ExtractAnswer
            
            print(f"🔍 Extracting answer for: {question}")
            start_time = time.time()
            
            answer = ExtractAnswer(text, question, session=self.session)
            
            execution_time = (time.time() - start_time) * 1000
            print(f"⏱️  Execution time: {execution_time:.0f}ms")
            
            return answer or "No answer found"
            
        except Exception as e:
            return f"❌ Error: {e}"
    
    def test_sentiment(self, text: str) -> str:
        """Test Cortex Sentiment function."""
        if not self.session:
            return "❌ No Snowflake session"
        
        try:
            from snowflake.cortex import Sentiment
            
            print(f"😊 Analyzing sentiment: {text[:30]}...")
            start_time = time.time()
            
            sentiment = Sentiment(text, session=self.session)
            
            execution_time = (time.time() - start_time) * 1000
            print(f"⏱️  Execution time: {execution_time:.0f}ms")
            
            return sentiment or "Unknown sentiment"
            
        except Exception as e:
            return f"❌ Error: {e}"
    
    def compare_models(self, prompt: str, models: Optional[List[str]] = None) -> Dict[str, str]:
        """Compare responses from different models."""
        models = models or self.available_models[:3]  # Use first 3 models by default
        results = {}
        
        print(f"🎯 Comparing {len(models)} models...")
        print(f"📝 Prompt: {prompt}")
        print("-" * 50)
        
        for model in models:
            try:
                response = self.test_complete(prompt, model)
                results[model] = response
                print(f"\n{model}:")
                print(f"{response[:200]}{'...' if len(response) > 200 else ''}")
                print("-" * 30)
                
            except Exception as e:
                results[model] = f"Error: {e}"
                print(f"\n{model}: ❌ {e}")
                print("-" * 30)
        
        return results
    
    def bteq_specific_tests(self):
        """Run BTEQ-specific test scenarios."""
        print("🔧 Running BTEQ-Specific Tests")
        print("=" * 35)
        
        # Test 1: BTEQ explanation
        print("\n1. BTEQ Explanation Test")
        print("-" * 25)
        response = self.test_complete("What is BTEQ? Explain in 2 sentences.")
        print(f"Response: {response}")
        
        # Test 2: Migration complexity assessment
        print("\n2. Migration Complexity Assessment")
        print("-" * 35)
        
        sample_bteq = """
        .RUN FILE=logon.sql
        .IF ERRORCODE <> 0 THEN .GOTO EXITERR
        
        DELETE FROM TARGET_TABLE;
        INSERT INTO TARGET_TABLE 
        SELECT * FROM SOURCE_TABLE 
        WHERE DATE_COL >= DATE '2024-01-01';
        
        .IF ERRORCODE <> 0 THEN .GOTO EXITERR
        .LABEL EXITERR
        .LOGOFF
        """
        
        complexity_prompt = f"""
        Rate the migration complexity of this BTEQ script to Snowflake (1-10 scale):
        
        {sample_bteq}
        
        Provide rating and brief explanation.
        """
        
        response = self.test_complete(complexity_prompt)
        print(f"Complexity Assessment: {response}")
        
        # Test 3: Snowflake procedure generation
        print("\n3. Snowflake Procedure Generation")
        print("-" * 35)
        
        generation_prompt = f"""
        Convert this BTEQ script to a Snowflake stored procedure:
        
        {sample_bteq}
        
        Include proper error handling and logging.
        """
        
        response = self.test_complete(generation_prompt)
        print(f"Generated Procedure: {response[:300]}...")
        
        # Test 4: Extract key patterns
        print("\n4. Pattern Extraction")
        print("-" * 20)
        
        pattern_question = "What BTEQ control flow patterns are used in this script?"
        answer = self.test_extract_answer(sample_bteq, pattern_question)
        print(f"Patterns: {answer}")
    
    def interactive_mode(self):
        """Interactive mode for manual testing."""
        print("\n🎮 Interactive Cortex Playground")
        print("=" * 35)
        print("Commands:")
        print("  complete <prompt>     - Test Complete function")
        print("  summarize <text>      - Test Summarize function")
        print("  extract <text> | <q>  - Test ExtractAnswer function")
        print("  sentiment <text>      - Test Sentiment function")
        print("  model <name>          - Switch model")
        print("  models                - List available models")
        print("  compare <prompt>      - Compare models")
        print("  bteq                  - Run BTEQ tests")
        print("  quit                  - Exit")
        print()
        
        while True:
            try:
                user_input = input(f"cortex[{self.current_model}]> ").strip()
                
                if not user_input:
                    continue
                
                if user_input.lower() == "quit":
                    break
                
                elif user_input.lower() == "models":
                    print("Available models:")
                    for i, model in enumerate(self.available_models, 1):
                        marker = "👈" if model == self.current_model else ""
                        print(f"  {i}. {model} {marker}")
                
                elif user_input.startswith("model "):
                    new_model = user_input[6:].strip()
                    if new_model in self.available_models:
                        self.current_model = new_model
                        print(f"✅ Switched to {new_model}")
                    else:
                        print(f"❌ Model not available: {new_model}")
                
                elif user_input.startswith("complete "):
                    prompt = user_input[9:].strip()
                    response = self.test_complete(prompt)
                    print(f"Response: {response}")
                
                elif user_input.startswith("summarize "):
                    text = user_input[10:].strip()
                    response = self.test_summarize(text)
                    print(f"Summary: {response}")
                
                elif user_input.startswith("extract "):
                    parts = user_input[8:].split(" | ")
                    if len(parts) == 2:
                        text, question = parts
                        response = self.test_extract_answer(text.strip(), question.strip())
                        print(f"Answer: {response}")
                    else:
                        print("Usage: extract <text> | <question>")
                
                elif user_input.startswith("sentiment "):
                    text = user_input[10:].strip()
                    response = self.test_sentiment(text)
                    print(f"Sentiment: {response}")
                
                elif user_input.startswith("compare "):
                    prompt = user_input[8:].strip()
                    self.compare_models(prompt)
                
                elif user_input.lower() == "bteq":
                    self.bteq_specific_tests()
                
                else:
                    print("❌ Unknown command. Type 'quit' to exit.")
                    
            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ Error: {e}")


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Cortex Playground")
    parser.add_argument("--interactive", "-i", action="store_true", help="Interactive mode")
    parser.add_argument("--bteq-tests", action="store_true", help="Run BTEQ-specific tests")
    parser.add_argument("--quick-test", action="store_true", help="Quick functionality test")
    
    args = parser.parse_args()
    
    playground = CortexPlayground()
    
    if not playground.session:
        print("❌ Cannot start playground without Snowflake session")
        sys.exit(1)
    
    if args.bteq_tests:
        playground.bteq_specific_tests()
    elif args.quick_test:
        # Quick test
        print("🏃 Quick Test")
        response = playground.test_complete("Say hello")
        print(f"Quick test result: {response}")
    elif args.interactive:
        playground.interactive_mode()
    else:
        # Default: run a few demonstration tests
        print("🎪 Cortex Playground Demo")
        print("=" * 25)
        
        # Demo 1: Basic complete
        print("\n1. Basic Complete Test")
        response = playground.test_complete("Explain what a database is in one sentence")
        print(f"Response: {response}")
        
        # Demo 2: Model comparison
        print("\n2. Model Comparison")
        playground.compare_models("What is BTEQ?", ["claude-3-5-sonnet", "llama3.1-8b"])
        
        # Demo 3: BTEQ tests
        playground.bteq_specific_tests()
        
        print("\n✨ Demo complete! Use --interactive for hands-on testing.")


if __name__ == "__main__":
    main()
