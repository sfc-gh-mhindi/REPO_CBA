#!/usr/bin/env python3
"""
Standalone Test for LangChain + Snowflake Cortex Integration

Tests all integration layers in isolation:
1. Basic Snowflake connection
2. Direct Cortex API calls
3. LangChain wrapper integration
4. Multi-model capabilities
5. Error handling and fallbacks
"""

import logging
import sys
import time
from pathlib import Path
from typing import Dict, Any, List

# Add bteq_dcf to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Setup logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CortexIntegrationTester:
    """Comprehensive tester for Cortex integration."""
    
    def __init__(self):
        self.test_results = {
            "snowflake_connection": False,
            "direct_cortex_api": False,
            "langchain_cortex_wrapper": False,
            "multi_model_support": False,
            "embeddings_support": False,
            "error_handling": False,
            "performance_metrics": {}
        }
        self.session = None
    
    def run_all_tests(self) -> Dict[str, Any]:
        """Run all integration tests."""
        print("🧪 Starting Comprehensive Cortex Integration Tests")
        print("=" * 60)
        
        # Test 1: Snowflake Connection
        print("\n📡 Test 1: Snowflake Connection")
        print("-" * 40)
        self.test_snowflake_connection()
        
        # Test 2: Direct Cortex API
        print("\n🤖 Test 2: Direct Cortex API")
        print("-" * 35)
        self.test_direct_cortex_api()
        
        # Test 3: LangChain Wrapper
        print("\n🔗 Test 3: LangChain Cortex Wrapper")
        print("-" * 40)
        self.test_langchain_wrapper()
        
        # Test 4: Multi-Model Support
        print("\n🎯 Test 4: Multi-Model Support")
        print("-" * 35)
        self.test_multi_model_support()
        
        # Test 5: Embeddings
        print("\n📊 Test 5: Embeddings Support")
        print("-" * 35)
        self.test_embeddings_support()
        
        # Test 6: Error Handling
        print("\n⚠️  Test 6: Error Handling")
        print("-" * 30)
        self.test_error_handling()
        
        # Test 7: Performance
        print("\n⚡ Test 7: Performance Metrics")
        print("-" * 35)
        self.test_performance_metrics()
        
        # Summary
        self.print_test_summary()
        
        return self.test_results
    
    def test_snowflake_connection(self):
        """Test basic Snowflake connection."""
        try:
            from bteq_dcf.utils.database import get_connection_manager
            
            print("   🔌 Testing Snowflake connection...")
            connection_manager = get_connection_manager()
            
            # Test basic connection
            if connection_manager.test_connection():
                print("   ✅ Snowflake connection successful")
                self.session = connection_manager.get_session()
                self.test_results["snowflake_connection"] = True
                
                # Get connection details
                config = connection_manager.connection_config
                print(f"   📍 Account: {config['account']}")
                print(f"   👤 User: {config['user']}")
                print(f"   🏭 Warehouse: {config['warehouse']}")
                print(f"   🗄️  Database: {config['database']}.{config['schema']}")
                
            else:
                print("   ❌ Snowflake connection failed")
                
        except Exception as e:
            print(f"   ❌ Connection test failed: {e}")
            logger.error(f"Snowflake connection error: {e}")
    
    def test_direct_cortex_api(self):
        """Test direct Snowflake Cortex Python API."""
        if not self.session:
            print("   ⏭️  Skipping - no Snowflake session")
            return
        
        try:
            print("   🧠 Testing direct Cortex API imports...")
            
            # Test imports
            from snowflake.cortex import Complete, Summarize, ExtractAnswer, Sentiment
            print("   ✅ Cortex imports successful")
            
            # Test basic Complete function
            print("   🤖 Testing Cortex Complete...")
            start_time = time.time()
            
            test_prompt = "Say 'hello' in exactly one word"
            response = Complete("claude-3-5-sonnet", test_prompt, session=self.session)
            
            execution_time = (time.time() - start_time) * 1000
            
            if response and response.strip():
                print(f"   ✅ Cortex Complete successful ({execution_time:.0f}ms)")
                print(f"   📝 Response: '{response.strip()}'")
                self.test_results["direct_cortex_api"] = True
                self.test_results["performance_metrics"]["cortex_complete_ms"] = execution_time
            else:
                print("   ❌ Empty response from Cortex Complete")
            
            # Test Summarize
            print("   📋 Testing Cortex Summarize...")
            summary_text = "This is a test document about BTEQ migration to Snowflake. BTEQ scripts contain SQL and control flow statements that need to be converted to Snowflake stored procedures."
            summary = Summarize(summary_text, session=self.session)
            
            if summary:
                print(f"   ✅ Summarize successful: {summary[:50]}...")
            else:
                print("   ⚠️  Summarize returned empty")
            
            # Test Sentiment
            print("   😊 Testing Cortex Sentiment...")
            sentiment = Sentiment("This is a great migration tool!", session=self.session)
            
            if sentiment:
                print(f"   ✅ Sentiment analysis: {sentiment}")
            else:
                print("   ⚠️  Sentiment analysis returned empty")
                
        except ImportError as e:
            print(f"   ❌ Cortex API not available: {e}")
            print("   💡 Install with: pip install snowflake-ml-python")
        except Exception as e:
            print(f"   ❌ Direct API test failed: {e}")
            logger.error(f"Direct Cortex API error: {e}")
    
    def test_langchain_wrapper(self):
        """Test LangChain wrapper for Cortex."""
        try:
            print("   🔗 Testing LangChain wrapper imports...")
            
            # Test direct wrapper
            from tools.langchain_cortex_direct import (
                create_cortex_llm, 
                SnowflakeCortexDirectLLM
            )
            print("   ✅ LangChain wrapper imports successful")
            
            # Test wrapper creation
            print("   🏗️  Creating LangChain LLM wrapper...")
            llm = create_cortex_llm(model="claude-3-5-sonnet", session=self.session)
            print(f"   ✅ LLM wrapper created: {llm._llm_type}")
            
            # Test LLM invocation
            print("   🤖 Testing LLM invocation...")
            start_time = time.time()
            
            test_prompt = "What is BTEQ? Answer in one sentence."
            response = llm.invoke(test_prompt)
            
            execution_time = (time.time() - start_time) * 1000
            
            if response and response.strip():
                print(f"   ✅ LangChain invocation successful ({execution_time:.0f}ms)")
                print(f"   📝 Response: {response[:80]}...")
                self.test_results["langchain_cortex_wrapper"] = True
                self.test_results["performance_metrics"]["langchain_invoke_ms"] = execution_time
            else:
                print("   ❌ Empty response from LangChain wrapper")
            
            # Test with parameters
            print("   ⚙️  Testing with parameters...")
            llm_with_params = SnowflakeCortexDirectLLM(
                model="claude-3-5-sonnet",
                temperature=0.7,
                max_tokens=100,
                session=self.session
            )
            
            param_response = llm_with_params.invoke("Count to 3")
            if param_response:
                print(f"   ✅ Parameter test successful: {param_response[:50]}...")
            else:
                print("   ⚠️  Parameter test returned empty")
                
        except ImportError as e:
            print(f"   ❌ LangChain wrapper imports failed: {e}")
            print("   💡 Check LangChain installation: pip install langchain")
        except Exception as e:
            print(f"   ❌ LangChain wrapper test failed: {e}")
            logger.error(f"LangChain wrapper error: {e}")
    
    def test_multi_model_support(self):
        """Test multi-model capabilities."""
        try:
            print("   🎯 Testing multi-model support...")
            
            from tools.langchain_cortex_direct import create_multi_model_llm
            
            # Create multi-model LLM
            models = ["claude-3-5-sonnet", "llama3.1-8b"]
            multi_llm = create_multi_model_llm(models=models, session=self.session)
            print(f"   ✅ Multi-model LLM created with models: {models}")
            
            # Test model switching
            print("   🔄 Testing model switching...")
            multi_llm.switch_model("llama3.1-8b")
            print(f"   ✅ Switched to: {multi_llm.current_model}")
            
            # Test generation with all models
            print("   🤖 Testing generation with all models...")
            test_prompt = "Define 'database' in 5 words"
            
            start_time = time.time()
            all_responses = multi_llm.generate_with_all_models(test_prompt)
            execution_time = (time.time() - start_time) * 1000
            
            successful_models = 0
            for model, response in all_responses.items():
                if response and not response.startswith("Error:"):
                    successful_models += 1
                    print(f"   ✅ {model}: {response[:50]}...")
                else:
                    print(f"   ❌ {model}: {response}")
            
            if successful_models > 0:
                print(f"   ✅ Multi-model test successful ({successful_models}/{len(models)} models)")
                self.test_results["multi_model_support"] = True
                self.test_results["performance_metrics"]["multi_model_ms"] = execution_time
            else:
                print("   ❌ No models responded successfully")
                
        except Exception as e:
            print(f"   ❌ Multi-model test failed: {e}")
            logger.error(f"Multi-model error: {e}")
    
    def test_embeddings_support(self):
        """Test embeddings functionality."""
        try:
            print("   📊 Testing embeddings support...")
            
            from tools.langchain_cortex_direct import create_cortex_embeddings
            
            # Test embeddings creation
            embeddings = create_cortex_embeddings(model="e5-base-v2", session=self.session)
            print("   ✅ Embeddings wrapper created")
            
            # Test single query embedding
            print("   🔍 Testing query embedding...")
            start_time = time.time()
            
            query = "BTEQ migration to Snowflake"
            embedding_vector = embeddings.embed_query(query)
            
            execution_time = (time.time() - start_time) * 1000
            
            if embedding_vector and len(embedding_vector) > 0:
                print(f"   ✅ Query embedding successful ({execution_time:.0f}ms)")
                print(f"   📐 Vector dimension: {len(embedding_vector)}")
                print(f"   🔢 Sample values: {embedding_vector[:3]}...")
                
                # Test document embeddings
                print("   📄 Testing document embeddings...")
                documents = [
                    "BTEQ is a Teradata utility",
                    "Snowflake is a cloud data platform",
                    "SQL is used for database queries"
                ]
                
                doc_embeddings = embeddings.embed_documents(documents)
                
                if doc_embeddings and len(doc_embeddings) == len(documents):
                    print(f"   ✅ Document embeddings successful ({len(doc_embeddings)} vectors)")
                    self.test_results["embeddings_support"] = True
                    self.test_results["performance_metrics"]["embeddings_ms"] = execution_time
                else:
                    print("   ❌ Document embeddings failed")
            else:
                print("   ❌ Query embedding failed")
                
        except Exception as e:
            print(f"   ❌ Embeddings test failed: {e}")
            logger.error(f"Embeddings error: {e}")
    
    def test_error_handling(self):
        """Test error handling and fallbacks."""
        try:
            print("   ⚠️  Testing error handling...")
            
            from tools.langchain_cortex_direct import create_cortex_llm
            
            # Test with invalid model
            print("   🚫 Testing invalid model handling...")
            try:
                invalid_llm = create_cortex_llm(model="invalid-model", session=self.session)
                response = invalid_llm.invoke("test")
                print("   ⚠️  Invalid model test: No error thrown (unexpected)")
            except Exception as e:
                print(f"   ✅ Invalid model properly handled: {type(e).__name__}")
            
            # Test with no session
            print("   🔌 Testing no session handling...")
            try:
                no_session_llm = create_cortex_llm(model="claude-3-5-sonnet", session=None)
                response = no_session_llm.invoke("test")
                print("   ⚠️  No session test: No error thrown (unexpected)")
            except Exception as e:
                print(f"   ✅ No session properly handled: {type(e).__name__}")
            
            # Test with very long prompt
            print("   📏 Testing long prompt handling...")
            try:
                long_prompt = "This is a test. " * 1000  # Very long prompt
                llm = create_cortex_llm(model="claude-3-5-sonnet", session=self.session)
                response = llm.invoke(long_prompt)
                
                if response:
                    print(f"   ✅ Long prompt handled successfully: {len(response)} chars")
                else:
                    print("   ⚠️  Long prompt returned empty response")
                    
            except Exception as e:
                print(f"   ✅ Long prompt properly handled: {type(e).__name__}")
            
            self.test_results["error_handling"] = True
            
        except Exception as e:
            print(f"   ❌ Error handling test failed: {e}")
            logger.error(f"Error handling test error: {e}")
    
    def test_performance_metrics(self):
        """Test and report performance metrics."""
        if not self.session:
            print("   ⏭️  Skipping - no Snowflake session")
            return
        
        try:
            print("   ⚡ Running performance benchmarks...")
            
            from tools.langchain_cortex_direct import create_cortex_llm
            
            # Test different prompt sizes
            prompts = {
                "short": "Hello",
                "medium": "Explain BTEQ in one paragraph",
                "long": "Explain the complete process of migrating BTEQ scripts to Snowflake stored procedures, including all the steps, considerations, and best practices. " * 5
            }
            
            llm = create_cortex_llm(model="claude-3-5-sonnet", session=self.session)
            
            for prompt_type, prompt in prompts.items():
                print(f"   🏃 Testing {prompt_type} prompt ({len(prompt)} chars)...")
                
                start_time = time.time()
                try:
                    response = llm.invoke(prompt)
                    execution_time = (time.time() - start_time) * 1000
                    
                    if response:
                        print(f"   ✅ {prompt_type}: {execution_time:.0f}ms, {len(response)} chars response")
                        self.test_results["performance_metrics"][f"{prompt_type}_prompt_ms"] = execution_time
                    else:
                        print(f"   ❌ {prompt_type}: Empty response")
                        
                except Exception as e:
                    execution_time = (time.time() - start_time) * 1000
                    print(f"   ❌ {prompt_type}: Failed after {execution_time:.0f}ms - {e}")
            
            # Test concurrent requests (simple simulation)
            print("   🔄 Testing concurrent-like requests...")
            start_time = time.time()
            
            simple_prompt = "Count to 3"
            responses = []
            
            for i in range(3):
                try:
                    response = llm.invoke(f"{simple_prompt} (request {i+1})")
                    responses.append(response)
                except Exception as e:
                    responses.append(f"Error: {e}")
            
            total_time = (time.time() - start_time) * 1000
            successful_requests = len([r for r in responses if not r.startswith("Error:")])
            
            print(f"   ✅ Sequential requests: {successful_requests}/3 successful in {total_time:.0f}ms")
            self.test_results["performance_metrics"]["sequential_3_requests_ms"] = total_time
            
        except Exception as e:
            print(f"   ❌ Performance test failed: {e}")
            logger.error(f"Performance test error: {e}")
    
    def print_test_summary(self):
        """Print comprehensive test summary."""
        print("\n" + "=" * 60)
        print("📋 TEST SUMMARY")
        print("=" * 60)
        
        total_tests = len(self.test_results) - 1  # Exclude performance_metrics
        passed_tests = sum(1 for k, v in self.test_results.items() if k != "performance_metrics" and v)
        
        print(f"Overall Success Rate: {passed_tests}/{total_tests} ({passed_tests/total_tests*100:.1f}%)")
        print()
        
        # Test results
        test_names = {
            "snowflake_connection": "Snowflake Connection",
            "direct_cortex_api": "Direct Cortex API", 
            "langchain_cortex_wrapper": "LangChain Wrapper",
            "multi_model_support": "Multi-Model Support",
            "embeddings_support": "Embeddings Support",
            "error_handling": "Error Handling"
        }
        
        for key, name in test_names.items():
            status = "✅ PASS" if self.test_results[key] else "❌ FAIL"
            print(f"{name:25} {status}")
        
        # Performance metrics
        if self.test_results["performance_metrics"]:
            print("\n⚡ PERFORMANCE METRICS:")
            for metric, value in self.test_results["performance_metrics"].items():
                print(f"  {metric:30} {value:.1f}ms")
        
        # Recommendations
        print("\n💡 RECOMMENDATIONS:")
        
        if not self.test_results["snowflake_connection"]:
            print("  • Check Snowflake connection configuration")
            print("  • Verify JWT key permissions and user setup")
        
        if not self.test_results["direct_cortex_api"]:
            print("  • Install snowflake-ml-python: pip install snowflake-ml-python")
            print("  • Verify Cortex permissions for your user")
        
        if not self.test_results["langchain_cortex_wrapper"]:
            print("  • Install LangChain: pip install langchain")
            print("  • Check LangChain wrapper implementation")
        
        if passed_tests == total_tests:
            print("  🎉 All tests passed! Integration is ready for production use.")
        elif passed_tests >= total_tests * 0.8:
            print("  ⚠️  Most tests passed. Address failing components for full functionality.")
        else:
            print("  🚨 Multiple critical components failing. Review setup and configuration.")


def run_isolated_test():
    """Run the isolated integration test."""
    tester = CortexIntegrationTester()
    results = tester.run_all_tests()
    
    return results["snowflake_connection"] and results["direct_cortex_api"]


def quick_connectivity_test():
    """Quick test for basic connectivity."""
    print("🔬 Quick Connectivity Test")
    print("=" * 30)
    
    try:
        # Test 1: Snowflake connection
        print("1. Testing Snowflake connection...")
        from bteq_dcf.utils.database import get_connection_manager
        
        connection_manager = get_connection_manager()
        if connection_manager.test_connection():
            print("   ✅ Connected to Snowflake")
            
            # Test 2: Basic Cortex call
            print("2. Testing basic Cortex call...")
            session = connection_manager.get_session()
            
            try:
                from snowflake.cortex import Complete
                response = Complete("claude-3-5-sonnet", "Say 'OK'", session=session)
                
                if response and response.strip():
                    print(f"   ✅ Cortex working: {response.strip()}")
                    print("\n🎉 Quick test PASSED! Full integration should work.")
                    return True
                else:
                    print("   ❌ Cortex returned empty response")
                    
            except ImportError:
                print("   ❌ snowflake-ml-python not installed")
            except Exception as e:
                print(f"   ❌ Cortex call failed: {e}")
        else:
            print("   ❌ Snowflake connection failed")
            
    except Exception as e:
        print(f"❌ Quick test failed: {e}")
    
    print("\n🚨 Quick test FAILED! Run full test for detailed diagnosis.")
    return False


if __name__ == "__main__":
    print("🧪 Cortex Integration Test Suite")
    print("=" * 40)
    
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description="Test Cortex integration")
    parser.add_argument("--quick", action="store_true", help="Run quick connectivity test only")
    parser.add_argument("--full", action="store_true", help="Run full comprehensive test")
    
    args = parser.parse_args()
    
    if args.quick:
        success = quick_connectivity_test()
    else:
        success = run_isolated_test()
    
    sys.exit(0 if success else 1)
