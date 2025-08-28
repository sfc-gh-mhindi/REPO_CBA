#!/usr/bin/env python3
"""
Simple Cortex Integration Test

Minimal test script to verify basic Cortex functionality
without any dependencies on the BTEQ migration pipeline.
"""

import sys
from pathlib import Path

# Add bteq_dcf to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_basic_cortex():
    """Test basic Cortex functionality."""
    print("🧪 Simple Cortex Test")
    print("=" * 25)
    
    try:
        print("1. Testing snowflake-ml-python import...")
        from snowflake.cortex import Complete, Summarize, ExtractAnswer, Sentiment
        print("   ✅ Cortex imports successful")
        
        print("2. Testing Snowflake connection...")
        from bteq_dcf.utils.database import get_connection_manager
        
        connection_manager = get_connection_manager()
        session = connection_manager.get_session()
        
        if session:
            print("   ✅ Snowflake session created")
            
            print("3. Testing Cortex Complete...")
            response = Complete("claude-3-5-sonnet", "Say hello", session=session)
            
            if response:
                print(f"   ✅ Cortex response: {response}")
                
                print("4. Testing other Cortex functions...")
                
                # Test Summarize
                summary = Summarize("This is a test document about BTEQ migration.", session=session)
                print(f"   📋 Summary: {summary}")
                
                # Test Sentiment
                sentiment = Sentiment("This is great!", session=session)
                print(f"   😊 Sentiment: {sentiment}")
                
                print("\n🎉 All Cortex functions working!")
                return True
            else:
                print("   ❌ Cortex returned empty response")
        else:
            print("   ❌ Failed to create Snowflake session")
            
    except ImportError as e:
        print(f"   ❌ Import failed: {e}")
        print("   💡 Install with: pip install snowflake-ml-python")
    except Exception as e:
        print(f"   ❌ Test failed: {e}")
    
    return False


def test_langchain_cortex():
    """Test LangChain Cortex integration."""
    print("\n🔗 LangChain Cortex Test")
    print("=" * 25)
    
    try:
        print("1. Testing LangChain imports...")
        from langchain_core.language_models.llms import LLM
        print("   ✅ LangChain imports successful")
        
        print("2. Testing Cortex LLM wrapper...")
        from tools.langchain_cortex_direct import create_cortex_llm
        
        llm = create_cortex_llm(model="claude-3-5-sonnet")
        print("   ✅ Cortex LLM wrapper created")
        
        print("3. Testing LLM invocation...")
        response = llm.invoke("What is 2+2?")
        
        if response:
            print(f"   ✅ LLM response: {response}")
            print("\n🎉 LangChain integration working!")
            return True
        else:
            print("   ❌ LLM returned empty response")
            
    except ImportError as e:
        print(f"   ❌ Import failed: {e}")
        print("   💡 Install with: pip install langchain")
    except Exception as e:
        print(f"   ❌ Test failed: {e}")
    
    return False


def test_multi_model():
    """Test multi-model functionality."""
    print("\n🎯 Multi-Model Test")
    print("=" * 20)
    
    try:
        from tools.langchain_cortex_direct import create_multi_model_llm
        
        print("1. Creating multi-model LLM...")
        models = ["claude-3-5-sonnet", "llama3.1-8b"]
        multi_llm = create_multi_model_llm(models=models)
        print(f"   ✅ Multi-model LLM created with: {models}")
        
        print("2. Testing generation with all models...")
        responses = multi_llm.generate_with_all_models("Count to 3")
        
        successful = 0
        for model, response in responses.items():
            if response and not response.startswith("Error:"):
                print(f"   ✅ {model}: {response[:30]}...")
                successful += 1
            else:
                print(f"   ❌ {model}: {response}")
        
        if successful > 0:
            print(f"\n🎉 Multi-model test successful ({successful}/{len(models)} models)")
            return True
        else:
            print("\n❌ No models responded successfully")
            
    except Exception as e:
        print(f"   ❌ Test failed: {e}")
    
    return False


if __name__ == "__main__":
    print("🚀 Starting Simple Cortex Tests\n")
    
    results = []
    
    # Run tests
    results.append(("Basic Cortex", test_basic_cortex()))
    results.append(("LangChain Integration", test_langchain_cortex()))
    results.append(("Multi-Model", test_multi_model()))
    
    # Summary
    print("\n" + "=" * 40)
    print("📊 TEST SUMMARY")
    print("=" * 40)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{name:20} {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Integration is working correctly.")
    elif passed > 0:
        print("⚠️  Partial success. Some components need attention.")
    else:
        print("🚨 All tests failed. Check your setup and configuration.")
    
    sys.exit(0 if passed > 0 else 1)
