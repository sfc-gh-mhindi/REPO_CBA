# Cortex Integration Testing Tools

## Overview

This directory contains comprehensive testing tools for validating the Snowflake Cortex + LangChain integration in isolation, without needing the full BTEQ migration pipeline.

## Test Scripts

### 1. `test_simple_cortex.py` - Basic Functionality Test

**Purpose**: Quick validation of core functionality
**Usage**:
```bash
python tools/test_simple_cortex.py
```

**Tests**:
- ✅ Basic Cortex API imports
- ✅ Snowflake connection
- ✅ Cortex Complete, Summarize, Sentiment functions
- ✅ LangChain wrapper integration
- ✅ Multi-model support

**Example Output**:
```
🧪 Simple Cortex Test
=====================
1. Testing snowflake-ml-python import...
   ✅ Cortex imports successful
2. Testing Snowflake connection...
   ✅ Snowflake session created
3. Testing Cortex Complete...
   ✅ Cortex response: Hello! How can I help you today?
```

### 2. `test_cortex_integration.py` - Comprehensive Test Suite

**Purpose**: Full integration validation with performance metrics
**Usage**:
```bash
# Full comprehensive test
python tools/test_cortex_integration.py --full

# Quick connectivity check
python tools/test_cortex_integration.py --quick
```

**Tests**:
- 🔌 Snowflake connection validation
- 🤖 Direct Cortex Python API testing
- 🔗 LangChain wrapper functionality
- 🎯 Multi-model orchestration
- 📊 Embeddings support
- ⚠️ Error handling and edge cases
- ⚡ Performance benchmarking

**Example Output**:
```
📋 TEST SUMMARY
=====================================
Overall Success Rate: 6/6 (100.0%)

Snowflake Connection     ✅ PASS
Direct Cortex API        ✅ PASS
LangChain Wrapper        ✅ PASS
Multi-Model Support      ✅ PASS
Embeddings Support       ✅ PASS
Error Handling          ✅ PASS

⚡ PERFORMANCE METRICS:
  cortex_complete_ms               1250.0ms
  langchain_invoke_ms              1180.0ms
  multi_model_ms                   3200.0ms
```

### 3. `cortex_playground.py` - Interactive Testing Environment

**Purpose**: Hands-on experimentation with Cortex functions
**Usage**:
```bash
# Interactive mode
python tools/cortex_playground.py --interactive

# BTEQ-specific tests
python tools/cortex_playground.py --bteq-tests

# Quick demo
python tools/cortex_playground.py
```

**Features**:
- 🎮 Interactive command-line interface
- 🔄 Real-time model switching
- 📝 Custom prompt testing
- 🎯 Model comparison capabilities
- 🔧 BTEQ-specific test scenarios

**Interactive Commands**:
```
cortex[claude-3-5-sonnet]> complete What is BTEQ?
cortex[claude-3-5-sonnet]> model llama3.1-8b
cortex[llama3.1-8b]> compare Explain databases
cortex[llama3.1-8b]> bteq
cortex[llama3.1-8b]> quit
```

## Setup Requirements

### Dependencies
```bash
# Core Snowflake dependencies
pip install snowflake-connector-python
pip install snowflake-snowpark-python
pip install snowflake-ml-python

# LangChain dependencies
pip install langchain
pip install langchain-core

# Crypto for JWT authentication
pip install cryptography
```

### Configuration

Ensure your Snowflake connection is configured in `snowflake_connection.py`:
```python
config = {
    "account": "SFPSCOGS-DEMO_PUPAD",
    "user": "svc_dcf", 
    "authenticator": "SNOWFLAKE_JWT",
    "private_key_path": "/Users/psundaram/.snowflake/keys/dcf_service_account_key.pem",
    "warehouse": "wh_psdm_xs",
    "database": "psundaram",
    "schema": "gdw1_mig"
}
```

### Snowflake Setup

1. **User Permissions**: Ensure your user has `CORTEX_USER` role
2. **Model Access**: Verify access to required models (claude-3-5-sonnet, llama3.1-8b, etc.)
3. **JWT Authentication**: Configure public key in Snowflake user settings

## Test Scenarios

### Basic Connectivity
```bash
python tools/test_simple_cortex.py
```
Validates that you can connect to Snowflake and make basic Cortex calls.

### Full Integration Validation
```bash
python tools/test_cortex_integration.py --full
```
Comprehensive testing including error handling, performance, and edge cases.

### BTEQ Migration Specific
```bash
python tools/cortex_playground.py --bteq-tests
```
Tests specific to BTEQ migration scenarios including:
- BTEQ pattern recognition
- Migration complexity assessment
- Snowflake procedure generation
- Control flow analysis

### Performance Benchmarking
The comprehensive test includes performance metrics for:
- Single model response times
- Multi-model comparison timing
- Prompt size impact
- Sequential request handling

## Integration with Main Pipeline

Once tests pass, the integration works with the main BTEQ migration pipeline:

```python
from agentic.integration import create_agentic_pipeline

# Create pipeline with Cortex integration
pipeline = create_agentic_pipeline(enable_agentic=True)

# Process BTEQ file with multi-model enhancement
result = pipeline.process_bteq_file("input.bteq", use_agentic=True)
```

## Troubleshooting

### Common Issues

1. **Import Error: snowflake.cortex**
   ```
   pip install snowflake-ml-python
   ```

2. **Authentication Failed**
   - Check JWT key path and permissions
   - Verify user has CORTEX_USER role

3. **Model Not Available**
   - Verify model access in your Snowflake account
   - Check available models with playground

4. **Empty Responses**
   - Check network connectivity
   - Verify Cortex service availability
   - Try different models

### Debug Mode

Enable detailed logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Manual Testing

Use the playground for manual debugging:
```bash
python tools/cortex_playground.py --interactive
```

## API Usage Examples

### Direct Cortex API (Recommended)
```python
from snowflake.cortex import Complete, Summarize, ExtractAnswer, Sentiment

# Using the direct Python API per Snowflake documentation
text = "The Snowflake company was co-founded by Thierry Cruanes..."

print(Complete("claude-3-5-sonnet", "how do snowflakes get their unique patterns?"))
print(ExtractAnswer(text, "When was snowflake founded?"))
print(Sentiment("I really enjoyed this restaurant. Fantastic service!"))
print(Summarize(text))
```

### LangChain Integration
```python
from tools.langchain_cortex_direct import create_cortex_llm

llm = create_cortex_llm(model="claude-3-5-sonnet")
response = llm.invoke("Explain BTEQ migration to Snowflake")
```

### Multi-Model Comparison
```python
from tools.langchain_cortex_direct import create_multi_model_llm

multi_llm = create_multi_model_llm(models=["claude-3-5-sonnet", "llama3.1-8b"])
responses = multi_llm.generate_with_all_models("Assess migration complexity")
```

## Success Criteria

✅ **All tests pass**: Basic functionality working
✅ **Performance acceptable**: Response times < 5 seconds  
✅ **Error handling robust**: Graceful fallbacks on failures
✅ **Multi-model support**: Multiple models working
✅ **LangChain integration**: Wrapper functions correctly
✅ **BTEQ scenarios**: Domain-specific testing passes

---

*Testing Tools Documentation - BTEQ DCF Framework*  
*Reference: [Snowflake Cortex API Documentation](https://docs.snowflake.com/en/developer-guide/snowpark-ml/reference/1.10.0/index-cortex)*
