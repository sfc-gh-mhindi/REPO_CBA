# Tools Directory

## Purpose
Development utilities, testing tools, and batch processing scripts for developers.

## Categories

### **Batch Processing Tools**
- `batch_dbt_conversion.py` - Batch processing for multiple DBT conversions
- `reorganize_dbt_output.py` - Reorganizes and structures DBT conversion output files

### **Demo and Generation Tools**
- `demo_generator.py` - Demonstrates BTEQ generator capabilities
- `simple_demo.py` - Simple BTEQ conversion demonstration  
- `standalone_demo.py` - Standalone BTEQ conversion example
- `demo_agentic_capabilities.py` - Showcases AI agent capabilities
- `demo_live_prompts.py` - Interactive prompt demonstration

### **Testing and Validation Tools**
- `test_agentic_pipeline.py` - Tests the agentic AI pipeline
- `test_claude_llm.py` - Tests Claude LLM integration
- `test_cortex_integration.py` - Tests Snowflake Cortex integration
- `test_dbt_converter.py` - Tests DBT conversion functionality
- `test_snowflake_connection.py` - Tests Snowflake connectivity
- `test_simple_cortex.py` - Simple Cortex functionality tests

### **Analysis and Development Tools**
- `analyze_all_bteq.py` - Comprehensive BTEQ analysis utility
- `cortex_playground.py` - Interactive Cortex development environment
- `extract_table_dependencies.py` - Extracts table dependency metadata
- `split_extraction_results.py` - Splits extraction results for analysis

### **Legacy Migration Tools**
- `run_real_agentic_with_logging.py` - Runs agentic pipeline with comprehensive logging

## Usage Guidelines

### **For Development**
```bash
# Run demos
python tools/demo_generator.py
python tools/simple_demo.py

# Run tests
python tools/test_dbt_converter.py
python tools/test_cortex_integration.py

# Batch processing
python tools/batch_dbt_conversion.py --input references/current_state/bteq_sql/
```

### **For Analysis**
```bash
# Analyze BTEQ files
python tools/analyze_all_bteq.py --directory references/current_state/

# Extract dependencies  
python tools/extract_table_dependencies.py --input references/current_state/
```

## Contributing

1. Add new development tools to this directory
2. Follow naming convention: `{action}_{target}.py` (e.g., `test_dbt_converter.py`)
3. Include proper documentation and help text
4. Update this README when adding new tools
5. Group related tools by category
