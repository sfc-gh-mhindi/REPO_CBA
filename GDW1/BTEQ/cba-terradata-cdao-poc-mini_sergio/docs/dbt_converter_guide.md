# BTEQ to DBT Model Converter

## Overview

The BTEQ to DBT Model Converter is a new component in the agentic BTEQ migration system that converts BTEQ SQL scripts to modern DBT (Data Build Tool) models with Jinja templating. This converter emphasizes DBT best practices, SQL optimization, and maintains a strict no-hallucination policy.

## Key Features

### 🎯 Dual LLM Comparison
- Uses multiple LLM models (default: Claude-4 Sonnet + Snowflake Llama-3.3-70B)
- Compares outputs and selects the best result based on quality metrics
- Provides detailed comparison notes and reasoning

### 🏗️ DBT Best Practices
- Modern DBT model structure with proper config blocks
- Appropriate materialization strategies (table/view/incremental)
- Clean Jinja templating and variable usage
- Data quality considerations and testing hooks

### 🚫 No Hallucination Policy
- Only transforms logic present in the source BTEQ
- Preserves all existing business logic exactly
- No addition of new columns, tables, or business rules
- Clear validation warnings for any potential issues

### 📊 Comprehensive Analysis
- Quality scoring for each generated model
- Feature detection (config blocks, macros, references)
- Migration notes explaining transformations
- Performance and maintainability warnings

## Architecture

```
Original BTEQ SQL
       ↓
   DBT Converter
   /           \
Model A      Model B
(Claude-4)   (Llama-3.3)
   \           /
    Comparison
        ↓
   Preferred DBT Model
```

## Usage Examples

### Basic Conversion

```python
from generator.dbt_converter import DBTConverter, DBTConversionContext

# Initialize converter with dual models
converter = DBTConverter(models=["claude-4-sonnet", "snowflake-llama-3.3-70b"])

# Create conversion context
context = DBTConversionContext(
    original_bteq_sql=bteq_content,
    chosen_stored_procedure=stored_proc_sql,  # For reference
    procedure_name="my_procedure"
)

# Perform conversion
result = converter.convert_to_dbt(context)

# Access the preferred result
dbt_sql = result.preferred_result.dbt_sql
quality_score = result.preferred_result.quality_score
```

### Integration with Agentic Pipeline

```python
from agentic.dbt_integration import create_dbt_enabled_pipeline

# Create DBT-enabled pipeline
pipeline = create_dbt_enabled_pipeline(output_dir="./output")

# Perform migration with DBT conversion
result = pipeline.migrate_with_dbt(
    original_bteq=bteq_content,
    procedure_name="my_procedure", 
    stored_procedure_sql=sp_sql,
    analysis_markdown=analysis
)

# Access both outputs
stored_proc_sql = result.preferred_stored_procedure_sql
dbt_model_sql = result.dbt_model_sql
```

### Simple Utility Function

```python
from agentic.dbt_integration import convert_bteq_to_dbt

# Quick conversion
dbt_sql = convert_bteq_to_dbt(
    original_bteq_sql=bteq_content,
    stored_procedure_sql=sp_sql,
    procedure_name="my_procedure",
    output_dir="./results"
)
```

## Prompt Engineering

### Core Prompt Structure

The DBT converter uses a carefully crafted prompt that emphasizes:

1. **Expert Persona**: "You are a senior data engineer expert in converting legacy BTEQ SQL scripts to modern DBT models"

2. **No Hallucination Policy**: 
   - ONLY transform the logic provided in the source
   - DO NOT add new business logic, columns, or transformations
   - PRESERVE all existing business logic exactly

3. **DBT Best Practices**:
   - Proper config blocks with materialization strategies
   - Use of ref(), var(), and source() macros
   - Clean CTE organization and documentation
   - Appropriate tagging and hooks

4. **Context-Rich Input**:
   - Original BTEQ SQL script
   - Reference stored procedure translation
   - Additional analysis context

### Example Generated Output

```sql
{%- set process_name = 'ACCT_BALN_BKDT_ADJ_RULE_LOAD' -%}
{%- set stream_name = 'SAP_BALANCE_ADJUSTMENT' -%}

{{
  config(
    materialized='table',
    database=var('target_database'),
    schema='DDSTG',
    tags=['stream_sap_balance', 'process_account_balance_backdate'],
    pre_hook=[
        "{{ log_dcf_exec_msg('Process started') }}"
    ],
    post_hook=[
        "{{ log_dcf_exec_msg('Process ended') }}"
    ]
  )
}}

/*
    Account Balance Backdated Adjustment Rule
    Purpose: Calculate backdated adjustments with business day logic
    Dependencies: Source adjustment tables and business calendar
*/

WITH source_adjustments AS (
    SELECT ...
),

business_day_logic AS (
    SELECT ...
)

SELECT * FROM final_transformations
```

## Quality Assessment

### Scoring Criteria

- **Base Model Score**: Different weights for different models
  - Claude-4 Sonnet: 0.9 baseline
  - Snowflake Llama-3.3-70B: 0.75 baseline

- **DBT Feature Bonuses**:
  - Config block: +0.05
  - Variable usage: +0.03
  - Model references: +0.03
  - CTEs usage: +0.02
  - Documentation: +0.02

### Warning Detection

- SELECT * usage warnings
- Missing config blocks
- Hardcoded database/schema values
- Incremental strategy issues

## File Structure

```
generator/
├── dbt_converter.py          # Core DBT conversion logic
└── ...

agentic/
├── agents.py                 # DBT conversion agent
├── dbt_integration.py        # Integration with pipeline
└── ...

tools/
├── test_dbt_converter.py     # Test script
└── ...

docs/
└── dbt_converter_guide.md    # This guide
```

## Testing

Run the test script to validate the converter:

```bash
cd /path/to/bteq_agentic
python tools/test_dbt_converter.py
```

This will:
1. Load example BTEQ SQL and reference files
2. Run dual-model DBT conversion
3. Compare results and select preferred output
4. Save results to `output/dbt_conversion_test/`
5. Display detailed comparison metrics

## Configuration Options

### Model Selection

```python
# Use different models
converter = DBTConverter(models=["claude-3-5-sonnet", "claude-4-sonnet"])

# Single model (no comparison)
converter = DBTConverter(models=["claude-4-sonnet"])
```

### Integration Settings

```python
# Enable/disable DBT in orchestration
config = OrchestrationConfig(
    enable_dbt_conversion=True,
    dbt_models_to_use=["claude-4-sonnet", "snowflake-llama-3.3-70b"]
)
```

## Best Practices

### For Optimal Results

1. **Provide Complete Context**: Include both original BTEQ and reference stored procedure
2. **Use Dual Models**: Leverage comparison for better quality
3. **Review Warnings**: Address any validation warnings in output
4. **Validate Business Logic**: Ensure no logic was lost in translation

### Common Patterns Handled

- **DELETE + INSERT → Materialization**: Converts procedural patterns to declarative DBT
- **BTEQ Variables → DBT Variables**: `%%VAR%%` becomes `{{ var('var_name') }}`
- **Control Flow → CTEs**: Procedural logic becomes organized CTEs
- **Date Functions**: Teradata functions converted to Snowflake/modern SQL

## Limitations

- Requires existing stored procedure as reference context
- Complex procedural logic may need manual review
- Custom business rules require domain expertise validation
- Performance tuning may need additional optimization

## Future Enhancements

- [ ] Automatic dbt test generation
- [ ] Schema.yml generation with documentation
- [ ] Integration with dbt-osmosis for column-level lineage
- [ ] Support for additional materialization strategies
- [ ] Custom macro generation for complex transformations
