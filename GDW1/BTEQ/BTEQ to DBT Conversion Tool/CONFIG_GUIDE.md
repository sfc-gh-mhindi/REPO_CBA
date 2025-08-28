# BTEQ to DBT Converter Configuration Guide

## Overview

The BTEQ to DBT converter supports YAML configuration files to customize model categories, directory mappings, and **🆕 automatic mart generation**. This allows you to adapt the converter to different BTEQ patterns and organizational structures while leveraging **INSERT operation detection** for automatic target table identification.

## **🆕 Enhanced Features**

### **INSERT Detection & Mart Generation**
The converter now automatically:
- **Detects INSERT operations** in BTEQ files
- **Extracts target table names** from INSERT statements  
- **Generates corresponding mart models** for identified target tables
- **Converts INSERT to SELECT** for intermediate models

### **Configuration-Driven Architecture**
- **Model Categories**: Define business domain groupings
- **Category Mapping**: Control model layer placement (intermediate/marts)
- **Categorization Patterns**: Configure file classification rules
- **Directory Structure**: Customize additional directories
- **Pattern Matching**: Case-sensitive pattern matching (uppercase recommended)

## Configuration File Structure

The configuration file uses YAML format and supports the following sections:

### 1. Model Categories
Define the categories that will be created in both intermediate and marts directories:

```yaml
model_categories:
  - account_balance
  - derived_account_party
  - portfolio_technical
  - process_control
```

**🆕 Enhanced Behavior**: Each category now automatically creates:
- `models/intermediate/{category}/` - For converted BTEQ logic
- `models/marts/{category}/` - For auto-generated target table marts

### 2. Category Mapping
Map BTEQ file categories to their target directory structure:

```yaml
category_mapping:
  account_balance: account_balance
  derived_account_party: derived_account_party
  portfolio_technical: portfolio_technical
  process_control: process_control
  data_loading: intermediate          # Maps to generic intermediate
  misc: intermediate                   # Maps to generic intermediate
  configuration: null                  # null means skip these files
```

**🆕 Enhanced Behavior**: 
- Categories mapped to specific names get their own intermediate and mart directories
- Categories mapped to `intermediate` go to generic intermediate folder
- Categories mapped to `null` are skipped entirely
- **Mart models are automatically placed** based on the original file's category

### 3. Categorization Patterns
Define patterns used to categorize BTEQ files based on filename:

```yaml
categorization_patterns:
  account_balance:
    - "ACCT_BALN_BKDT"
  derived_account_party:
    - "DERV_ACCT_PATY"
  portfolio_technical:
    - "PRTF_TECH"      # 🆕 Case-sensitive (uppercase)
  process_control:
    - "SP_"
    - "_SP_"
  data_loading:
    - "BTEQ_"
  configuration:
    - "LOGIN"           # 🆕 Case-sensitive (uppercase)
    - ".snowct"
    - "storage"
```

**⚠️ Important**: Patterns are **case-sensitive** and should be **UPPERCASE** to match typical BTEQ file naming conventions.

### 4. Additional Directories
Specify additional directories to create in the DBT project:

```yaml
additional_directories:
  - snapshots          # 🆕 Added for DBT snapshots
  - docs               # 🆕 Added for documentation
  - analyses           # Ad-hoc analyses
  - seeds             # Reference data
  - tests             # Data quality tests
```

**Note**: The `models/marts/{category}` directories are now **automatically created** based on `model_categories`, so they don't need to be listed here.

## **🆕 Complete Example Configuration**

### Standard GDW1 Configuration (converter_config.yaml)
```yaml
# Model categories - each gets intermediate and marts directories
model_categories:
  - account_balance
  - derived_account_party
  - portfolio_technical
  - process_control

# Category mapping - controls where models are placed
category_mapping:
  account_balance: account_balance
  derived_account_party: derived_account_party
  portfolio_technical: portfolio_technical
  process_control: process_control
  data_loading: intermediate
  misc: intermediate
  configuration: null  # Skip configuration files

# File categorization patterns (case-sensitive, uppercase recommended)
categorization_patterns:
  account_balance:
    - "ACCT_BALN_BKDT"
  derived_account_party:
    - "DERV_ACCT_PATY"
  portfolio_technical:
    - "PRTF_TECH"
  process_control:
    - "SP_"
    - "_SP_"
  data_loading:
    - "BTEQ_"
  configuration:
    - "LOGIN"
    - ".snowct"
    - "storage"

# Additional directories to create
additional_directories:
  - snapshots
  - docs
```

**Result**: This configuration generates:
- **51 intermediate models** in categorized directories
- **55+ mart models** automatically generated from detected INSERT operations
- **Complete directory structure** with all specified additional directories

## Usage

### Using Default Configuration
If no config file is specified, the converter uses built-in defaults:

```bash
python bteq_to_dbt_converter.py \
  --source ./Original_Files \
  --target ./My_DBT_Project \
  --project-name "GDW1 Migration"
```

### Using Custom Configuration (Recommended)
Specify a custom configuration file:

```bash
python bteq_to_dbt_converter.py \
  --source ./Original_Files \
  --target ./My_DBT_Project \
  --project-name "GDW1 Migration" \
  --config converter_config.yaml
```

Or using short form:

```bash
python bteq_to_dbt_converter.py \
  -s ./Original_Files \
  -t ./My_DBT_Project \
  -n "GDW1 Migration" \
  -c converter_config.yaml
```

## **🆕 INSERT Detection Configuration**

### How INSERT Detection Works
The converter automatically detects these INSERT patterns:

1. **Schema-qualified with variables**: `INSERT INTO %%VAR%%.TABLE_NAME`
2. **Schema-qualified**: `INSERT INTO SCHEMA.TABLE_NAME`
3. **Simple table name**: `INSERT INTO TABLE_NAME`
4. **With column lists**: `INSERT INTO TABLE (col1, col2) SELECT...`

### Target Table Identification
For each detected INSERT operation:
- **Table name is extracted** (e.g., `ACCT_BALN_BKDT` from `INSERT INTO %%CAD_PROD_DATA%%.ACCT_BALN_BKDT`)
- **Source file category is determined** (e.g., `account_balance` from filename pattern)
- **Mart model is generated** at `models/marts/{category}/mart_{table_name}.sql`

### Example: Automatic Mart Generation
```sql
-- Input: ACCT_BALN_BKDT_ISRT.sql contains:
INSERT INTO %%CAD_PROD_DATA%%.ACCT_BALN_BKDT (cols...) SELECT...

-- Generated Intermediate: models/intermediate/account_balance/int_acct_baln_bkdt_isrt.sql
SELECT cols... FROM source_table

-- 🆕 Generated Mart: models/marts/account_balance/mart_acct_baln_bkdt.sql
SELECT *, CURRENT_TIMESTAMP() AS MART_CREATED_TS 
FROM {{ ref('int_acct_baln_bkdt_isrt') }}
```

## Customization Examples

### Example 1: Different Business Domains
For a project with different business domains:

```yaml
model_categories:
  - customer_management
  - financial_reporting
  - risk_analytics
  - data_quality

category_mapping:
  customer_management: customer_management
  financial_reporting: financial_reporting
  risk_analytics: risk_analytics
  data_quality: data_quality
  data_loading: intermediate
  configuration: null

categorization_patterns:
  customer_management:
    - "CUST_"
    - "CLIENT_"
  financial_reporting:
    - "FIN_RPT"
    - "LEDGER"
  risk_analytics:
    - "RISK_"
    - "CREDIT_"
  data_quality:
    - "DQ_"
    - "AUDIT_"

additional_directories:
  - snapshots
  - docs
  - seeds/customer_data
  - seeds/reference_data
```

### Example 2: Simplified Structure
For a simpler project structure:

```yaml
model_categories:
  - core_processing
  - reporting

category_mapping:
  core_processing: core_processing
  reporting: reporting
  data_loading: core_processing
  misc: core_processing
  configuration: null

categorization_patterns:
  core_processing:
    - "PROC_"
    - "ETL_"
    - "LOAD_"
    - "INSERT_"
  reporting:
    - "RPT_"
    - "REPORT_"
    - "MART_"

additional_directories:
  - snapshots
  - analyses/performance
  - analyses/data_quality
```

### Example 3: **🆕 Advanced Mart Configuration**
For projects requiring specific mart organization:

```yaml
model_categories:
  - core_tables
  - dimension_tables
  - fact_tables
  - summary_tables

category_mapping:
  core_tables: core_tables
  dimension_tables: dimension_tables
  fact_tables: fact_tables
  summary_tables: summary_tables
  utilities: intermediate
  configuration: null

categorization_patterns:
  core_tables:
    - "TBL_"
    - "BASE_"
  dimension_tables:
    - "DIM_"
    - "REF_"
  fact_tables:
    - "FACT_"
    - "TXN_"
  summary_tables:
    - "SUM_"
    - "AGG_"
  utilities:
    - "UTIL_"
    - "HELPER_"

additional_directories:
  - snapshots/dimensions
  - snapshots/facts
  - docs/business_rules
  - docs/data_dictionary
```

## **🆕 Configuration Impact on Generated Output**

### Directory Structure Generated
Based on your configuration, the converter creates:

```
Generated-DBT-Project/
├── models/
│   ├── intermediate/
│   │   ├── {category1}/          # From model_categories
│   │   │   └── int_*.sql         # Converted BTEQ files
│   │   ├── {category2}/
│   │   └── ...
│   └── marts/
│       ├── {category1}/          # 🆕 Auto-generated based on model_categories
│       │   └── mart_*.sql        # 🆕 From detected INSERT operations
│       ├── {category2}/
│       └── ...
├── {additional_directory1}/       # From additional_directories
├── {additional_directory2}/
└── ...
```

### Generated README Documentation
The converter automatically documents:
- **Target Tables Identified**: List of tables from INSERT operations
- **Mart Models Generated**: Complete mapping of source files to mart models
- **File Categorization Summary**: Statistics by configured categories
- **Configuration Used**: Documents the YAML configuration applied

## Configuration Validation

The converter will:
- **Use default values** if the config file doesn't exist
- **Print warnings** if the config file has errors
- **Fall back to defaults** if any section is missing
- **Skip categories** mapped to `null`
- **🆕 Validate pattern matching** and report categorization results
- **🆕 Report INSERT detection** and mart generation statistics

## Best Practices

### 1. Pattern Configuration
- **Use uppercase patterns**: `"ACCT_BALN_BKDT"` not `"acct_baln_bkdt"`
- **Be specific**: Use unique patterns to avoid miscategorization
- **Test patterns**: Run on a small set of files first to verify categorization

### 2. Category Design
- **Align with business domains**: Categories should reflect business areas
- **Keep manageable**: Don't create too many categories (4-8 is optimal)
- **Plan for marts**: Consider how categories will translate to mart models

### 3. Configuration Management
- **Start with defaults**: Copy `converter_config.yaml` and modify as needed
- **Version control**: Keep configuration files in source control
- **Document decisions**: Add comments explaining categorization logic
- **Test incrementally**: Start with small changes to verify behavior

### 4. **🆕 INSERT Detection Optimization**
- **Review generated marts**: Check that mart models align with business needs
- **Validate table names**: Ensure extracted table names are correct
- **Consider consolidation**: Multiple INSERTs to same table create combined marts

## Troubleshooting

### Config file not loading
- Check file path is correct
- Verify YAML syntax is valid (use YAML validator)
- Check file permissions
- Look for configuration load success message: `✅ Loaded configuration from converter_config.yaml`

### Categories not created
- Verify category names in `model_categories` list
- Check `category_mapping` for null values
- Ensure patterns match your BTEQ file names (case-sensitive)
- Review categorization output in console logs

### Files not categorized correctly
- **🆕 Check case sensitivity**: Patterns must match exact filename case
- Review `categorization_patterns` section
- Use uppercase patterns for typical BTEQ naming
- Add debug prints to see which patterns match
- Look for categorization summary in generated output

### **🆕 INSERT detection issues**
- Check console output for "Files with INSERT Operations" count
- Review generated `CONVERSION_SUMMARY.md` for target table list
- Verify INSERT syntax matches supported patterns
- Look for mart generation messages: `✅ Generated mart: models/marts/{category}/mart_{table}.sql`

### **🆕 Missing mart models**
- Confirm files have INSERT operations (check intermediate models for INSERT→SELECT conversion)
- Verify category mapping doesn't map to `null`
- Check target table extraction in conversion summary
- Review mart directory creation in console output

## **🆕 Configuration Testing Checklist**

Before running large conversions:

✅ **YAML Syntax**: Validate YAML file syntax  
✅ **Pattern Matching**: Test categorization patterns on sample files  
✅ **Directory Structure**: Verify expected directory creation  
✅ **INSERT Detection**: Confirm target tables are identified correctly  
✅ **Mart Generation**: Check that mart models are created as expected  
✅ **Documentation**: Review generated README for accuracy  
✅ **File Counts**: Verify intermediate and mart model counts match expectations  

## Advanced Configuration Features

### **🆕 Pattern Debugging**
Add this to your configuration for debugging:

```yaml
# Debug configuration (for testing only)
debug_categorization: true  # Prints pattern matching details
show_insert_detection: true  # Shows detected INSERT operations
```

### **🆕 Custom Mart Templates**
For future enhancement, the system is designed to support custom mart templates:

```yaml
# Future feature
mart_templates:
  account_balance: "custom_account_balance_template.sql"
  derived_account_party: "custom_party_template.sql"
```

---

**🎯 Ready to configure your conversion?** Start with the standard `converter_config.yaml` and customize as needed!

**For more examples and patterns, see the generated output from `OutputTest4/` which demonstrates the full enhanced conversion capabilities.** 