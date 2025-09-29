# DBT Conversion Results - 20250821_155225

## 📁 Directory Structure

```
consol_dbt_20250821_155225/
├── README.md                          # This file
├── batch_conversion_summary.json      # Overall batch processing summary
├── dbt_models/                        # DBT model files (.sql)
├── references/                        # Reference stored procedures (.sql)  
├── metadata/                          # Conversion metadata (.json)
└── summaries/                         # Individual migration summaries (.json)
```

## 📊 File Counts

- **DBT Models**: 11 files
- **References**: 11 files  
- **Metadata**: 11 files
- **Summaries**: 11 files

## 🎯 DBT Models

The `dbt_models/` directory contains the converted DBT models ready for use in a DBT project:

```yaml
# Example dbt_project.yml structure
model-paths: ["models"]

models:
  your_project:
    acct_baln:
      materialized: table
      tags: ["account_balance", "backdated"]
```

## 🔗 References

The `references/` directory contains the generated Snowflake stored procedures that were used as reference during DBT conversion.

## 📊 Metadata & Summaries

- `metadata/`: Detailed conversion metadata including LLM model comparisons, quality scores, etc.
- `summaries/`: Individual file processing summaries with timing and status information.

## 📋 Batch Summary

See `batch_conversion_summary.json` for complete batch processing statistics.

---
Generated on: 2025-08-21T15:52:25.312695
Total files processed: 44
