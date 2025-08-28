# 🚀 BTEQ Agentic Migration Runbook

## **Operational Guide for BTEQ to Snowflake Migration Framework**

This runbook provides comprehensive instructions for running the agentic BTEQ to DBT converter with all available processing modes and options.

---

## 📋 **Quick Reference**

### **Basic Syntax**
```bash
python main.py --input <path> --mode <mode> [options]
```

### **Available Processing Modes**
| Mode | Description | LLM Usage | Output |
|------|-------------|-----------|---------|
| `v1_prscrip_sp` | Traditional rule-based conversion | None | Snowflake SP |
| `v2_prscrip_claude_sp` | Rule-based + Claude enhancement | Claude | Enhanced SP |
| `v3_prscrip_claude_sp_claude_dbt` | SP + DBT conversion | Claude | SP + DBT |
| `v4_prscrip_claude_llama_sp` | Multi-model consensus | Claude + Llama | Best SP |
| `v5_claude_dbt` | Direct BTEQ → DBT | Claude | DBT only |

---

## 🎯 **Processing Mode Details**

### **v1_prscrip_sp - Traditional Prescriptive**
**Use Case**: Fast, reliable conversion using SQL parsing rules
**No AI/LLM required**

```bash
python main.py \
  --input references/current_state/bteq_sql/ACCT_BALN_BKDT_ADJ_RULE_ISRT.sql \
  --mode v1_prscrip_sp \
  --output output/v1_run
```

**✅ Advantages**: Fast, deterministic, no LLM costs
**❌ Limitations**: Limited handling of complex SQL patterns

---

### **v2_prscrip_claude_sp - Enhanced Prescriptive**
**Use Case**: Rule-based conversion enhanced by Claude for better quality
**LLM**: Claude-4-sonnet

```bash
python main.py \
  --input references/current_state/bteq_sql/ACCT_BALN_BKDT_ADJ_RULE_ISRT.sql \
  --mode v2_prscrip_claude_sp \
  --output output/v2_run \
  --verbose
```

**✅ Advantages**: Combines rule reliability with AI enhancement
**❌ Limitations**: Single model, no DBT output

---

### **v3_prscrip_claude_sp_claude_dbt - Complete Pipeline**
**Use Case**: Full migration - SP generation + DBT model creation
**LLM**: Claude-4-sonnet (for both SP and DBT)

```bash
python main.py \
  --input references/current_state/bteq_sql/ACCT_BALN_BKDT_ADJ_RULE_ISRT.sql \
  --mode v3_prscrip_claude_sp_claude_dbt \
  --output output/v3_run \
  --verbose
```

**✅ Advantages**: Complete migration pipeline, high-quality DBT models
**❌ Limitations**: Multiple LLM calls, longer execution time

---

### **v4_prscrip_claude_llama_sp - Multi-Model Consensus**
**Use Case**: Highest quality SP generation using model comparison
**LLM**: Claude-4-sonnet + Snowflake-Llama-3.3-70b

```bash
python main.py \
  --input references/current_state/bteq_sql/ACCT_BALN_BKDT_ADJ_RULE_ISRT.sql \
  --mode v4_prscrip_claude_llama_sp \
  --output output/v4_run \
  --verbose
```

**✅ Advantages**: Best quality through model consensus
**❌ Limitations**: Highest cost, longest execution time

---

### **v5_claude_dbt - Direct DBT Conversion** ⭐ **RECOMMENDED**
**Use Case**: Streamlined BTEQ → DBT conversion using exemplar-based prompts
**LLM**: Claude-4-sonnet with external prompt templates

```bash
python main.py \
  --input references/current_state/bteq_sql/ACCT_BALN_BKDT_ADJ_RULE_ISRT.sql \
  --mode v5_claude_dbt \
  --output output/v5_run \
  --verbose
```

**✅ Advantages**: 
- Single LLM call efficiency
- External prompt management
- High-quality exemplar-driven output
- Snowflake-specific syntax validation

**❌ Limitations**: DBT-only output (no intermediate SP)

---

## 🔧 **Command Line Options**

### **Input Options**
```bash
# Single file
--input path/to/file.sql

# Directory (processes all .sql files)
--input path/to/directory/

# Absolute paths supported
--input /Users/psundaram/Documents/prjs/cba/agentic/bteq_agentic/references/current_state/bteq_sql/
```

### **Output Options**
```bash
# Custom output directory
--output custom_output_dir

# Default: output/migration_run_YYYYMMDD_HHMMSS
```

### **Logging and Debug Options**
```bash
# Verbose logging
--verbose

# Set log level
--log-level DEBUG
--log-level INFO
--log-level WARNING

# Dry-run mode (generate prompts without LLM calls)
--dry-run
```

### **Configuration Options**
```bash
# Custom config file
--config custom_config.cfg

# Default: config.cfg
```

---

## 🧪 **Development and Testing Workflows**

### **1. Dry-Run Mode - Prompt Generation Only**
**Use Case**: Validate prompts, test external template system, debug without LLM costs

```bash
python main.py \
  --input references/current_state/bteq_sql/ACCT_BALN_BKDT_ADJ_RULE_ISRT.sql \
  --mode v5_claude_dbt \
  --dry-run \
  --verbose
```

**Output**: Generates prompts in `output/*/llm_interactions/requests/` without calling LLMs

### **2. External Prompt Template Testing**
**Use Case**: Test new prompt configurations

```bash
# Edit external prompts
vim config/prompts/dbt/snowflake_conversion.yaml
vim config/prompts/dbt/conversion_template.jinja2

# Test with dry-run
python main.py --input test.sql --mode v5_claude_dbt --dry-run
```

### **3. Quick Quality Check**
**Use Case**: Fast validation of converted DBT models

```bash
# Generate DBT model
python main.py --input input.sql --mode v5_claude_dbt --output test_run

# Navigate to DCF and test the model
cd dcf
dbt run -s model_name
```

---

## 📊 **Output Structure**

### **Standard Output Directory**
```
output/migration_run_YYYYMMDD_HHMMSS/
├── logs/
│   └── bteq_migration_YYYYMMDD_HHMMSS.log
├── results/
│   ├── stored_procedures/           # v1-v4 modes
│   ├── dbt/                        # v3, v5 modes
│   │   ├── model_name.sql
│   │   ├── model_name_metadata.json
│   │   └── migration_summary.json
│   └── llm_interactions/           # All LLM modes
│       ├── requests/
│       │   └── bteq2dbt_model_claude4_request.txt
│       ├── responses/
│       │   └── bteq2dbt_model_claude4_response.txt
│       └── summary/
│           └── session_summary.json
```

### **Key Output Files**

**DBT Models** (`results/dbt/`):
- `*.sql` - Ready-to-use DBT model
- `*_metadata.json` - Model configuration and metadata
- `migration_summary.json` - Complete migration details

**LLM Interactions** (`results/llm_interactions/`):
- **Requests**: Complete prompts sent to LLMs
- **Responses**: Full LLM responses with metadata
- **Summary**: Session analytics and performance metrics

---

## 🎯 **Common Usage Patterns**

### **Production Migration Workflow**
```bash
# 1. Test with dry-run first
python main.py --input bteq_file.sql --mode v5_claude_dbt --dry-run

# 2. Run actual conversion
python main.py --input bteq_file.sql --mode v5_claude_dbt --verbose

# 3. Test generated DBT model
cd dcf && dbt run -s generated_model_name

# 4. Review outputs and LLM interactions
ls -la output/migration_run_*/results/
```

### **Batch Processing Multiple Files**
```bash
# Process entire directory
python main.py \
  --input references/current_state/bteq_sql/ \
  --mode v5_claude_dbt \
  --output output/batch_migration \
  --verbose
```

### **Development and Prompt Engineering**
```bash
# 1. Modify external prompts
vim config/prompts/dbt/conversion_template.jinja2

# 2. Test changes with dry-run
python main.py --input test.sql --mode v5_claude_dbt --dry-run

# 3. Review generated prompts
cat output/*/results/llm_interactions/requests/*_request.txt

# 4. Run actual conversion to test quality
python main.py --input test.sql --mode v5_claude_dbt
```

---

## 🐛 **Troubleshooting Guide**

### **Common Issues and Solutions**

#### **LLM Service Unavailable**
```
Error: LLM service required but is unavailable
```

**Solution**:
1. Check Snowflake connection: `python -c "from utils.database.snowflake_connection import test_connection; test_connection()"`
2. Verify Cortex access in Snowflake
3. Use `--dry-run` mode for testing

#### **Invalid Processing Mode**
```
Error: Unknown processing mode 'xyz'
```

**Solution**: Use valid modes: `v1_prscrip_sp`, `v2_prscrip_claude_sp`, `v3_prscrip_claude_sp_claude_dbt`, `v4_prscrip_claude_llama_sp`, `v5_claude_dbt`

#### **File Not Found**
```
Error: Input file not found
```

**Solution**: Use absolute paths or verify relative paths from project root

#### **DBT Model Syntax Errors**
```
Database Error: syntax error line X
```

**Solution**: 
1. Check LLM-generated model uses Snowflake syntax
2. Review external prompt templates for Snowflake requirements
3. Use exemplar-based generation (v5 mode)

#### **Prompt Template Issues**
```
Error: Failed to render template
```

**Solution**:
1. Validate YAML syntax: `python -c "import yaml; yaml.safe_load(open('config/prompts/dbt/snowflake_conversion.yaml'))"`
2. Check Jinja2 template syntax
3. Verify template variables match context

---

## 📈 **Performance Benchmarks**

### **Execution Times (Approximate)**
| Mode | Small File (<100 lines) | Large File (>500 lines) | Notes |
|------|-------------------------|--------------------------|-------|
| v1_prscrip_sp | 5-10s | 15-30s | No LLM calls |
| v2_prscrip_claude_sp | 30-60s | 60-120s | 1 LLM call |
| v3_prscrip_claude_sp_claude_dbt | 60-120s | 120-240s | 2 LLM calls |
| v4_prscrip_claude_llama_sp | 90-180s | 180-360s | 2 LLM calls + consensus |
| v5_claude_dbt | 30-60s | 60-120s | 1 LLM call (optimized) |

### **Resource Usage**
- **Memory**: 512MB - 2GB (depending on file size)
- **Disk**: 10-100MB per migration run
- **Network**: LLM API calls (Claude: ~1-5MB per call)

---

## 🔄 **Integration with DCF Framework**

### **Post-Migration DBT Testing**
```bash
# After successful migration
cd dcf

# Test individual model
dbt run -s generated_model_name

# Run with validation
dbt test -s generated_model_name

# Check compiled SQL
dbt compile -s generated_model_name
cat target/compiled/gdw1_csel4_migration/models/path/to/model.sql
```

### **Production Deployment**
```bash
# Copy generated model to appropriate DCF location
cp output/*/results/dbt/model.sql dcf/models/appropriate/path/

# Update DCF documentation
dbt docs generate && dbt docs serve
```

---

## 📚 **Reference Links**

- **External Prompt Templates**: `config/prompts/dbt/`
- **DCF Framework**: `dcf/README.md`
- **Architecture Documentation**: `PATHS_AND_ROUTES_ANALYSIS.md`
- **Operations Guide**: `docs/ops_runbook.md`
- **Service Architecture**: `REFACTORING_SUCCESS.md`

---

## ✅ **Pre-Flight Checklist**

Before running migrations:

- [ ] Snowflake connection validated (`dbt debug` in dcf/)
- [ ] Input file exists and is valid BTEQ SQL
- [ ] Output directory has write permissions
- [ ] LLM services available (unless using `--dry-run`)
- [ ] External prompt templates up to date
- [ ] Sufficient disk space for outputs

---

**🎯 This runbook enables efficient, reliable BTEQ to Snowflake migrations with comprehensive agentic AI assistance!** 🚀
