# BTEQ DCF - Unified Entry Point

## 🚀 **Single Entry Point for All BTEQ Migration Modes**

The unified CLI provides **versioned processing modes** with clear progression paths from simple rule-based processing to advanced multi-model AI orchestration.

## **Entry Points**

### Primary Entry Point (Recommended)
```bash
# From project root
python bteq_migrate.py [options]

# From within bteq_dcf directory  
python main.py [options]

# As Python module
python -m bteq_dcf [options]
```

## **Processing Mode Versions**

The system uses **versioned keywords** to control processing paths:

| Mode | Description | Speed | Intelligence | Use Case |
|------|-------------|--------|--------------|----------|
| **v1_prescriptive** | Pure rule-based processing | ⚡⚡⚡ | 🧠 | Fast batch processing |
| **v2_prescriptive_enhanced** | Rules + basic AI enhancement | ⚡⚡ | 🧠🧠 | Quality improvement |
| **v3_hybrid_foundation** | Traditional + AI agents | ⚡ | 🧠🧠🧠 | **RECOMMENDED** |
| **v4_agentic_orchestrated** | Full multi-agent workflow | ⚡ | 🧠🧠🧠🧠 | Complex migrations |
| **v5_agentic_multi_model** | Multi-model consensus | ⚡ | 🧠🧠🧠🧠🧠 | Highest quality |

## **Usage Examples**

### **V1: Pure Rule-Based (Fastest)**
```bash
# Process single file
python bteq_migrate.py --input my_script.bteq --mode v1_prescriptive

# Process directory
python bteq_migrate.py --input /path/to/bteq_files/ --mode v1_prescriptive
```

### **V2: Rules + Basic AI Enhancement**
```bash
python bteq_migrate.py --input my_script.bteq --mode v2_prescriptive_enhanced
```

### **V3: Hybrid Foundation (RECOMMENDED)**
```bash
# Single file with hybrid processing
python bteq_migrate.py --input my_script.bteq --mode v3_hybrid_foundation

# Directory with custom output
python bteq_migrate.py --input /path/to/files/ --mode v3_hybrid_foundation \\
  --output ./migration_results --config ./my_config.cfg
```

### **V4: Full Agentic Orchestration**
```bash
python bteq_migrate.py --input /path/to/files/ --mode v4_agentic_orchestrated \\
  --model-strategy quality_first --validation --error-correction
```

### **V5: Advanced Multi-Model Consensus**
```bash
python bteq_migrate.py --input /path/to/files/ --mode v5_agentic_multi_model \\
  --model-strategy performance_first --multi-model --complexity-target high
```

## **Key Parameters**

### **Required**
- `--input` / `-i`: BTEQ file or directory path
- `--mode` / `-m`: Processing mode version (default: `v3_hybrid_foundation`)

### **Optional**
- `--output` / `-o`: Output directory (default: `./output`)
- `--config` / `-c`: Configuration file (default: `./config.cfg`)

### **AI Configuration (for v3/v4/v5)**
- `--model-strategy`: `quality_first` | `performance_first` | `balanced` | `cost_optimized`
- `--multi-model`: Enable multi-model consensus
- `--validation`: Enable validation agents (default: true)
- `--error-correction`: Enable error correction (default: true)
- `--complexity-target`: `low` | `medium` | `high`

### **Logging**
- `--verbose` / `-v`: Detailed output
- `--log-level`: `DEBUG` | `INFO` | `WARNING` | `ERROR`
- `--dry-run`: Analysis only, no output files

## **Processing Mode Details**

### **V1_PRESCRIPTIVE: Pure Rule-Based**
- **Components**: Variable substitution → BTEQ parsing → SQL transpilation → Basic SP generation
- **Speed**: <1 second per file
- **Output**: Basic stored procedures with core functionality
- **Best for**: Large batch processing, simple scripts

### **V2_PRESCRIPTIVE_ENHANCED: Rules + Basic AI**
- **Components**: V1 pipeline + AI quality enhancement
- **Speed**: 2-3 seconds per file
- **Output**: Enhanced procedures with improved structure
- **Best for**: Balance of speed and quality

### **V3_HYBRID_FOUNDATION: Traditional + AI Agents**
- **Components**: V1 pipeline + Multi-agent enhancement
- **Speed**: 3-5 seconds per file
- **Output**: Production-ready procedures with error handling, logging
- **Best for**: Most production migrations (RECOMMENDED)

### **V4_AGENTIC_ORCHESTRATED: Full Multi-Agent**
- **Components**: Analysis → Generation → Validation → Error correction
- **Speed**: 5-8 seconds per file
- **Output**: High-quality procedures with comprehensive optimization
- **Best for**: Complex migrations requiring deep analysis

### **V5_AGENTIC_MULTI_MODEL: Advanced Consensus**
- **Components**: V4 + Multi-model comparison + Quality consensus
- **Speed**: 8-12 seconds per file
- **Output**: Highest quality with model consensus selection
- **Best for**: Critical migrations requiring maximum quality

## **Parameter-Based Routing Logic**

```python
# The system routes based on mode keywords:
if mode == "v1_prescriptive":
    → Pure rule-based pipeline
elif mode == "v2_prescriptive_enhanced": 
    → Rule-based + basic AI enhancement
elif mode == "v3_hybrid_foundation":
    → Traditional foundation + AI agents
elif mode == "v4_agentic_orchestrated":
    → Full multi-agent workflow
elif mode == "v5_agentic_multi_model":
    → Multi-model consensus + error correction
```

## **Output Structure**

Each mode generates structured output:

```
./output/migration_results_20250121_143022/
├── migration_results_20250121_143022.json    # Complete results summary
├── enhanced_procedures/                       # Generated stored procedures
├── analysis_reports/                         # Migration analysis
├── validation_reports/                       # Quality validation
└── logs/                                    # Execution logs
```

## **Legacy Compatibility**

The system maintains backward compatibility:
- `--mode prescriptive` → `v1_prescriptive`  
- `--mode agentic` → `v4_agentic_orchestrated`
- `--mode hybrid` → `v3_hybrid_foundation`

## **Quick Start**

1. **Test connectivity**: `python bteq_migrate.py --help`
2. **Process single file**: `python bteq_migrate.py --input test.bteq --mode v3_hybrid_foundation`
3. **Process directory**: `python bteq_migrate.py --input ./bteq_files/ --mode v3_hybrid_foundation`
4. **Advanced processing**: `python bteq_migrate.py --input ./files/ --mode v5_agentic_multi_model --multi-model`

## **Architecture Benefits**

### **Single Entry Point**
- ✅ One command for all processing modes
- ✅ Clear versioned progression path
- ✅ Parameter-based routing logic
- ✅ Consistent interface across all modes

### **Flexible Processing**
- ⚡ **Speed**: Choose appropriate mode for requirements
- 🎯 **Quality**: Scale AI involvement based on needs
- 💰 **Cost**: Optimize compute and API usage
- 🔄 **Compatibility**: Support for legacy and new workflows

This unified entry point provides **clear, versioned control** over the entire BTEQ migration pipeline with **parameter-based routing** exactly as requested!
