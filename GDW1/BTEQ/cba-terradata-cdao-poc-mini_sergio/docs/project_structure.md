# Project Structure & Codebase Analysis

## CBA Teradata CDAO POC - BTEQ to Snowflake Migration

This project is organized into several key modules to support the automated migration of BTEQ scripts to Snowflake using AI-powered agents. The architecture combines **prescriptive/rule-based components** (fast, deterministic) with **LLM-based agentic components** (context-aware, adaptive).

## Root Directory Structure

```
bteq_agentic/
├── agentic/                     # AI Agent System
├── analysis/                    # Analysis and reporting
├── generator/                   # Code generation services
├── parser/                      # BTEQ parsing and tokenization  
├── substitution/                # Template substitution system
├── utils/                       # Utilities and infrastructure
├── tools/                       # Testing and development utilities
├── docs/                        # Project documentation
├── output/                      # Generated outputs and migration results
├── references/                  # Reference materials and legacy assets
│   ├── current_state/          # Original BTEQ/SQL source files
│   ├── gdw1_xfm_frmw/          # dbt transformation framework
│   └── snowconv_bteq/          # SnowConvert output and reports
├── main.py                     # Main entry point
├── bteq_migrate.py             # Migration script entry point
├── cli.py                      # Command-line interface
├── config.cfg                  # Configuration file
├── requirements.txt            # Python dependencies
└── README.md                   # Main project documentation
```

## Core Modules Structure

The main application logic is organized into focused modules at the root level:

### `agentic/` - AI Agent System
```
agentic/
├── __init__.py
├── agents.py                   # Core agent implementations
├── integration.py              # Agent integration logic
├── orchestrator.py             # Multi-agent orchestration
└── README.md
```

### `analysis/` - Analysis and Reporting
```
analysis/
├── __init__.py
├── reports/                    # Generated analysis reports
└── README.md
```

### `generator/` - Code Generation Services
```
generator/
├── __init__.py
├── cli.py
├── generator_service.py
├── llm_enhanced_generator.py
├── snowflake_sp_generator.py
├── cortex_direct_integration.py
└── [additional generator modules]
```

### `parser/` - BTEQ Parsing and Tokenization
```
parser/
├── __init__.py
├── bteq_lexer.py
├── parser_service.py
├── td_sql_parser.py
└── tokens.py
```

### `substitution/` - Template Substitution System
```
substitution/
├── __init__.py
├── cli.py
├── pipeline.py
└── substitution_service.py
```

### `utils/` - Utilities and Infrastructure
```
utils/
├── __init__.py
├── config/                     # Configuration management
│   ├── __init__.py
│   ├── application.properties
│   ├── config_manager.py
│   ├── model_manager.py
│   └── models.json
├── database/                   # Database connectivity
│   ├── __init__.py
│   └── snowflake_connection.py
└── logging/                    # Logging infrastructure
    ├── __init__.py
    └── llm_logger.py
```

### `tools/` - Testing and Development Utilities
```
tools/
├── __init__.py
├── README_TESTING.md
├── test_*.py                   # Unit and integration tests
├── demo_*.py                   # Demo and example scripts
├── run_*.py                    # Execution utilities
└── analyze_*.py                # Analysis utilities
```

## Reference Materials: `references/`

Legacy assets and reference materials have been organized in the references directory:

### Data Transformation Framework: `references/gdw1_xfm_frmw/`

dbt-based transformation framework for data processing:

```
references/gdw1_xfm_frmw/
├── dbt_project.yml            # dbt project configuration
├── profiles_template.yml      # Connection profiles template
│
├── models/                    # dbt models
│   ├── ccods/                # CCODS domain models
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── csel4/                # CSEL4 domain models
│       ├── staging/
│       ├── intermediate/
│       ├── marts/
│       └── reference/
│
├── macros/                    # dbt macros
│   └── dcf/                  # Domain-specific macros
│
├── schema_definitions/        # DDL and schema files
├── tools/                    # Framework utilities
└── _docs/                    # Framework documentation
```

### Source Assets: `references/current_state/`

Original source files and extracted content:

```
references/current_state/
├── bteq_bteq/                 # Original BTEQ scripts
└── bteq_sql/                  # Extracted SQL from BTEQ
```

### SnowConvert Output: `references/snowconv_bteq/`

SnowConvert conversion results and reports:

```
references/snowconv_bteq/
├── bteq-[timestamp]/          # BTEQ conversion outputs
├── sql-[timestamp]/           # SQL conversion outputs  
└── Reports/                   # Conversion assessment reports
```

## Testing and Development Guidelines

### Testing Scripts Location
**IMPORTANT**: All testing scripts, demo scripts, and development utilities are centralized in the root-level `tools/` directory:

- **All testing scripts**: Place in `tools/`
- **Demo and example scripts**: Place in `tools/`
- **Development utilities**: Place in `tools/`
- **Framework-specific utilities**: Place in `references/gdw1_xfm_frmw/tools/` for dbt-related tools

### Testing Script Naming Conventions
- Test scripts: `test_*.py`
- Demo scripts: `demo_*.py`
- Utility scripts: Use descriptive names (e.g., `analyze_all_bteq.py`)
- Development/debug scripts: `dev_*.py` or `debug_*.py`
- Execution scripts: `run_*.py`

### Script Organization Rules
1. **Production Scripts**: Core execution scripts at project root (`main.py`, `bteq_migrate.py`, `cli.py`)
2. **Testing & Development**: All testing, demo, and development scripts in `tools/` directory
3. **Centralized Tools**: Single location for all development utilities makes them easier to find and manage
4. **Consistent Naming**: Use prefixes (`test_`, `demo_`, `run_`) to clearly identify script purpose

### File Organization Principles

1. **Modular Structure**: Each major functionality area has its own module
2. **Clear Separation**: Business logic, configuration, and utilities are separated
3. **Searchable Code**: Descriptive file and function names for easy navigation
4. **Documentation**: Each module includes README.md with usage instructions
5. **Testing Isolation**: All testing code is contained within `tools/` directories

## Key Design Patterns

### Agent-Based Architecture
The system uses specialized AI agents for different aspects of the migration:
- **BteqAnalysisAgent**: Analyzes BTEQ complexity and patterns
- **MultiModelGenerationAgent**: Generates Snowflake code using multiple models
- **ValidationAgent**: Validates generated SQL syntax and logic
- **OrchestrationAgent**: Coordinates multi-agent workflows

### Logging and Observability
Comprehensive logging system for debugging and monitoring:
- **LLM Logger**: Captures all AI model interactions
- **Structured Logging**: JSON-formatted logs for analysis
- **Performance Metrics**: Response times and token usage tracking

### Configuration Management
Centralized configuration system:
- **Model Configuration**: AI model settings and parameters
- **Environment Settings**: Database connections and credentials
- **Feature Flags**: Enable/disable experimental features

## Dependencies

### Core Dependencies
- **Python 3.8+**: Core runtime
- **Pandas/NumPy**: Data processing
- **Snowflake Connector**: Database integration
- **LangChain**: LLM orchestration

### Development Dependencies
- **dbt**: Data transformation framework
- **Pytest**: Testing framework
- **Black/Flake8**: Code formatting and linting

## Getting Started

1. **Environment Setup**: Configure Python environment and dependencies
2. **Configuration**: Set up `config/application.properties` with your credentials
3. **Testing**: Run test scripts in `tools/` to verify setup
4. **Usage**: Use CLI interface or import modules programmatically

For detailed usage instructions, see the main [README.md](../../README.md) and module-specific documentation.

## Production Execution

The main production script is `bteq_dcf/run_real_agentic_with_logging.py` which provides:
- Complete agentic pipeline execution
- Comprehensive logging and monitoring
- Real BTEQ file processing
- Multi-agent coordination
- Error handling and recovery

For development and testing, use the scripts in `bteq_dcf/tools/` directory.

## 📊 Comprehensive Codebase Analysis

### Component Classification & Usage Status

| Module/File | Type | Status | Functionality | Key Components |
|-------------|------|--------|---------------|----------------|
| **🏗️ CORE INFRASTRUCTURE** |||||
| `utils/database/snowflake_connection.py` | Prescriptive | ✅ **ACTIVE** | Database connectivity | `SnowflakeConnectionManager` - JWT-based Snowflake connection management |
| `utils/config/config_manager.py` | Prescriptive | ✅ **ACTIVE** | Configuration management | `ConfigManager` - Application settings and database configurations |
| `utils/config/model_manager.py` | Prescriptive | ✅ **ACTIVE** | Model selection logic | `ModelManager`, `TaskType` - AI model routing and configuration |
| `utils/logging/llm_logger.py` | Prescriptive | ✅ **ACTIVE** | Logging infrastructure | `LLMLogger` - LLM interaction logging and debugging |
| **📝 TRADITIONAL PIPELINE (Prescriptive/Rule-Based)** |||||
| `substitution/substitution_service.py` | Prescriptive | ✅ **ACTIVE** | Variable substitution | `SubstitutionService` - Regex-based BTEQ variable replacement |
| `substitution/pipeline.py` | Hybrid | ✅ **ACTIVE** | Pipeline orchestration | `SubstitutionPipeline` - Main orchestrator for traditional + optional LLM |
| `substitution/cli.py` | Prescriptive | ✅ **ACTIVE** | CLI interface | Primary CLI entry point for production use |
| `parser/bteq_lexer.py` | Prescriptive | ✅ **ACTIVE** | BTEQ tokenization | Grammar-based BTEQ control statement parsing |
| `parser/tokens.py` | Prescriptive | ✅ **ACTIVE** | Data structures | `ControlStatement`, `SqlBlock` - Type definitions for parsed BTEQ |
| `parser/parser_service.py` | Prescriptive | ✅ **ACTIVE** | SQL transpilation | `ParserService` - SQLGlot-based Teradata → Snowflake conversion |
| `generator/snowflake_sp_generator.py` | Prescriptive | ✅ **ACTIVE** | Basic SP generation | Template-based stored procedure generation |
| **🤖 AGENTIC AI FRAMEWORK (LLM-Based)** |||||
| `agentic/orchestrator.py` | LLM-Based | ✅ **ACTIVE** | Multi-agent orchestration | `AgenticOrchestrator` - LangGraph-based state management |
| `agentic/agents.py` | LLM-Based | ✅ **ACTIVE** | AI agent implementations | `BteqAnalysisAgent`, `MultiModelGenerationAgent`, `ValidationAgent` |
| `agentic/integration.py` | LLM-Based | ✅ **ACTIVE** | Agent integration | `AgenticMigrationPipeline` - Integration layer for agents |
| `generator/llm_enhanced_generator.py` | LLM-Based | ✅ **ACTIVE** | AI-powered generation | `LLMEnhancedGenerator` - Context-aware code enhancement |
| `generator/cortex_direct_integration.py` | LLM-Based | ✅ **ACTIVE** | Snowflake Cortex API | `DirectCortexLLMService` - Direct Cortex function integration |
| `tools/langchain_cortex_direct.py` | LLM-Based | ✅ **ACTIVE** | LangChain integration | `SnowflakeCortexDirectLLM` - LangChain wrapper for Cortex |
| **🔧 PRODUCTION TOOLS** |||||
| `tools/run_real_agentic_with_logging.py` | Hybrid | ✅ **ACTIVE** | Production execution | Primary production script for agentic processing |
| `tools/analyze_all_bteq.py` | Prescriptive | ✅ **ACTIVE** | Batch analysis | Mass analysis of BTEQ files with comprehensive reporting |
| **🟡 DEVELOPMENT & TESTING** |||||
| `tools/cortex_playground.py` | LLM-Based | 🟡 **DEVELOPMENT** | Interactive testing | Interactive LLM testing and experimentation |
| `tools/test_cortex_integration.py` | LLM-Based | 🟡 **TESTING** | Integration testing | Comprehensive Cortex connectivity validation |
| `tools/demo_agentic_capabilities.py` | LLM-Based | 🟡 **DEMO** | Capability demo | Showcases agentic framework capabilities |
| `tools/demo_parameterized_models.py` | LLM-Based | 🟡 **DEMO** | Model comparison | Multi-model selection strategy demonstrations |
| **❌ LEGACY/UNUSED** |||||
| `cli.py` (root) | Prescriptive | ❌ **UNUSED** | Legacy CLI | Original CLI - superseded by `substitution/cli.py` |
| `__main__.py` | Prescriptive | ❌ **UNUSED** | Module entry | Unused module entry point |
| `generator/cli.py` | Prescriptive | ❌ **UNUSED** | Generator CLI | Standalone generator CLI - not used in main flows |

### Architecture Insights

#### Component Type Summary
| Component Type | Count | Examples | Characteristics |
|----------------|-------|----------|----------------|
| **✅ Active Production** | 23 | Core pipeline, agentic framework | Essential functionality, actively used |  
| **🟡 Development/Testing** | 12 | Demo scripts, test suites | Development support, validation |
| **❌ Legacy/Unused** | 6 | Root CLI, basic test artifacts | Can be removed, technical debt |

#### Usage Patterns
**Primary Entry Points:**
- **Traditional Pipeline**: `python -m bteq_dcf.substitution.cli run-pipeline`
- **Agentic Processing**: `python tools/run_real_agentic_with_logging.py`  
- **Batch Analysis**: `python tools/analyze_all_bteq.py`

**Development Tools:**
- **Testing**: `tools/test_*.py` - Comprehensive validation suites
- **Demos**: `tools/demo_*.py` - Capability demonstrations
- **Interactive**: `tools/cortex_playground.py` - LLM experimentation

#### Architectural Benefits
- **⚡ Speed**: Traditional components handle 80% of work in <1s  
- **🎯 Intelligence**: AI enhances quality for complex reasoning
- **💰 Cost Efficient**: AI used only for high-value contextual tasks
- **🔄 Reliable**: Deterministic foundation with AI enhancement layer

For comprehensive analysis including detailed functionality descriptions and architectural recommendations, see: **[Complete Codebase Analysis](codebase_analysis_table.md)**