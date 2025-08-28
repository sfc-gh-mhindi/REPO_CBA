# BTEQ DCF Codebase Analysis

## Architecture Overview: Prescriptive vs LLM-Based Components

| Module/File | Type | Functionality | Usage Status | Key Classes/Functions | Description |
|-------------|------|---------------|--------------|---------------------|-------------|
| **🏗️ CORE INFRASTRUCTURE** |
| `utils/database/snowflake_connection.py` | Prescriptive | Database connectivity | ✅ **ACTIVE** | `SnowflakeConnectionManager` | JWT-based Snowflake connection management for all components |
| `utils/config/config_manager.py` | Prescriptive | Configuration management | ✅ **ACTIVE** | `ConfigManager` | Application settings and database configurations |
| `utils/config/model_manager.py` | Prescriptive | Model selection logic | ✅ **ACTIVE** | `ModelManager`, `TaskType`, `ModelSelectionStrategy` | AI model routing and configuration management |
| `utils/logging/llm_logger.py` | Prescriptive | Logging infrastructure | ✅ **ACTIVE** | `LLMLogger` | Comprehensive LLM interaction logging and debugging |
| **📝 TRADITIONAL PIPELINE (Prescriptive/Rule-Based)** |
| `substitution/substitution_service.py` | Prescriptive | Variable substitution | ✅ **ACTIVE** | `SubstitutionService` | Regex-based BTEQ variable replacement (%%VAR%% → schema.table) |
| `substitution/pipeline.py` | Hybrid | Pipeline orchestration | ✅ **ACTIVE** | `SubstitutionPipeline` | Main orchestrator for traditional + optional LLM enhancement |
| `substitution/cli.py` | Prescriptive | CLI interface | ✅ **ACTIVE** | `main()`, command handlers | Primary CLI entry point for production use |
| `parser/bteq_lexer.py` | Prescriptive | BTEQ tokenization | ✅ **ACTIVE** | `lex_bteq()`, pattern matching | Grammar-based BTEQ control statement parsing |
| `parser/tokens.py` | Prescriptive | Data structures | ✅ **ACTIVE** | `ControlStatement`, `SqlBlock`, `ParserResult` | Type definitions for parsed BTEQ components |
| `parser/parser_service.py` | Prescriptive | SQL transpilation | ✅ **ACTIVE** | `ParserService` | SQLGlot-based Teradata → Snowflake SQL conversion |
| `parser/cli.py` | Prescriptive | Parser CLI | 🟡 **LIMITED** | `main()` | Standalone parser tool (used for testing) |
| `generator/snowflake_sp_generator.py` | Prescriptive | Basic SP generation | ✅ **ACTIVE** | `SnowflakeSPGenerator` | Template-based stored procedure generation |
| **🤖 AGENTIC AI FRAMEWORK (LLM-Based)** |
| `agentic/orchestrator.py` | LLM-Based | Multi-agent orchestration | ✅ **ACTIVE** | `AgenticOrchestrator` | LangGraph-based state management and agent coordination |
| `agentic/agents.py` | LLM-Based | AI agent implementations | ✅ **ACTIVE** | `BteqAnalysisAgent`, `MultiModelGenerationAgent`, `ValidationAgent` | Core AI agents for analysis, generation, validation, correction |
| `agentic/integration.py` | LLM-Based | Agent integration | ✅ **ACTIVE** | `AgenticMigrationPipeline` | Integration layer between traditional pipeline and agents |
| `generator/llm_enhanced_generator.py` | LLM-Based | AI-powered generation | ✅ **ACTIVE** | `LLMEnhancedGenerator` | Context-aware code enhancement using LLMs |
| `generator/cortex_direct_integration.py` | LLM-Based | Snowflake Cortex API | ✅ **ACTIVE** | `DirectCortexLLMService` | Direct Snowflake Cortex function integration |
| `generator/llm_integration.py` | LLM-Based | Generic LLM interface | ✅ **ACTIVE** | `SnowflakeLLMService` | Abstraction layer for various LLM providers |
| `tools/langchain_cortex_direct.py` | LLM-Based | LangChain integration | ✅ **ACTIVE** | `SnowflakeCortexDirectLLM` | LangChain wrapper for Snowflake Cortex |
| **🔧 PRODUCTION TOOLS & TESTING** |
| `tools/run_real_agentic_with_logging.py` | Hybrid | Production execution | ✅ **ACTIVE** | `main()`, pipeline runners | Primary production script for agentic processing |
| `tools/analyze_all_bteq.py` | Prescriptive | Batch analysis | ✅ **ACTIVE** | Batch processing functions | Mass analysis of BTEQ files with reporting |
| `tools/cortex_playground.py` | LLM-Based | Interactive testing | 🟡 **DEVELOPMENT** | `CortexPlayground` | Interactive LLM testing and experimentation |
| `tools/test_cortex_integration.py` | LLM-Based | Integration testing | 🟡 **TESTING** | Test suite functions | Comprehensive Cortex connectivity and functionality tests |
| `tools/test_agentic_pipeline.py` | LLM-Based | Pipeline testing | 🟡 **TESTING** | Agent workflow tests | End-to-end agentic pipeline validation |
| **📊 DEMO & DEVELOPMENT TOOLS** |
| `tools/demo_agentic_capabilities.py` | LLM-Based | Capability demo | 🟡 **DEMO** | Demo functions | Showcases agentic framework capabilities |
| `tools/demo_parameterized_models.py` | LLM-Based | Model comparison | 🟡 **DEMO** | Model comparison functions | Demonstrates multi-model selection strategies |
| `tools/example_agentic_usage.py` | LLM-Based | Usage examples | 🟡 **DEMO** | Example implementations | Code examples for agentic framework usage |
| `tools/demo_live_prompts.py` | LLM-Based | Prompt testing | 🟡 **DEMO** | Interactive prompt testing | Real-time prompt engineering and testing |
| `generator/demo_*.py` | LLM-Based | Generator demos | 🟡 **DEMO** | Various demo functions | Development demonstrations of generation capabilities |
| **❌ LEGACY/UNUSED COMPONENTS** |
| `cli.py` (root) | Prescriptive | Legacy CLI | ❌ **UNUSED** | `main()` | Original CLI - superseded by substitution/cli.py |
| `__main__.py` | Prescriptive | Module entry | ❌ **UNUSED** | Module runner | Unused module entry point |
| `generator/cli.py` | Prescriptive | Generator CLI | ❌ **UNUSED** | Generator commands | Standalone generator CLI - not used in main flows |
| `tools/cortex_rest_test.py` | LLM-Based | REST API testing | ❌ **DEVELOPMENT** | REST test functions | Development artifact for API testing |
| `tools/cortex_test.py` | LLM-Based | Basic Cortex test | ❌ **DEVELOPMENT** | Basic test functions | Superseded by more comprehensive test tools |
| **📁 CONFIGURATION & DATA** |
| `utils/config/application.properties` | Prescriptive | App configuration | ✅ **ACTIVE** | Properties file | LLM model settings and application parameters |
| `utils/config/models.json` | Prescriptive | Model definitions | ✅ **ACTIVE** | JSON configuration | Model metadata and performance characteristics |
| `config.cfg` | Prescriptive | Variable mappings | ✅ **ACTIVE** | Config file | Teradata → Snowflake schema mappings |

## Usage Patterns & Entry Points

### Primary Production Entry Points ✅
1. **Traditional Pipeline**: `python -m bteq_dcf.substitution.cli run-pipeline`
2. **Agentic Processing**: `python tools/run_real_agentic_with_logging.py`
3. **Batch Analysis**: `python tools/analyze_all_bteq.py`

### Development & Testing Tools 🟡
- `tools/cortex_playground.py` - Interactive LLM testing
- `tools/test_*` - Comprehensive test suites
- `tools/demo_*` - Capability demonstrations

### Unused/Legacy Components ❌
- Root `cli.py` and `__main__.py` - Superseded by module-specific CLIs
- Some `generator/demo_*` files - Development artifacts
- Basic test files - Replaced by comprehensive test suites

## Architecture Insights

### Why This Hybrid Approach Works
| Component Type | Characteristics | Examples | Rationale |
|----------------|----------------|-----------|-----------|
| **Prescriptive** | Fast, deterministic, rule-based | Variable substitution, BTEQ parsing, SQLGlot transpilation | Perfect for well-defined transformations |
| **LLM-Based** | Context-aware, adaptive, quality-focused | Code enhancement, validation, error correction | Essential for complex reasoning and quality |
| **Hybrid** | Orchestrates both types | Pipeline management, integration layers | Combines speed of rules with intelligence of AI |

### Model Usage Strategy
- **Claude-4-Sonnet**: High-quality analysis and generation (2.8s avg, 95% quality)
- **Snowflake-Llama-3.3-70B**: Performance-optimized generation (1.9s avg, 85% quality)
- **Multi-Model**: Parallel processing with quality comparison and selection

### Code Health Assessment
- **✅ Production Ready**: 23 active components with clear responsibilities
- **🟡 Development/Testing**: 12 components for development and validation
- **❌ Technical Debt**: 6 unused/legacy components that can be removed

## Recommendations

### Immediate Actions
1. **Remove Legacy Components**: Delete unused CLI files and basic test artifacts
2. **Consolidate Demos**: Merge demo files into comprehensive examples
3. **Document Entry Points**: Clear documentation of production vs development tools

### Architecture Improvements
1. **Component Boundaries**: Clear separation between prescriptive and LLM-based logic
2. **Configuration Management**: Centralized model and pipeline configuration
3. **Testing Strategy**: Maintain separation between unit tests and integration demos
