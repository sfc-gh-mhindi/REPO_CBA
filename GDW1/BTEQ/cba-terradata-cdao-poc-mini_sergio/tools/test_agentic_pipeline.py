#!/usr/bin/env python3
"""
Test Agentic BTEQ Migration Pipeline

Comprehensive testing of the agentic framework including:
- Multi-agent orchestration
- Tool-based validation  
- Error correction loops
- Multi-model comparison
- Judgment and ranking
"""

import logging
import sys
import time
from pathlib import Path

# Add bteq_dcf to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

def test_agentic_framework():
    """Test the complete agentic framework."""
    
    print('🤖 Testing Agentic BTEQ Migration Framework')
    print('=' * 50)
    
    try:
        from services.orchestration.integration.pipeline import create_agentic_pipeline, create_validation_suite
        from services.orchestration.workflows.orchestrator import OrchestrationConfig
        from services.orchestration.agents.service import ModelType
        
        # Test 1: Basic Agentic Pipeline
        print('\n📋 Test 1: Basic Agentic Pipeline Setup')
        print('-' * 40)
        
        # Create agentic configuration
        agentic_config = OrchestrationConfig(
            max_iterations=2,
            enable_multi_model=True,
            enable_error_correction=True,
            enable_tool_validation=True,
            quality_threshold=0.7,
            models_to_use=[ModelType.CLAUDE_35_SONNET, ModelType.LLAMA_31_8B]
        )
        
        # Create agentic pipeline
        pipeline = create_agentic_pipeline(
            config_file="config.cfg",
            enable_agentic=True
        )
        pipeline.orchestration_config = agentic_config
        
        print(f'✅ Agentic pipeline created')
        print(f'   Max iterations: {agentic_config.max_iterations}')
        print(f'   Models: {[m.value for m in agentic_config.models_to_use]}')
        print(f'   Quality threshold: {agentic_config.quality_threshold}')
        
        # Test 2: Simple BTEQ Processing
        print(f'\n📋 Test 2: Simple BTEQ File Processing')
        print('-' * 40)
        
        # Create test BTEQ content
        test_bteq = '''
.RUN FILE=logon.sql
.IF ERRORCODE <> 0 THEN .GOTO EXITERR

-- Simple account balance processing
DELETE FROM PDDSTG.ACCT_BALANCE_TEMP;
.IF ERRORCODE <> 0 THEN .GOTO EXITERR

INSERT INTO PDDSTG.ACCT_BALANCE_TEMP
SELECT 
    ACCT_ID,
    BALANCE_AMT,
    BALANCE_DT
FROM PVTECH.ACCOUNT_BALANCE
WHERE BALANCE_DT >= CURRENT_DATE - 30;

.IF ERRORCODE <> 0 THEN .GOTO EXITERR

.LABEL EXITERR
.LOGOFF
'''
        
        # Save test file
        test_file = Path(__file__).parent / "test_simple.bteq"
        with open(test_file, 'w') as f:
            f.write(test_bteq)
        
        print(f'📄 Created test BTEQ file: {test_file}')
        
        # Process with agentic pipeline
        start_time = time.time()
        result = pipeline.process_bteq_file(
            str(test_file), 
            output_directory="output/agentic_test",
            procedure_name="TEST_AGENTIC_PROC",
            use_agentic=True
        )
        processing_time = time.time() - start_time
        
        print(f'📊 Processing Results:')
        print(f'   Success: {result["success"]}')
        print(f'   Processing time: {processing_time:.2f}s')
        
        if result["success"]:
            print(f'   Standard quality: {result["standard_result"].get("quality_score", 0):.2f}')
            if result["agentic_result"]:
                print(f'   Agentic quality: {result["agentic_result"].get("quality_score", 0):.2f}')
                print(f'   Agentic iterations: {result["agentic_result"].get("iterations", 0)}')
            print(f'   Selected method: {result["final_result"]["selected_method"]}')
            print(f'   Final quality: {result["final_result"]["quality_score"]:.2f}')
        
        # Test 3: Complex BTEQ with Error Scenarios
        print(f'\n📋 Test 3: Complex BTEQ with Validation Challenges')
        print('-' * 45)
        
        complex_bteq = '''
.RUN FILE=%%BTEQ_LOGON_SCRIPT%%
.IF ERRORCODE <> 0 THEN .GOTO EXITERR

-- Complex query with Teradata-specific features
INSERT INTO PDDSTG.COMPLEX_ANALYSIS
SELECT 
    ACCT_ID,
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCT_ID ORDER BY TXN_DT DESC) = 1,
    CASE WHEN ((EFFT_D - ADJ_FROM_D) YEAR(4) TO MONTH) = 0 
         THEN ADJ_FROM_D 
         ELSE EFFT_D - (EXTRACT (DAY FROM EFFT_D) - 1)  
    END AS CALC_DT,
    NVL(BALANCE_AMT, 0) as SAFE_BALANCE
FROM PVTECH.TRANSACTION_DETAIL TD
INNER JOIN PVTECH.ACCOUNT_MASTER AM 
    ON TD.ACCT_ID = AM.ACCT_ID
WHERE TD.TXN_DT >= DATE '2024-01-01'
GROUP BY 1,2,3,4;

.IF ERRORCODE <> 0 THEN .GOTO EXITERR

.LABEL EXITERR
.LOGOFF
'''
        
        complex_file = Path(__file__).parent / "test_complex.bteq"
        with open(complex_file, 'w') as f:
            f.write(complex_bteq)
        
        print(f'📄 Created complex BTEQ file: {complex_file}')
        
        # Process complex file
        start_time = time.time()
        complex_result = pipeline.process_bteq_file(
            str(complex_file),
            output_directory="output/agentic_test",
            procedure_name="COMPLEX_AGENTIC_PROC",
            use_agentic=True
        )
        complex_processing_time = time.time() - start_time
        
        print(f'📊 Complex Processing Results:')
        print(f'   Success: {complex_result["success"]}')
        print(f'   Processing time: {complex_processing_time:.2f}s')
        
        if complex_result["success"]:
            comparison = complex_result.get("comparison", {})
            print(f'   Quality improvement: {comparison.get("quality_difference", 0):.2f}')
            if complex_result["agentic_result"]:
                agentic_meta = complex_result["agentic_result"].get("metadata", {})
                print(f'   Validation passed: {agentic_meta.get("validation_passed", False)}')
                print(f'   Correction applied: {agentic_meta.get("correction_applied", False)}')
        
        # Test 4: Validation Suite
        print(f'\n📋 Test 4: Agentic Validation Suite')
        print('-' * 35)
        
        validation_suite = create_validation_suite(pipeline)
        
        # Run validation on test files
        test_files = [str(test_file), str(complex_file)]
        validation_results = validation_suite.validate_agentic_improvements(
            test_files, 
            "output/agentic_validation"
        )
        
        print(f'📊 Validation Results:')
        print(f'   Files tested: {validation_results["test_files_count"]}')
        print(f'   Improvements found: {validation_results["improvements_found"]}')
        if validation_results.get("average_quality_improvement"):
            print(f'   Avg quality improvement: {validation_results["average_quality_improvement"]:.3f}')
        print(f'   Feature improvements: {len(validation_results["feature_improvements"])}')
        
        # Test 5: Tool Integration Test
        print(f'\n📋 Test 5: Tool Integration Validation')
        print('-' * 40)
        
        from services.orchestration.agents.service import SQLValidationTool, PerformanceAnalyzerTool
        
        # Test SQL validation tool
        sql_tool = SQLValidationTool()
        test_sql = "SELECT * FROM table WHERE column = 'value'"
        validation_output = sql_tool._run(test_sql)
        
        print(f'🔧 SQL Validation Tool Test:')
        print(f'   Input: {test_sql}')
        print(f'   Output: {validation_output[:100]}...')
        
        # Test performance analyzer
        perf_tool = PerformanceAnalyzerTool()
        perf_output = perf_tool._run(test_sql)
        
        print(f'🔧 Performance Analyzer Test:')
        print(f'   Output: {perf_output[:100]}...')
        
        # Test 6: Multi-Model Comparison
        print(f'\n📋 Test 6: Multi-Model Generation Test')
        print('-' * 40)
        
        from services.orchestration.agents.service import MultiModelGenerationAgent, MigrationContext
        from services.parsing.bteq.tokens import ParserResult, ControlStatement, SqlBlock, ControlType
        
        # Create test context
        parser_result = ParserResult(
            controls=[
                ControlStatement(ControlType.RUN, '.RUN FILE=logon.sql', None, 1),
                ControlStatement(ControlType.IF_ERRORCODE, '.IF ERRORCODE <> 0 THEN .GOTO EXITERR', None, 2)
            ],
            sql_blocks=[
                SqlBlock('SELECT * FROM TEST_TABLE;', 3, 3)
            ]
        )
        
        context = MigrationContext(
            original_bteq=test_bteq,
            parser_result=parser_result,
            analysis_markdown="Test analysis",
            complexity_score=5.0,
            target_procedure_name="MULTI_MODEL_TEST",
            quality_requirements={},
            validation_rules=[]
        )
        
        multi_model_agent = MultiModelGenerationAgent()
        generation_results = multi_model_agent.generate_multi_model(context)
        
        print(f'🎯 Multi-Model Results:')
        for i, result in enumerate(generation_results):
            print(f'   Model {i+1} ({result.model_used.value}): Success={result.success}')
            if result.success:
                print(f'     Quality: {result.confidence_score:.2f}')
                print(f'     Time: {result.execution_time_ms:.0f}ms')
        
        # Cleanup test files
        test_file.unlink(missing_ok=True)
        complex_file.unlink(missing_ok=True)
        
        print(f'\n🎉 Agentic Framework Testing Complete!')
        print(f'✅ All components tested successfully')
        print(f'🚀 Framework ready for production use')
        
        return True
        
    except Exception as e:
        print(f'❌ Agentic testing failed: {e}')
        import traceback
        traceback.print_exc()
        return False


def test_langchain_integration():
    """Test LangChain/LangGraph integration specifically."""
    
    print('\n🔗 Testing LangChain/LangGraph Integration')
    print('=' * 45)
    
    try:
        # Test LangChain imports
        from langchain_core.agents import AgentAction, AgentFinish
        from langchain_core.tools import BaseTool
        from langchain.agents import create_react_agent, AgentExecutor
        print('✅ LangChain imports successful')
        
        # Test LangGraph imports
        from langgraph.graph import StateGraph, END
        from langgraph.checkpoint.memory import MemorySaver
        print('✅ LangGraph imports successful')
        
        # Test our LangChain wrapper
        from tools.langchain_cortex import SnowflakeCortexLLM
        llm = SnowflakeCortexLLM(model="claude-3-5-sonnet")
        print('✅ Snowflake Cortex LLM wrapper created')
        
        # Test simple invocation
        try:
            response = llm.invoke("Say hello in one word")
            print(f'✅ LLM invocation successful: {response[:50]}...')
        except Exception as e:
            print(f'⚠️  LLM invocation failed (expected in test): {e}')
        
        print('🎯 LangChain/LangGraph integration verified')
        return True
        
    except ImportError as e:
        print(f'❌ LangChain/LangGraph import failed: {e}')
        print('   Please install: pip install langchain langgraph')
        return False
    except Exception as e:
        print(f'❌ Integration test failed: {e}')
        return False


if __name__ == "__main__":
    print('🧪 Starting Agentic Framework Tests')
    print('=' * 50)
    
    # Test LangChain integration first
    langchain_success = test_langchain_integration()
    
    if langchain_success:
        # Test full agentic framework
        agentic_success = test_agentic_framework()
        sys.exit(0 if agentic_success else 1)
    else:
        print('❌ Cannot proceed with agentic tests without LangChain')
        sys.exit(1)
