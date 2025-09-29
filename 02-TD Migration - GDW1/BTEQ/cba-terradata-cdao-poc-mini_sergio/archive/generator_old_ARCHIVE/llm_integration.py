"""
LLM Integration for BTEQ to Snowflake Enhancement

Provides interface to call Snowflake Cortex LLM for procedure enhancement.
"""

import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict

# Import improved logging system
from utils.logging.improved_llm_logger import (
    ImprovedLLMLogger, ConversionType, LLMRequest, LLMResponse,
    create_improved_llm_logger, log_bteq_to_sp_request
)

logger = logging.getLogger(__name__)


class SnowflakeLLMService:
    """Service for calling Snowflake Cortex LLM functions."""
    
    def __init__(self, llm_log_dir: str = None):
        self.available = self._check_llm_availability()
        self.llm_log_dir = Path(llm_log_dir) if llm_log_dir else None
        self.llm_loggers = {}  # Cache for model-specific loggers
        
        # Initialize improved logger if log directory provided
        if llm_log_dir:
            # Extract the parent directory (migration run directory) for unified logging
            run_dir = Path(llm_log_dir).parent.parent if Path(llm_log_dir).name == "llm_interactions" else Path(llm_log_dir).parent
            self.improved_logger = create_improved_llm_logger(str(run_dir))
            logger.info(f"✅ Initialized improved LLM logger for SP generation: {run_dir}/llm_interactions/")
        else:
            self.improved_logger = None
    
    def _check_llm_availability(self) -> bool:
        """Check if Snowflake Cortex functions are available."""
        try:
            # Check if we can establish Snowflake connection and use Cortex
            # Use absolute imports to fix packaging issues
            try:
                from utils.database import get_connection_manager
            except ImportError:
                # Fallback for when running as part of package
                from utils.database import get_connection_manager
            
            connection_manager = get_connection_manager()
            
            # Test connection
            if not connection_manager.test_connection():
                logger.warning("Snowflake connection failed")
                return False
            
            # Test Cortex availability
            if not connection_manager.test_cortex_availability():
                logger.warning("Snowflake Cortex functions not available")
                return False
            
            logger.info("Snowflake Cortex functions available via connection")
            return True
            
        except ImportError as e:
            logger.warning(f"Snowflake libraries not available: {e}")
            return False
        except Exception as e:
            logger.warning(f"Error checking Snowflake Cortex availability: {e}")
            return False
    
    def _setup_llm_logger(self, model: str) -> logging.Logger:
        """Set up dedicated logger for specific LLM model interactions."""
        if model in self.llm_loggers:
            return self.llm_loggers[model]
        
        # Create model-specific logger
        model_safe = model.replace("-", "_").replace(".", "_")
        logger_name = f"llm_interactions.{model_safe}"
        llm_logger = logging.getLogger(logger_name)
        llm_logger.setLevel(logging.INFO)
        
        # Avoid duplicate handlers
        if llm_logger.handlers:
            self.llm_loggers[model] = llm_logger
            return llm_logger
        
        # Create log file in the run directory if available
        if self.llm_log_dir:
            self.llm_log_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = self.llm_log_dir / f"llm_{model_safe}_{timestamp}.log"
            
            # Create file handler
            file_handler = logging.FileHandler(str(log_file))
            file_handler.setLevel(logging.INFO)
            
            # Create formatter
            formatter = logging.Formatter('%(asctime)s - %(message)s')
            file_handler.setFormatter(formatter)
            
            # Add handler to logger
            llm_logger.addHandler(file_handler)
            llm_logger.propagate = False  # Prevent propagation to root logger
            
            logger.info(f"📝 Created LLM interaction log: {log_file.name}")
        
        self.llm_loggers[model] = llm_logger
        return llm_logger
    
    def _log_llm_interaction(self, model: str, interaction_type: str, content: str, metadata: dict = None):
        """Log LLM interaction to dedicated model log file."""
        llm_logger = self._setup_llm_logger(model)
        
        # Create structured log entry
        log_entry = f"""
{'='*80}
{interaction_type.upper()}: {model}
{'='*80}
Timestamp: {datetime.now().isoformat()}
Model: {model}
"""
        
        if metadata:
            for key, value in metadata.items():
                log_entry += f"{key}: {value}\n"
        
        log_entry += f"""
{'─'*80}
CONTENT:
{'─'*80}
{content}
{'─'*80}
{'='*80}
"""
        
        llm_logger.info(log_entry)
    
    def enhance_procedure(self, prompt: str, model: str = "claude-3-5-sonnet", procedure_name: str = "unknown_procedure") -> Optional[str]:
        """
        Enhance a stored procedure using Snowflake Cortex LLM.
        
        Args:
            prompt: The enhancement prompt including context
            model: The specific model to use
            procedure_name: Name of procedure being enhanced (for improved logging)
            
        Returns:
            Enhanced SQL or None if LLM not available
        """
        if not self.available:
            logger.info("Snowflake Cortex not available, using fallback")
            return None
        
        try:
            # Use Snowflake Cortex via SQL connection
            # Use absolute imports to fix packaging issues
            try:
                from utils.database import get_connection_manager
            except ImportError:
                # Fallback for when running as part of package
                from utils.database import get_connection_manager
            
            logger.info("Calling Snowflake Cortex Complete function via SQL")
            
            # Use improved logging if available
            interaction_id = None
            if self.improved_logger:
                # Log request with improved system
                interaction_id = log_bteq_to_sp_request(
                    self.improved_logger,
                    procedure_name,
                    model,
                    prompt,
                    {"enhancement_type": "stored_procedure"}
                )
            else:
                # Fallback to old logging system
                self._log_llm_interaction(
                    model=model,
                    interaction_type="PROMPT",
                    content=prompt,
                    metadata={
                        "Prompt length": f"{len(prompt)} characters",
                        "Call timestamp": datetime.now().isoformat()
                    }
                )
            
            # Brief log to main log file
            logger.info("="*80)
            logger.info("🤖 LLM PROMPT BEING SENT:")
            logger.info("="*80)
            logger.info(f"Model: {model}")
            logger.info(f"Prompt length: {len(prompt)} characters")
            logger.info("📝 Full prompt logged to dedicated LLM log file")
            logger.info("="*80)
            
            connection_manager = get_connection_manager()
            session = connection_manager.get_session()
            
            # Clean prompt for SQL
            cleaned_prompt = prompt.replace("'", "''").replace("$$", "$DOLLAR$")
            
            # Call Snowflake Cortex Complete via SQL
            cortex_query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                '{model}',
                $${cleaned_prompt}$$
            ) AS enhanced_procedure
            """
            
            start_time = time.time()
            result = session.sql(cortex_query).collect()
            processing_time_ms = int((time.time() - start_time) * 1000)
            
            if result and len(result) > 0:
                enhanced_sql = result[0]['ENHANCED_PROCEDURE']
                if enhanced_sql and enhanced_sql.strip():
                    # Use improved logging if available
                    if self.improved_logger and interaction_id:
                        llm_response = LLMResponse(
                            conversion_type=ConversionType.BTEQ_TO_SP,
                            procedure_name=procedure_name,
                            model=model,
                            response=enhanced_sql,
                            success=True,
                            processing_time_ms=processing_time_ms,
                            quality_score=0.8  # Default quality score for SP enhancement
                        )
                        self.improved_logger.log_response(llm_response, interaction_id)
                    else:
                        # Fallback to old logging system
                        self._log_llm_interaction(
                            model=model,
                            interaction_type="RESPONSE",
                            content=enhanced_sql,
                            metadata={
                                "Response length": f"{len(enhanced_sql)} characters",
                                "Response timestamp": datetime.now().isoformat(),
                                "Status": "Success"
                            }
                        )
                    
                    # Brief log to main log file
                    logger.info("="*80)
                    logger.info("🤖 LLM RESPONSE RECEIVED:")
                    logger.info("="*80)
                    logger.info(f"Response length: {len(enhanced_sql)} characters")
                    logger.info("📝 Full response logged to dedicated LLM log file")
                    logger.info("="*80)
                    logger.info("Successfully generated enhanced procedure with Snowflake Cortex")
                    return enhanced_sql.strip()
                else:
                    logger.warning("Empty response from Snowflake Cortex")
                    return None
            else:
                logger.warning("No result from Snowflake Cortex")
                return None
            
        except Exception as e:
            # Log the error to dedicated LLM log file
            self._log_llm_interaction(
                model=model,
                interaction_type="ERROR",
                content=str(e),
                metadata={
                    "Error timestamp": datetime.now().isoformat(),
                    "Status": "Failed"
                }
            )
            
            logger.error(f"Snowflake Cortex call failed: {e}")
            return None
    
    def enhance_procedure_multi_model(self, prompt: str, models: List[str] = None, procedure_name: str = "unknown_procedure") -> Dict[str, Optional[str]]:
        """
        Enhance a stored procedure using multiple Snowflake Cortex LLMs.
        
        Args:
            prompt: The enhancement prompt including context
            models: List of models to use (defaults to configured pair)
            procedure_name: Name of procedure being enhanced (for improved logging)
            
        Returns:
            Dictionary mapping model names to enhanced SQL (or None if failed)
        """
        if models is None:
            # Use default pair from configuration
            models = ["claude-4-sonnet", "snowflake-llama-3.3-70b"]
        
        logger.info(f"🔄 Starting multi-model enhancement with {len(models)} models: {models}")
        results = {}
        
        for model in models:
            logger.info(f"🤖 Calling model: {model}")
            try:
                enhanced_sql = self.enhance_procedure(prompt, model, procedure_name)
                results[model] = enhanced_sql
                
                if enhanced_sql:
                    logger.info(f"✅ {model} completed successfully ({len(enhanced_sql)} chars)")
                else:
                    logger.warning(f"⚠️  {model} returned no result")
                    
            except Exception as e:
                logger.error(f"❌ {model} failed: {e}")
                results[model] = None
        
        successful_models = [m for m, r in results.items() if r is not None]
        logger.info(f"📊 Multi-model completion: {len(successful_models)}/{len(models)} models successful")
        
        return results
    
    def _simulate_llm_enhancement(self, prompt: str) -> str:
        """Simulate LLM enhancement for demonstration."""
        
        # Extract key information from the prompt
        has_bteq_control = any(keyword in prompt for keyword in ['.IF', '.GOTO', '.LABEL', 'ERRORCODE'])
        has_sql_blocks = 'SQL Block' in prompt
        has_complex_logic = any(keyword in prompt for keyword in ['CASE', 'JOIN', 'UNION', 'QUALIFY'])
        
        # Generate enhanced procedure based on detected patterns
        enhanced_sql = """CREATE OR REPLACE PROCEDURE ENHANCED_BTEQ_MIGRATION(
    -- Enhanced parameters with comprehensive validation
    PROCESS_KEY STRING DEFAULT 'UNKNOWN_PROCESS',
    ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG',
    DEBUG_MODE BOOLEAN DEFAULT FALSE,
    BATCH_SIZE INTEGER DEFAULT 10000
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
COMMENT = 'LLM-Enhanced BTEQ Migration - Generated with AI assistance'
AS
$$
DECLARE
    -- Enhanced error handling and tracking variables
    error_code INTEGER DEFAULT 0;
    error_message STRING DEFAULT '';
    current_step STRING DEFAULT 'INITIALIZATION';
    row_count INTEGER DEFAULT 0;
    start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    end_time TIMESTAMP_NTZ;
    
    -- Business logic variables (AI-enhanced)
    result_status STRING DEFAULT 'PENDING';
    processing_batch INTEGER DEFAULT 0;
    total_batches INTEGER DEFAULT 0;
    
    -- Performance tracking
    performance_metrics OBJECT DEFAULT OBJECT_CONSTRUCT();
    
BEGIN
    -- Comprehensive input validation (AI-enhanced)
    IF PROCESS_KEY IS NULL OR PROCESS_KEY = '' THEN
        RAISE EXCEPTION 'PROCESS_KEY cannot be null or empty';
    END IF;
    
    IF BATCH_SIZE <= 0 OR BATCH_SIZE > 100000 THEN
        RAISE EXCEPTION 'BATCH_SIZE must be between 1 and 100000';
    END IF;
    
    -- Enhanced logging and audit trail
    current_step := 'AUDIT_START';
    IF DEBUG_MODE THEN
        CALL SYSTEM$LOG('INFO', 'Starting enhanced procedure for process: ' || PROCESS_KEY);
        CALL SYSTEM$LOG('INFO', 'Parameters - Batch Size: ' || BATCH_SIZE || ', Debug: ' || DEBUG_MODE);
    END IF;
    
    -- Initialize performance tracking
    performance_metrics := OBJECT_CONSTRUCT(
        'start_time', start_time,
        'process_key', PROCESS_KEY,
        'batch_size', BATCH_SIZE
    );"""

        # Add BTEQ-specific enhancements if detected
        if has_bteq_control:
            enhanced_sql += """
    
    -- AI-Enhanced BTEQ Control Flow Migration
    current_step := 'BTEQ_CONTROL_MIGRATION';
    
    -- Convert BTEQ .IF/.GOTO patterns to modern conditional logic
    -- This replaces the legacy BTEQ error handling with robust Snowflake patterns
    LET continue_processing BOOLEAN DEFAULT TRUE;
    LET control_flow_state STRING DEFAULT 'NORMAL';
    
    -- Enhanced error checking (replaces BTEQ ERRORCODE patterns)
    IF ERROR_TABLE IS NOT NULL THEN
        -- Verify error table exists and is accessible
        CALL SYSTEM$LOG('INFO', 'Validating error table: ' || ERROR_TABLE);
    END IF;"""

        if has_sql_blocks:
            enhanced_sql += """
    
    -- AI-Enhanced SQL Block Processing
    current_step := 'SQL_PROCESSING';
    
    -- Process SQL blocks with enhanced error handling and performance optimization
    -- This section would contain the actual business logic converted from BTEQ SQL
    
    -- Batch processing for large datasets (AI optimization)
    LET records_processed INTEGER DEFAULT 0;
    LET current_batch INTEGER DEFAULT 1;
    
    -- Main processing loop with intelligent batching
    WHILE continue_processing DO
        BEGIN
            -- Process current batch
            current_step := 'BATCH_' || current_batch;
            
            -- AI-optimized SQL execution would go here
            -- Original BTEQ SQL converted to Snowflake-optimized queries
            
            -- Update metrics
            records_processed := records_processed + BATCH_SIZE;
            current_batch := current_batch + 1;
            
            -- Check continuation condition
            IF records_processed >= (SELECT COUNT(*) FROM source_table_placeholder) THEN
                continue_processing := FALSE;
            END IF;
            
        EXCEPTION
            WHEN OTHER THEN
                continue_processing := FALSE;
                error_code := SQLCODE;
                error_message := 'Batch processing error: ' || SQLERRM;
                RAISE;
        END;
    END WHILE;"""

        # Add completion and cleanup
        enhanced_sql += """
    
    -- AI-Enhanced Success Path and Cleanup
    current_step := 'COMPLETION';
    end_time := CURRENT_TIMESTAMP();
    result_status := 'SUCCESS';
    
    -- Enhanced performance reporting
    performance_metrics := OBJECT_INSERT(performance_metrics, 'end_time', end_time);
    performance_metrics := OBJECT_INSERT(performance_metrics, 'duration_seconds', 
                                       DATEDIFF('seconds', start_time, end_time));
    performance_metrics := OBJECT_INSERT(performance_metrics, 'rows_processed', row_count);
    
    -- Comprehensive success logging
    IF DEBUG_MODE THEN
        CALL SYSTEM$LOG('INFO', 'Process completed successfully');
        CALL SYSTEM$LOG('INFO', 'Performance: ' || performance_metrics::STRING);
    END IF;
    
    -- Audit trail update
    INSERT INTO IDENTIFIER(:ERROR_TABLE) (
        PROCESS_KEY, ERROR_CODE, ERROR_MESSAGE, ERROR_STEP, ERROR_TIMESTAMP, METADATA
    ) VALUES (
        :PROCESS_KEY, 0, 'SUCCESS', current_step, CURRENT_TIMESTAMP(), performance_metrics
    );
    
    RETURN 'SUCCESS: Enhanced procedure completed. Rows: ' || row_count || 
           ', Duration: ' || DATEDIFF('seconds', start_time, end_time) || 's' ||
           ', Quality: AI-Enhanced';

EXCEPTION
    WHEN OTHER THEN
        error_code := SQLCODE;
        error_message := SQLERRM;
        end_time := CURRENT_TIMESTAMP();
        
        -- AI-Enhanced error handling with detailed context
        performance_metrics := OBJECT_INSERT(performance_metrics, 'error_time', end_time);
        performance_metrics := OBJECT_INSERT(performance_metrics, 'error_step', current_step);
        
        -- Comprehensive error logging
        INSERT INTO IDENTIFIER(:ERROR_TABLE) (
            PROCESS_KEY, ERROR_CODE, ERROR_MESSAGE, ERROR_STEP, ERROR_TIMESTAMP, METADATA
        ) VALUES (
            :PROCESS_KEY, error_code, error_message, current_step, CURRENT_TIMESTAMP(), performance_metrics
        );
        
        -- Return enhanced error information
        RETURN 'ERROR in ' || current_step || ': ' || error_message || 
               ' (Code: ' || error_code || ', AI-Enhanced Error Handling)';
END;
$$;"""
        
        return enhanced_sql
    
    def summarize_analysis(self, analysis_text: str) -> str:
        """Summarize BTEQ analysis for concise insights."""
        
        if not self.available:
            return "Analysis summary unavailable (Snowflake Cortex not available)"
        
        try:
            # Use absolute imports to fix packaging issues
            try:
                from utils.database import get_connection_manager
            except ImportError:
                # Fallback for when running as part of package
                from utils.database import get_connection_manager
            
            logger.info("Summarizing BTEQ analysis")
            
            connection_manager = get_connection_manager()
            session = connection_manager.get_session()
            
            # Clean text for SQL
            cleaned_text = analysis_text.replace("'", "''")[:2000]  # Limit length
            
            summary_query = f"""
            SELECT SNOWFLAKE.CORTEX.SUMMARIZE('{cleaned_text}') AS summary
            """
            
            result = session.sql(summary_query).collect()
            summary = result[0]['SUMMARY'] if result else "Unable to generate summary"
            
            return summary
            
        except Exception as e:
            logger.error(f"Analysis summarization failed: {e}")
            return f"Summary generation failed: {e}"
    
    def extract_key_insights(self, analysis_text: str, question: str) -> str:
        """Extract specific insights from analysis using Q&A."""
        
        if not self.available:
            return "Insights unavailable (Snowflake Cortex not available)"
        
        try:
            # Use absolute imports to fix packaging issues
            try:
                from utils.database import get_connection_manager
            except ImportError:
                # Fallback for when running as part of package
                from utils.database import get_connection_manager
            
            logger.info(f"Extracting insights: {question}")
            
            connection_manager = get_connection_manager()
            session = connection_manager.get_session()
            
            # Clean inputs for SQL
            cleaned_text = analysis_text.replace("'", "''")[:2000]
            cleaned_question = question.replace("'", "''")
            
            extract_query = f"""
            SELECT SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
                '{cleaned_text}',
                '{cleaned_question}'
            ) AS answer
            """
            
            result = session.sql(extract_query).collect()
            answer = result[0]['ANSWER'] if result else "No specific insights found"
            
            return answer
            
        except Exception as e:
            logger.error(f"Insight extraction failed: {e}")
            return f"Insight extraction failed: {e}"
    
    def assess_migration_complexity(self, bteq_content: str) -> str:
        """Assess migration complexity using LLM analysis."""
        
        if not self.available:
            return "Complexity assessment unavailable"
        
        try:
            # Use absolute imports to fix packaging issues
            try:
                from utils.database import get_connection_manager
            except ImportError:
                # Fallback for when running as part of package
                from utils.database import get_connection_manager
            
            connection_manager = get_connection_manager()
            session = connection_manager.get_session()
            
            # Clean content for SQL
            cleaned_content = bteq_content.replace("'", "''")[:2000]
            
            complexity_prompt = f"""
            Analyze this BTEQ script and rate its migration complexity to Snowflake on a scale of 1-10:
            
            {cleaned_content}
            
            Consider:
            - Number of control flow statements (.IF, .GOTO, .LABEL)
            - Complex SQL patterns (QUALIFY, window functions, recursive CTEs)
            - Teradata-specific functions
            - Data type conversions needed
            - Error handling complexity
            
            Respond with just a number (1-10) and brief explanation (1-2 sentences).
            """
            
            cleaned_prompt = complexity_prompt.replace("'", "''")
            
            complexity_query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'claude-3-5-sonnet',
                $${cleaned_prompt}$$
            ) AS complexity_assessment
            """
            
            result = session.sql(complexity_query).collect()
            complexity_assessment = result[0]['COMPLEXITY_ASSESSMENT'] if result else "Unable to assess complexity"
            
            return complexity_assessment
            
        except Exception as e:
            logger.error(f"Complexity assessment failed: {e}")
            return f"Complexity assessment failed: {e}"


def create_llm_service(llm_log_dir: str = None) -> SnowflakeLLMService:
    """Create and return a Snowflake LLM service instance."""
    return SnowflakeLLMService(llm_log_dir=llm_log_dir)
