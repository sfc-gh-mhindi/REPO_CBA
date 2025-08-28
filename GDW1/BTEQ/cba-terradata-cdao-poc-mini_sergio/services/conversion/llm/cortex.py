"""
Direct Snowflake Cortex API Integration for BTEQ Migration

Uses the snowflake.cortex Python API directly instead of SQL format
for better performance and cleaner code.
"""

import logging
from typing import Optional, Dict, Any, List
import time

logger = logging.getLogger(__name__)


class DirectCortexLLMService:
    """Service using direct Snowflake Cortex Python API."""
    
    def __init__(self, session=None, default_model: str = "claude-4-sonnet"):
        """
        Initialize direct Cortex service.
        
        Args:
            session: Snowpark session (optional, will create if needed)
            default_model: Default LLM model to use
        """
        self.session = session
        self.default_model = default_model
        self.available = self._check_cortex_availability()
        
        # Initialize Cortex functions
        if self.available:
            self._init_cortex_functions()
    
    def _check_cortex_availability(self) -> bool:
        """Check if Snowflake Cortex Python API is available."""
        try:
            # Try importing Cortex functions
            from snowflake.cortex import Complete, ExtractAnswer, Sentiment, Summarize, Translate
            
            # Get or create session
            if not self.session:
                # Use absolute imports to fix packaging issues
                from utils.database import get_connection_manager
                connection_manager = get_connection_manager()
                self.session = connection_manager.get_session()
            
            # Test basic connectivity
            if self.session:
                logger.info("Snowflake Cortex Python API available")
                return True
            else:
                logger.warning("No Snowflake session available")
                return False
                
        except ImportError as e:
            logger.warning(f"Snowflake Cortex Python API not available: {e}")
            logger.info("Install with: pip install snowflake-ml-python")
            return False
        except Exception as e:
            logger.warning(f"Error checking Cortex availability: {e}")
            return False
    
    def _init_cortex_functions(self):
        """Initialize Cortex function references."""
        try:
            from snowflake.cortex import Complete, ExtractAnswer, Sentiment, Summarize, Translate
            
            self.complete = Complete
            self.extract_answer = ExtractAnswer
            self.sentiment = Sentiment
            self.summarize = Summarize
            self.translate = Translate
            
            logger.info("Cortex functions initialized successfully")
            
        except ImportError as e:
            logger.error(f"Failed to initialize Cortex functions: {e}")
            self.available = False
    
    def enhance_procedure(self, 
                         prompt: str, 
                         model: Optional[str] = None,
                         temperature: Optional[float] = None,
                         max_tokens: Optional[int] = None) -> Optional[str]:
        """
        Enhance a stored procedure using direct Cortex Complete API.
        
        Args:
            prompt: The enhancement prompt
            model: LLM model to use (defaults to self.default_model)
            temperature: Temperature for generation
            max_tokens: Maximum tokens to generate
            
        Returns:
            Enhanced SQL or None if failed
        """
        if not self.available:
            logger.warning("Cortex not available, cannot enhance procedure")
            return None
        
        model = model or self.default_model
        
        try:
            logger.info(f"Enhancing procedure with Cortex Complete API (model: {model})")
            start_time = time.time()
            
            # Prepare options if specified
            options = {}
            if temperature is not None:
                options["temperature"] = temperature
            if max_tokens is not None:
                options["max_tokens"] = max_tokens
            
            # Call Cortex Complete directly
            if options:
                # If we have options, we might need to use CompleteOptions
                try:
                    from snowflake.cortex import CompleteOptions
                    complete_options = CompleteOptions(**options)
                    response = self.complete(model, prompt, complete_options, session=self.session)
                except ImportError:
                    # Fallback to basic complete if CompleteOptions not available
                    logger.warning("CompleteOptions not available, using basic complete")
                    response = self.complete(model, prompt, session=self.session)
            else:
                response = self.complete(model, prompt, session=self.session)
            
            execution_time = (time.time() - start_time) * 1000
            
            if response and response.strip():
                logger.info(f"Cortex Complete successful ({execution_time:.0f}ms)")
                return response.strip()
            else:
                logger.warning("Empty response from Cortex Complete")
                return None
                
        except Exception as e:
            logger.error(f"Cortex Complete failed: {e}")
            return None
    
    def summarize_analysis(self, analysis_text: str) -> str:
        """Summarize BTEQ analysis using Cortex Summarize."""
        if not self.available:
            return "Analysis summary unavailable (Cortex not available)"
        
        try:
            logger.info("Summarizing analysis with Cortex Summarize")
            
            # Truncate if too long
            truncated_text = analysis_text[:2000] if len(analysis_text) > 2000 else analysis_text
            
            summary = self.summarize(truncated_text, session=self.session)
            
            if summary:
                logger.info("Analysis summarization successful")
                return summary
            else:
                return "Unable to generate summary"
                
        except Exception as e:
            logger.error(f"Analysis summarization failed: {e}")
            return f"Summary generation failed: {e}"
    
    def extract_key_insights(self, analysis_text: str, question: str) -> str:
        """Extract specific insights using Cortex ExtractAnswer."""
        if not self.available:
            return "Insights unavailable (Cortex not available)"
        
        try:
            logger.info(f"Extracting insights with question: {question}")
            
            # Truncate if too long
            truncated_text = analysis_text[:2000] if len(analysis_text) > 2000 else analysis_text
            
            answer = self.extract_answer(truncated_text, question, session=self.session)
            
            if answer:
                logger.info("Insight extraction successful")
                return answer
            else:
                return "No specific insights found"
                
        except Exception as e:
            logger.error(f"Insight extraction failed: {e}")
            return f"Insight extraction failed: {e}"
    
    def assess_migration_complexity(self, bteq_content: str) -> str:
        """Assess migration complexity using Cortex Complete."""
        if not self.available:
            return "Complexity assessment unavailable"
        
        try:
            # Truncate content for analysis
            truncated_content = bteq_content[:2000] if len(bteq_content) > 2000 else bteq_content
            
            complexity_prompt = f"""
Analyze this BTEQ script and rate its migration complexity to Snowflake on a scale of 1-10:

{truncated_content}

Consider:
- Number of control flow statements (.IF, .GOTO, .LABEL)
- Complex SQL patterns (QUALIFY, window functions, recursive CTEs)
- Teradata-specific functions
- Data type conversions needed
- Error handling complexity

Respond with just a number (1-10) and brief explanation (1-2 sentences).
"""
            
            complexity_assessment = self.complete(
                self.default_model, 
                complexity_prompt, 
                session=self.session
            )
            
            if complexity_assessment:
                return complexity_assessment
            else:
                return "Unable to assess complexity"
                
        except Exception as e:
            logger.error(f"Complexity assessment failed: {e}")
            return f"Complexity assessment failed: {e}"
    
    def analyze_sentiment(self, text: str) -> str:
        """Analyze sentiment of text (useful for error messages, comments)."""
        if not self.available:
            return "Sentiment analysis unavailable"
        
        try:
            sentiment = self.sentiment(text, session=self.session)
            return sentiment if sentiment else "Unknown sentiment"
            
        except Exception as e:
            logger.error(f"Sentiment analysis failed: {e}")
            return f"Sentiment analysis failed: {e}"
    
    def translate_comments(self, text: str, target_language: str = "en") -> str:
        """Translate comments or documentation."""
        if not self.available:
            return text  # Return original if translation not available
        
        try:
            # Detect source language (assume it's not English if we're translating to English)
            source_lang = "auto"  # Snowflake Cortex might support auto-detection
            
            translated = self.translate(text, source_lang, target_language, session=self.session)
            return translated if translated else text
            
        except Exception as e:
            logger.error(f"Translation failed: {e}")
            return text  # Return original on failure
    
    def multi_model_generate(self, 
                           prompt: str, 
                           models: List[str] = None) -> Dict[str, str]:
        """Generate responses using multiple models for comparison."""
        if not self.available:
            return {}
        
        models = models or ["claude-4-sonnet", "snowflake-llama-3.3-70b"]
        results = {}
        
        for model in models:
            try:
                logger.info(f"Generating with model: {model}")
                response = self.complete(model, prompt, session=self.session)
                
                if response:
                    results[model] = response.strip()
                else:
                    results[model] = f"No response from {model}"
                    
            except Exception as e:
                logger.error(f"Generation failed for {model}: {e}")
                results[model] = f"Error: {str(e)}"
        
        return results
    
    def get_available_models(self) -> List[str]:
        """Get list of available models (hardcoded for now)."""
        # These are commonly available models in Snowflake Cortex
        return [
            "claude-4-sonnet",
            "snowflake-llama-3.3-70b",
            "claude-3-5-sonnet",
            "claude-3-haiku", 
            "claude-3-sonnet",
            "llama3.1-8b",
            "llama3.1-70b",
            "mixtral-8x7b",
            "gemma-7b",
            "snowflake-arctic"
        ]


class CortexAgentLLM:
    """LLM wrapper for agent use with direct Cortex API."""
    
    def __init__(self, 
                 model: str = "claude-4-sonnet",
                 session=None,
                 temperature: Optional[float] = None,
                 max_tokens: Optional[int] = None):
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.cortex_service = DirectCortexLLMService(session, model)
    
    def invoke(self, prompt: str) -> str:
        """Invoke the LLM with a prompt."""
        response = self.cortex_service.enhance_procedure(
            prompt, 
            model=self.model,
            temperature=self.temperature,
            max_tokens=self.max_tokens
        )
        return response or ""
    
    def __call__(self, prompt: str) -> str:
        """Allow calling the LLM directly."""
        return self.invoke(prompt)
    
    @property
    def _llm_type(self) -> str:
        return "cortex_direct"


# Factory functions
def create_direct_cortex_service(session=None, model: str = "claude-4-sonnet") -> DirectCortexLLMService:
    """Create a direct Cortex service instance."""
    return DirectCortexLLMService(session, model)

def create_cortex_agent_llm(model: str = "claude-4-sonnet", 
                           session=None,
                           **kwargs) -> CortexAgentLLM:
    """Create a Cortex LLM for agent use."""
    return CortexAgentLLM(model, session, **kwargs)
