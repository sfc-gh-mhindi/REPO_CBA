#!/usr/bin/env python3
"""
Direct LangChain wrapper for Snowflake Cortex Python API

Enhanced LangChain integration using the direct snowflake.cortex API
instead of SQL queries for better performance and cleaner code.
"""

import os
import logging
from typing import Any, Dict, List, Optional, Iterator

from langchain_core.language_models.llms import LLM
from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.outputs import GenerationChunk, LLMResult, Generation
from langchain_core.embeddings import Embeddings

logger = logging.getLogger(__name__)


class SnowflakeCortexDirectLLM(LLM):
    """LangChain LLM wrapper using direct Snowflake Cortex Python API."""

    model: str = "claude-3-5-sonnet"
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    top_p: Optional[float] = None
    session: Optional[Any] = None
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._cortex_complete = None
        self._setup_cortex()
    
    def _setup_cortex(self):
        """Setup Cortex API connection."""
        try:
            from snowflake.cortex import Complete
            self._cortex_complete = Complete
            
            # Get session if not provided
            if not self.session:
                # Use absolute imports to fix packaging issues
                try:
                    from utils.database.snowflake_connection import get_snowpark_session
                except ImportError:
                    # Fallback for when running as part of package
                    from utils.database.snowflake_connection import get_snowpark_session
                connection_manager = get_snowpark_session()
                self.session = connection_manager.get_session()
            
            logger.info(f"Cortex Direct LLM initialized with model: {self.model}")
            
        except ImportError as e:
            logger.error(f"Snowflake Cortex not available: {e}")
            logger.info("Install with: pip install snowflake-ml-python")
            self._cortex_complete = None
        except Exception as e:
            logger.error(f"Failed to setup Cortex: {e}")
            self._cortex_complete = None

    @property
    def _llm_type(self) -> str:
        return "snowflake_cortex_direct"

    @property
    def _identifying_params(self) -> Dict[str, Any]:
        return {
            "model": self.model,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "top_p": self.top_p,
        }

    def _call(
        self,
        prompt: str,
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> str:
        """Call the Cortex Complete API directly."""
        if not self._cortex_complete or not self.session:
            raise RuntimeError("Snowflake Cortex not available")
        
        try:
            # Prepare options
            options = {}
            if self.temperature is not None:
                options["temperature"] = self.temperature
            if self.max_tokens is not None:
                options["max_tokens"] = self.max_tokens
            if self.top_p is not None:
                options["top_p"] = self.top_p
            
            # Add any additional kwargs
            options.update(kwargs)
            
            # Call Cortex Complete
            if options:
                try:
                    from snowflake.cortex import CompleteOptions
                    complete_options = CompleteOptions(**options)
                    response = self._cortex_complete(
                        self.model, 
                        prompt, 
                        complete_options, 
                        session=self.session
                    )
                except (ImportError, TypeError):
                    # Fallback if CompleteOptions not available or not supported
                    response = self._cortex_complete(self.model, prompt, session=self.session)
            else:
                response = self._cortex_complete(self.model, prompt, session=self.session)
            
            # Handle stop sequences
            if stop and response:
                for stop_seq in stop:
                    if stop_seq in response:
                        response = response.split(stop_seq)[0]
                        break
            
            return response or ""
            
        except Exception as e:
            logger.error(f"Cortex Complete call failed: {e}")
            raise

    def _generate(
        self,
        prompts: List[str],
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> LLMResult:
        """Generate responses for multiple prompts."""
        generations = []
        
        for prompt in prompts:
            try:
                response = self._call(prompt, stop, run_manager, **kwargs)
                generations.append([Generation(text=response)])
            except Exception as e:
                logger.error(f"Generation failed for prompt: {e}")
                generations.append([Generation(text="", generation_info={"error": str(e)})])
        
        return LLMResult(generations=generations)

    def _stream(
        self,
        prompt: str,
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> Iterator[GenerationChunk]:
        """Stream response (fallback to non-streaming for now)."""
        # Cortex Python API doesn't support streaming yet
        # Fall back to getting full response and yielding as one chunk
        try:
            response = self._call(prompt, stop, run_manager, **kwargs)
            yield GenerationChunk(text=response)
        except Exception as e:
            logger.error(f"Streaming failed: {e}")
            yield GenerationChunk(text="", generation_info={"error": str(e)})


class SnowflakeCortexDirectEmbeddings(Embeddings):
    """LangChain Embeddings wrapper using direct Snowflake Cortex API."""

    model: str = "e5-base-v2"
    session: Optional[Any] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._cortex_embed = None
        self._setup_cortex()
    
    def _setup_cortex(self):
        """Setup Cortex embeddings."""
        try:
            # Import the appropriate embedding function
            if "1024" in self.model:
                from snowflake.cortex import embed_text_1024
                self._cortex_embed = embed_text_1024
            else:
                from snowflake.cortex import embed_text_768
                self._cortex_embed = embed_text_768
            
            # Get session if not provided
            if not self.session:
                # Use absolute imports to fix packaging issues
                try:
                    from utils.database.snowflake_connection import get_snowpark_session
                except ImportError:
                    # Fallback for when running as part of package
                    from utils.database.snowflake_connection import get_snowpark_session
                connection_manager = get_snowpark_session()
                self.session = connection_manager.get_session()
            
            logger.info(f"Cortex Direct Embeddings initialized with model: {self.model}")
            
        except ImportError as e:
            logger.error(f"Snowflake Cortex embeddings not available: {e}")
            self._cortex_embed = None
        except Exception as e:
            logger.error(f"Failed to setup Cortex embeddings: {e}")
            self._cortex_embed = None

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """Embed a list of documents."""
        if not self._cortex_embed or not self.session:
            raise RuntimeError("Snowflake Cortex embeddings not available")
        
        embeddings = []
        for text in texts:
            try:
                embedding = self._cortex_embed(text, session=self.session)
                if isinstance(embedding, list):
                    embeddings.append(embedding)
                else:
                    # Handle different response formats
                    embeddings.append([])
            except Exception as e:
                logger.error(f"Embedding failed for text: {e}")
                embeddings.append([])
        
        return embeddings

    def embed_query(self, text: str) -> List[float]:
        """Embed a single query."""
        if not self._cortex_embed or not self.session:
            raise RuntimeError("Snowflake Cortex embeddings not available")
        
        try:
            embedding = self._cortex_embed(text, session=self.session)
            if isinstance(embedding, list):
                return embedding
            else:
                return []
        except Exception as e:
            logger.error(f"Query embedding failed: {e}")
            return []


class CortexMultiModelLLM(LLM):
    """Multi-model LLM that can switch between different Cortex models."""
    
    models: List[str] = ["claude-3-5-sonnet", "llama3.1-8b"]
    current_model: str = "claude-3-5-sonnet"
    session: Optional[Any] = None
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.current_model = self.models[0] if self.models else "claude-3-5-sonnet"
        self._setup_cortex()
    
    def _setup_cortex(self):
        """Setup Cortex connection."""
        try:
            from snowflake.cortex import Complete
            self._cortex_complete = Complete
            
            if not self.session:
                # Use absolute imports to fix packaging issues
                try:
                    from utils.database.snowflake_connection import get_snowpark_session
                except ImportError:
                    # Fallback for when running as part of package
                    from utils.database.snowflake_connection import get_snowpark_session
                connection_manager = get_snowpark_session()
                self.session = connection_manager.get_session()
            
            logger.info(f"Multi-model LLM initialized with models: {self.models}")
            
        except Exception as e:
            logger.error(f"Failed to setup multi-model LLM: {e}")
            self._cortex_complete = None

    @property
    def _llm_type(self) -> str:
        return "snowflake_cortex_multi_model"

    def switch_model(self, model: str):
        """Switch to a different model."""
        if model in self.models:
            self.current_model = model
            logger.info(f"Switched to model: {model}")
        else:
            logger.warning(f"Model {model} not in available models: {self.models}")

    def _call(
        self,
        prompt: str,
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> str:
        """Call the current model."""
        if not self._cortex_complete or not self.session:
            raise RuntimeError("Snowflake Cortex not available")
        
        try:
            response = self._cortex_complete(
                self.current_model, 
                prompt, 
                session=self.session
            )
            
            # Handle stop sequences
            if stop and response:
                for stop_seq in stop:
                    if stop_seq in response:
                        response = response.split(stop_seq)[0]
                        break
            
            return response or ""
            
        except Exception as e:
            logger.error(f"Multi-model call failed with {self.current_model}: {e}")
            raise

    def generate_with_all_models(self, prompt: str) -> Dict[str, str]:
        """Generate responses using all available models."""
        results = {}
        original_model = self.current_model
        
        for model in self.models:
            try:
                self.switch_model(model)
                response = self._call(prompt)
                results[model] = response
            except Exception as e:
                logger.error(f"Generation failed for {model}: {e}")
                results[model] = f"Error: {str(e)}"
        
        # Restore original model
        self.switch_model(original_model)
        return results


# Factory functions for easy usage
def create_cortex_llm(model: str = "claude-3-5-sonnet", **kwargs) -> SnowflakeCortexDirectLLM:
    """Create a direct Cortex LLM instance."""
    return SnowflakeCortexDirectLLM(model=model, **kwargs)

def create_cortex_embeddings(model: str = "e5-base-v2", **kwargs) -> SnowflakeCortexDirectEmbeddings:
    """Create a direct Cortex embeddings instance."""
    return SnowflakeCortexDirectEmbeddings(model=model, **kwargs)

def create_multi_model_llm(models: List[str] = None, **kwargs) -> CortexMultiModelLLM:
    """Create a multi-model LLM instance."""
    if models is None:
        models = ["claude-3-5-sonnet", "llama3.1-8b"]
    return CortexMultiModelLLM(models=models, **kwargs)


# Example usage and testing
if __name__ == "__main__":
    # Test basic functionality
    try:
        print("🧪 Testing Direct Cortex LLM Integration")
        
        # Test single model
        llm = create_cortex_llm("claude-3-5-sonnet")
        response = llm.invoke("Say hello in one word")
        print(f"Single model response: {response}")
        
        # Test multi-model
        multi_llm = create_multi_model_llm()
        multi_responses = multi_llm.generate_with_all_models("Explain what BTEQ is in one sentence")
        print(f"Multi-model responses: {multi_responses}")
        
        # Test embeddings
        embeddings = create_cortex_embeddings()
        embedding_vector = embeddings.embed_query("BTEQ migration to Snowflake")
        print(f"Embedding vector length: {len(embedding_vector)}")
        
        print("✅ Direct Cortex integration test successful!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
