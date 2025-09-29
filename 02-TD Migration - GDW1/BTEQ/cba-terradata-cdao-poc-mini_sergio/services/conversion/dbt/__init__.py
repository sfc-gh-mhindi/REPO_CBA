"""
DBT Model Conversion Services

AI-powered DBT model generation and conversion:
- Agentic BTEQ to DBT model transformation (SnowflakeDBTGeneratorAgtc)
- Response data models and structures
- LLM-enhanced generation with best practices
"""

# New clean architecture
from .generator_agtc import SnowflakeDBTGeneratorAgtc, DBTConversionContext, DBTConversionResult, DBTModelResult
from .models import DBTModelResponse, DBTModelResponseEnvelope

# Legacy compatibility
try:
    from .converter import DBTConverter  # Original - kept for backwards compatibility
except ImportError:
    pass

__all__ = [
    # New clean architecture
    'SnowflakeDBTGeneratorAgtc', 'DBTConversionContext', 'DBTConversionResult', 'DBTModelResult',
    'DBTModelResponse', 'DBTModelResponseEnvelope',
    
    # Legacy
    'DBTConverter'
]
