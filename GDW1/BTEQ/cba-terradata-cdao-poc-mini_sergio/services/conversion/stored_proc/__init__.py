"""
Stored Procedure Conversion Services

Code generation services for Snowflake stored procedures:
- Deterministic rule-based generation (SnowflakeSPGeneratorDmst)
- Agentic LLM-based generation (SnowflakeSPGeneratorAgtc)
- Multi-model comparison and optimization
"""

# New clean architecture
from .generator_dmst import SnowflakeSPGeneratorDmst, GeneratedProcedure
from .generator_agtc import SnowflakeSPGeneratorAgtc, AgenticGeneratedProcedure, AgenticSPContext
from .models import SPGenerationResponse, SPConversionError, SPGenerationResponseEnvelope

# Legacy imports (for backwards compatibility)
try:
    from .generator import SnowflakeSPGenerator
    from .service import GeneratorService, GenerationResult  
    from .llm_enhanced import LLMEnhancedGenerator, EnhancedGeneratedProcedure
except ImportError:
    pass  # Some legacy components might not exist during refactoring

__all__ = [
    # New clean architecture
    'SnowflakeSPGeneratorDmst', 'SnowflakeSPGeneratorAgtc',
    'GeneratedProcedure', 'AgenticGeneratedProcedure', 'AgenticSPContext',
    'SPGenerationResponse', 'SPConversionError', 'SPGenerationResponseEnvelope',
    
    # Legacy (when available)
    'SnowflakeSPGenerator', 'GeneratorService', 'GenerationResult',
    'LLMEnhancedGenerator', 'EnhancedGeneratedProcedure'
]
