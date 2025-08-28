"""
Configuration Management Utilities

Centralized configuration and model management for BTEQ DCF pipeline.
"""

from .config_manager import ConfigManager
from .model_manager import ModelManager, TaskType, ModelSelectionStrategy, get_model_manager

# Create singleton instances
_config_manager = None
_model_manager = None

def get_config_manager() -> ConfigManager:
    """Get the global configuration manager instance."""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager()
    return _config_manager

__all__ = [
    'ConfigManager',
    'get_config_manager',
    'ModelManager', 
    'get_model_manager',
    'TaskType',
    'ModelSelectionStrategy'
]
