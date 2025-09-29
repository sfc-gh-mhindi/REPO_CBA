"""
Model Configuration Manager

Centralized management of LLM models configuration from external parameter files.
Supports dynamic model loading, task-specific routing, and performance optimization.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class TaskType(Enum):
    """Types of tasks for model selection."""
    BTEQ_ANALYSIS = "bteq_analysis"
    CODE_GENERATION = "code_generation"
    ERROR_CORRECTION = "error_correction"
    PERFORMANCE_OPTIMIZATION = "performance_optimization"
    SUMMARIZATION = "summarization"
    JUDGMENT_EVALUATION = "judgment_evaluation"
    BATCH_PROCESSING = "batch_processing"


class ModelSelectionStrategy(Enum):
    """Strategies for selecting models."""
    QUALITY_FIRST = "quality_first"
    PERFORMANCE_FIRST = "performance_first"
    BALANCED = "balanced"
    COST_OPTIMIZED = "cost_optimized"


@dataclass
class ModelInfo:
    """Information about a specific model."""
    name: str
    description: str
    provider: str
    capabilities: List[str]
    use_cases: List[str]
    quality_score: float
    speed: str
    cost: str
    
    @classmethod
    def from_dict(cls, name: str, data: Dict[str, Any]) -> 'ModelInfo':
        """Create ModelInfo from dictionary data."""
        return cls(
            name=name,
            description=data.get("description", ""),
            provider=data.get("provider", "unknown"),
            capabilities=data.get("capabilities", []),
            use_cases=data.get("use_cases", []),
            quality_score=data.get("performance", {}).get("quality_score", 0.5),
            speed=data.get("performance", {}).get("speed", "medium"),
            cost=data.get("performance", {}).get("cost", "medium")
        )


class ModelManager:
    """Manages model configurations and selection strategies."""
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize model manager.
        
        Args:
            config_file: Path to models configuration file
        """
        self.config_file = config_file or self._find_default_config()
        self.config = {}
        self.models_info: Dict[str, ModelInfo] = {}
        self.load_config()
    
    def _find_default_config(self) -> str:
        """Find the default models configuration file."""
        search_paths = [
            "config/models.json",
            "models.json",
            os.path.join(Path(__file__).parent, "models.json"),
        ]
        
        for path in search_paths:
            if os.path.exists(path):
                logger.info(f"Found models config: {path}")
                return path
        
        # Use default path
        default_path = os.path.join(Path(__file__).parent, "models.json")
        logger.warning(f"Models config not found, using default: {default_path}")
        return default_path
    
    def load_config(self) -> bool:
        """Load models configuration from file."""
        try:
            if not os.path.exists(self.config_file):
                logger.error(f"Models config file not found: {self.config_file}")
                self._create_default_config()
                return False
            
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
            
            # Load model information
            self._load_models_info()
            
            logger.info(f"Loaded models config from: {self.config_file}")
            logger.info(f"Available models: {list(self.models_info.keys())}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load models config: {e}")
            self._create_default_config()
            return False
    
    def _load_models_info(self):
        """Load detailed model information."""
        models_data = self.config.get("models", {})
        
        for model_key, model_data in models_data.items():
            model_name = model_data.get("name")
            if model_name:
                self.models_info[model_name] = ModelInfo.from_dict(model_name, model_data)
    
    def _create_default_config(self):
        """Create default configuration."""
        self.config = {
            "models": {
                "primary": {"name": "claude-4-sonnet"},
                "secondary": {"name": "snowflake-llama-3.3-70b"}
            },
            "model_groups": {
                "default_pair": ["claude-4-sonnet", "snowflake-llama-3.3-70b"]
            },
            "usage_policies": {
                "default_model": "claude-4-sonnet",
                "fallback_model": "snowflake-llama-3.3-70b"
            }
        }
    
    def get_default_model(self) -> str:
        """Get the default model name."""
        return self.config.get("usage_policies", {}).get("default_model", "claude-4-sonnet")
    
    def get_fallback_model(self) -> str:
        """Get the fallback model name."""
        return self.config.get("usage_policies", {}).get("fallback_model", "snowflake-llama-3.3-70b")
    
    def get_model_group(self, group_name: str) -> List[str]:
        """Get a list of models for a specific group."""
        return self.config.get("model_groups", {}).get(group_name, [])
    
    def get_default_pair(self) -> List[str]:
        """Get the default pair of models for multi-model workflows."""
        return self.get_model_group("default_pair")
    
    def get_all_available_models(self) -> List[str]:
        """Get all available models."""
        return self.get_model_group("all_available")
    
    def get_models_for_task(self, task: TaskType) -> List[str]:
        """Get recommended models for a specific task."""
        task_models = self.config.get("task_specific_models", {})
        return task_models.get(task.value, self.get_default_pair())
    
    def select_model_for_complexity(self, complexity_score: float) -> str:
        """Select model based on complexity score."""
        routing = self.config.get("model_routing", {})
        threshold = routing.get("complexity_threshold", 0.7)
        
        if complexity_score >= threshold:
            high_complexity_models = routing.get("high_complexity_models", [self.get_default_model()])
            return high_complexity_models[0] if high_complexity_models else self.get_default_model()
        else:
            low_complexity_models = routing.get("low_complexity_models", [self.get_fallback_model()])
            return low_complexity_models[0] if low_complexity_models else self.get_fallback_model()
    
    def get_model_info(self, model_name: str) -> Optional[ModelInfo]:
        """Get detailed information about a model."""
        return self.models_info.get(model_name)
    
    def rank_models_for_task(self, 
                           task: TaskType, 
                           strategy: ModelSelectionStrategy = ModelSelectionStrategy.QUALITY_FIRST) -> List[str]:
        """Rank models for a task based on selection strategy."""
        task_models = self.get_models_for_task(task)
        
        if strategy == ModelSelectionStrategy.QUALITY_FIRST:
            # Sort by quality score descending
            return sorted(task_models, 
                         key=lambda m: self.models_info.get(m, ModelInfo("", "", "", [], [], 0.0, "", "")).quality_score,
                         reverse=True)
        elif strategy == ModelSelectionStrategy.PERFORMANCE_FIRST:
            # Sort by speed (fast > medium > slow)
            speed_order = {"fast": 3, "medium": 2, "slow": 1}
            return sorted(task_models,
                         key=lambda m: speed_order.get(
                             self.models_info.get(m, ModelInfo("", "", "", [], [], 0.0, "medium", "")).speed, 2),
                         reverse=True)
        elif strategy == ModelSelectionStrategy.COST_OPTIMIZED:
            # Sort by cost (low > medium > high)
            cost_order = {"low": 3, "medium": 2, "high": 1}
            return sorted(task_models,
                         key=lambda m: cost_order.get(
                             self.models_info.get(m, ModelInfo("", "", "", [], [], 0.0, "", "medium")).cost, 2),
                         reverse=True)
        else:  # BALANCED
            # Balanced scoring
            def balanced_score(model_name: str) -> float:
                info = self.models_info.get(model_name)
                if not info:
                    return 0.5
                
                quality_weight = 0.4
                speed_weight = 0.3
                cost_weight = 0.3
                
                speed_score = {"fast": 1.0, "medium": 0.6, "slow": 0.3}.get(info.speed, 0.6)
                cost_score = {"low": 1.0, "medium": 0.6, "high": 0.3}.get(info.cost, 0.6)
                
                return (info.quality_score * quality_weight + 
                       speed_score * speed_weight + 
                       cost_score * cost_weight)
            
            return sorted(task_models, key=balanced_score, reverse=True)
    
    def get_usage_policies(self) -> Dict[str, Any]:
        """Get usage policies configuration."""
        return self.config.get("usage_policies", {})
    
    def is_multi_model_enabled(self) -> bool:
        """Check if multi-model processing is enabled."""
        return self.get_usage_policies().get("multi_model_enabled", True)
    
    def get_max_models_per_request(self) -> int:
        """Get maximum number of models per request."""
        return self.get_usage_policies().get("max_models_per_request", 2)
    
    def validate_model_availability(self, models: List[str]) -> Tuple[List[str], List[str]]:
        """
        Validate model availability.
        
        Returns:
            Tuple of (available_models, unavailable_models)
        """
        all_available = self.get_all_available_models()
        available = [m for m in models if m in all_available]
        unavailable = [m for m in models if m not in all_available]
        
        return available, unavailable
    
    def get_model_statistics(self) -> Dict[str, Any]:
        """Get statistics about loaded models."""
        return {
            "total_models": len(self.models_info),
            "providers": list(set(info.provider for info in self.models_info.values())),
            "average_quality_score": sum(info.quality_score for info in self.models_info.values()) / len(self.models_info) if self.models_info else 0,
            "capabilities": list(set(cap for info in self.models_info.values() for cap in info.capabilities)),
            "default_pair": self.get_default_pair(),
            "multi_model_enabled": self.is_multi_model_enabled()
        }


# Global model manager instance
_model_manager: Optional[ModelManager] = None

def get_model_manager(config_file: Optional[str] = None) -> ModelManager:
    """Get the global model manager instance."""
    global _model_manager
    if _model_manager is None or config_file is not None:
        _model_manager = ModelManager(config_file)
    return _model_manager

def get_default_model() -> str:
    """Get the default model name."""
    return get_model_manager().get_default_model()

def get_fallback_model() -> str:
    """Get the fallback model name."""
    return get_model_manager().get_fallback_model()

def get_models_for_task(task: TaskType) -> List[str]:
    """Get models for a specific task."""
    return get_model_manager().get_models_for_task(task)

def get_default_model_pair() -> List[str]:
    """Get the default pair of models."""
    return get_model_manager().get_default_pair()

# Add missing import
import os
