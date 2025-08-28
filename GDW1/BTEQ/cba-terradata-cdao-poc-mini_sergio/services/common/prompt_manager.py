"""
Prompt Management Service

Handles loading and rendering of external prompt templates.
Separates prompt engineering from business logic.
"""

import yaml
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, Template
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class PromptManager:
    """Manages external prompt templates and configuration."""
    
    def __init__(self, prompts_dir: str = "config/prompts"):
        """Initialize prompt manager with prompts directory."""
        self.prompts_dir = Path(prompts_dir)
        self.jinja_env = Environment(
            loader=FileSystemLoader(self.prompts_dir),
            trim_blocks=True,
            lstrip_blocks=True
        )
        self._config_cache = {}
        
    def load_prompt_config(self, config_path: str) -> Dict[str, Any]:
        """Load prompt configuration from YAML file."""
        config_file = self.prompts_dir / config_path
        
        if config_path in self._config_cache:
            return self._config_cache[config_path]
            
        if not config_file.exists():
            raise FileNotFoundError(f"Prompt config not found: {config_file}")
            
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            
        self._config_cache[config_path] = config
        logger.info(f"Loaded prompt config: {config.get('name', config_path)}")
        return config
        
    def render_template(self, template_path: str, context: Dict[str, Any]) -> str:
        """Render Jinja2 template with given context."""
        try:
            template = self.jinja_env.get_template(template_path)
            rendered = template.render(**context)
            logger.debug(f"Rendered template: {template_path}")
            return rendered
        except Exception as e:
            logger.error(f"Failed to render template {template_path}: {e}")
            raise
            
    def build_dbt_conversion_prompt(self, 
                                  original_bteq_sql: str,
                                  chosen_stored_procedure: str,
                                  additional_analysis: str = "") -> str:
        """Build complete DBT conversion prompt from templates."""
        
        # Load configuration
        config = self.load_prompt_config("dbt/snowflake_conversion.yaml")
        
        # Prepare context
        context = {
            **config,  # Include all config values
            "original_bteq_sql": original_bteq_sql,
            "chosen_stored_procedure": chosen_stored_procedure,
            "additional_analysis": additional_analysis or "Standard BTEQ to DBT model transformation"
        }
        
        # Render template
        prompt = self.render_template("dbt/conversion_template.jinja2", context)
        return prompt
        
    def build_sp_generation_prompt(self, 
                                 substituted_bteq: str,
                                 procedure_name: str,
                                 reference_deterministic_sp: str = "",
                                 analysis_notes: str = "",
                                 complexity_hints: str = "") -> str:
        """Build complete SP generation prompt from templates."""
        
        # Load configuration
        config = self.load_prompt_config("stored_proc/snowflake_conversion.yaml")
        
        # Check token optimization settings
        token_config = config.get("token_optimization", {})
        include_deterministic_ref = token_config.get("include_deterministic_reference", True)
        
        # Conditionally exclude deterministic SP reference to save tokens
        if not include_deterministic_ref:
            logger.info("🪙 Token optimization: Excluding deterministic SP reference from prompt")
            reference_deterministic_sp = ""
        
        # Optional: Limit BTEQ content size
        max_bteq_lines = token_config.get("max_bteq_preview_lines")
        if max_bteq_lines and substituted_bteq:
            bteq_lines = substituted_bteq.split('\n')
            if len(bteq_lines) > max_bteq_lines:
                substituted_bteq = '\n'.join(bteq_lines[:max_bteq_lines]) + f"\n\n-- [Truncated: {len(bteq_lines) - max_bteq_lines} more lines omitted for token optimization]"
                logger.info(f"🪙 Token optimization: Limited BTEQ content to {max_bteq_lines} lines")
        
        # Prepare context
        context = {
            **config,  # Include all config values
            "substituted_bteq": substituted_bteq,
            "procedure_name": procedure_name,
            "reference_deterministic_sp": reference_deterministic_sp,
            "analysis_notes": analysis_notes or "",
            "complexity_hints": complexity_hints or ""
        }
        
        # Render template
        prompt = self.render_template("stored_proc/sp_generation_template.jinja2", context)
        return prompt
        
    def get_prompt_metadata(self, config_path: str) -> Dict[str, Any]:
        """Get metadata about a prompt configuration."""
        config = self.load_prompt_config(config_path)
        return {
            "name": config.get("name", "Unknown"),
            "version": config.get("version", "1.0"),
            "description": config.get("description", ""),
            "last_modified": self.prompts_dir / config_path
        }


# Global prompt manager instance
_prompt_manager: Optional[PromptManager] = None

def get_prompt_manager() -> PromptManager:
    """Get global prompt manager instance."""
    global _prompt_manager
    if _prompt_manager is None:
        _prompt_manager = PromptManager()
    return _prompt_manager
