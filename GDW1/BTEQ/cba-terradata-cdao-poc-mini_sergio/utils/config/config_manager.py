"""
Configuration Manager for BTEQ DCF
Handles loading and validation of application properties.
"""

import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional, Union
import configparser
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SnowflakeConfig:
    """Snowflake connection configuration."""
    account: str
    user: str
    authenticator: str
    private_key_path: str
    warehouse: str
    database: str
    schema: str
    network_timeout: int = 1000
    socket_timeout: int = 1000


@dataclass
class LLMConfig:
    """LLM configuration."""
    provider: str = "snowflake_cortex"
    model: str = "claude-3-5-sonnet"
    fallback_model: str = "llama3.1-8b"
    temperature: float = 0.1
    max_tokens: int = 4000
    timeout_seconds: int = 60
    enable_fallback: bool = True


@dataclass
class LLMLoggingConfig:
    """LLM logging configuration."""
    enabled: bool = True
    log_requests: bool = True
    log_responses: bool = True
    log_level: str = "INFO"
    output_directory: str = "logs/llm_interactions"
    max_file_size_mb: int = 100
    max_files: int = 10


@dataclass
class ProcessingConfig:
    """Processing configuration."""
    batch_size: int = 50
    max_concurrent: int = 5
    timeout_seconds: int = 300
    enable_advanced_analysis: bool = True
    enable_llm_enhancement: bool = True


@dataclass
class ApplicationConfig:
    """Complete application configuration."""
    snowflake: SnowflakeConfig
    llm: LLMConfig
    llm_logging: LLMLoggingConfig
    processing: ProcessingConfig
    
    # Additional configurations as flat attributes
    input_bteq_directory: str = "current_state/bteq_bteq"
    input_variable_config: str = "config/database/variable_substitution.cfg"
    output_base_directory: str = "output"
    logging_level: str = "INFO"
    debug_mode: bool = False


class ConfigManager:
    """Manages application configuration from properties files."""
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_file: Path to properties file. Defaults to application.properties
        """
        self.config_file = config_file or self._find_default_config()
        self.config = configparser.ConfigParser()
        self._config_data: Dict[str, Any] = {}
        self._app_config: Optional[ApplicationConfig] = None
        
    def _find_default_config(self) -> str:
        """Find the default configuration file."""
        # Look for config file in several locations
        search_paths = [
            "config/application.properties",
            "application.properties",
            os.path.join(Path(__file__).parent, "application.properties"),
            os.path.join(Path(__file__).parent.parent, "config", "application.properties")
        ]
        
        for path in search_paths:
            if os.path.exists(path):
                logger.info(f"Found configuration file: {path}")
                return path
        
        # If no config file found, use the default path
        default_path = os.path.join(Path(__file__).parent, "application.properties")
        logger.warning(f"No configuration file found, using default: {default_path}")
        return default_path
    
    def load_config(self) -> ApplicationConfig:
        """Load configuration from properties file."""
        if self._app_config:
            return self._app_config
            
        try:
            # Load properties file
            if os.path.exists(self.config_file):
                self.config.read(self.config_file)
                logger.info(f"Loaded configuration from: {self.config_file}")
            else:
                logger.warning(f"Configuration file not found: {self.config_file}")
                logger.info("Using default configuration values")
            
            # Convert to flat dictionary for easier access
            self._config_data = {}
            for section in self.config.sections():
                for key, value in self.config.items(section):
                    self._config_data[f"{section}.{key}"] = value
            
            # Also load from DEFAULT section (no section prefix)
            if self.config.has_section('DEFAULT') or len(self.config.defaults()) > 0:
                for key, value in self.config.defaults().items():
                    if key not in self._config_data:
                        self._config_data[key] = value
            
            # Parse configuration into structured objects
            self._app_config = self._parse_application_config()
            return self._app_config
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            # Return default configuration
            return self._get_default_config()
    
    def _parse_application_config(self) -> ApplicationConfig:
        """Parse loaded properties into ApplicationConfig."""
        
        # Parse Snowflake configuration
        snowflake_config = SnowflakeConfig(
            account=self._get_value("snowflake.account", "SFPSCOGS-DEMO_PUPAD"),
            user=self._get_value("snowflake.user", "svc_dcf"),
            authenticator=self._get_value("snowflake.authenticator", "SNOWFLAKE_JWT"),
            private_key_path=self._get_value("snowflake.private_key_path", 
                                           "/Users/psundaram/.snowflake/keys/dcf_service_account_key.pem"),
            warehouse=self._get_value("snowflake.warehouse", "wh_psdm_xs"),
            database=self._get_value("snowflake.database", "psundaram"),
            schema=self._get_value("snowflake.schema", "gdw1_mig"),
            network_timeout=self._get_int_value("snowflake.network_timeout", 1000),
            socket_timeout=self._get_int_value("snowflake.socket_timeout", 1000)
        )
        
        # Parse LLM configuration
        llm_config = LLMConfig(
            provider=self._get_value("llm.provider", "snowflake_cortex"),
            model=self._get_value("llm.model", "claude-3-5-sonnet"),
            fallback_model=self._get_value("llm.fallback_model", "llama3.1-8b"),
            temperature=self._get_float_value("llm.temperature", 0.1),
            max_tokens=self._get_int_value("llm.max_tokens", 4000),
            timeout_seconds=self._get_int_value("llm.timeout_seconds", 60),
            enable_fallback=self._get_bool_value("llm.enable_fallback", True)
        )
        
        # Parse LLM Logging configuration
        llm_logging_config = LLMLoggingConfig(
            enabled=self._get_bool_value("llm.logging.enabled", True),
            log_requests=self._get_bool_value("llm.logging.log_requests", True),
            log_responses=self._get_bool_value("llm.logging.log_responses", True),
            log_level=self._get_value("llm.logging.log_level", "INFO"),
            output_directory=self._get_value("llm.logging.output_directory", "logs/llm_interactions"),
            max_file_size_mb=self._get_int_value("llm.logging.max_file_size_mb", 100),
            max_files=self._get_int_value("llm.logging.max_files", 10)
        )
        
        # Parse Processing configuration
        processing_config = ProcessingConfig(
            batch_size=self._get_int_value("processing.batch_size", 50),
            max_concurrent=self._get_int_value("processing.max_concurrent", 5),
            timeout_seconds=self._get_int_value("processing.timeout_seconds", 300),
            enable_advanced_analysis=self._get_bool_value("processing.enable_advanced_analysis", True),
            enable_llm_enhancement=self._get_bool_value("processing.enable_llm_enhancement", True)
        )
        
        return ApplicationConfig(
            snowflake=snowflake_config,
            llm=llm_config,
            llm_logging=llm_logging_config,
            processing=processing_config,
            input_bteq_directory=self._get_value("input.bteq_directory", "current_state/bteq_bteq"),
            input_variable_config=self._get_value("input.variable_config", "config/database/variable_substitution.cfg"),
            output_base_directory=self._get_value("output.base_directory", "output"),
            logging_level=self._get_value("logging.level", "INFO"),
            debug_mode=self._get_bool_value("debug.enable_debug_mode", False)
        )
    
    def _get_value(self, key: str, default: str) -> str:
        """Get string value from configuration."""
        return self._config_data.get(key, default)
    
    def _get_int_value(self, key: str, default: int) -> int:
        """Get integer value from configuration."""
        value = self._config_data.get(key)
        if value is not None:
            try:
                return int(value)
            except ValueError:
                logger.warning(f"Invalid integer value for {key}: {value}, using default: {default}")
        return default
    
    def _get_float_value(self, key: str, default: float) -> float:
        """Get float value from configuration."""
        value = self._config_data.get(key)
        if value is not None:
            try:
                return float(value)
            except ValueError:
                logger.warning(f"Invalid float value for {key}: {value}, using default: {default}")
        return default
    
    def _get_bool_value(self, key: str, default: bool) -> bool:
        """Get boolean value from configuration."""
        value = self._config_data.get(key)
        if value is not None:
            return str(value).lower() in ('true', '1', 'yes', 'on', 'enabled')
        return default
    
    def _get_default_config(self) -> ApplicationConfig:
        """Get default configuration when file loading fails."""
        logger.info("Using default configuration")
        
        return ApplicationConfig(
            snowflake=SnowflakeConfig(
                account="SFPSCOGS-DEMO_PUPAD",
                user="svc_dcf",
                authenticator="SNOWFLAKE_JWT",
                private_key_path="/Users/psundaram/.snowflake/keys/dcf_service_account_key.pem",
                warehouse="wh_psdm_xs",
                database="psundaram",
                schema="gdw1_mig"
            ),
            llm=LLMConfig(),
            llm_logging=LLMLoggingConfig(),
            processing=ProcessingConfig()
        )
    
    def get_config(self) -> ApplicationConfig:
        """Get the current application configuration."""
        if not self._app_config:
            return self.load_config()
        return self._app_config
    
    def get_snowflake_connection_params(self) -> Dict[str, Any]:
        """Get Snowflake connection parameters as dictionary."""
        config = self.get_config()
        sf_config = config.snowflake
        
        # Read private key
        private_key_path = Path(sf_config.private_key_path).expanduser()
        if not private_key_path.exists():
            raise FileNotFoundError(f"Private key not found: {private_key_path}")
        
        try:
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.primitives.serialization import load_pem_private_key
            
            with open(private_key_path, 'rb') as key_file:
                private_key_data = key_file.read()
            
            private_key_obj = load_pem_private_key(private_key_data, password=None)
            private_key = private_key_obj.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
        except ImportError:
            logger.warning("cryptography library not available, using text format")
            with open(private_key_path, 'r') as key_file:
                private_key = key_file.read().encode('utf-8')
        
        return {
            "account": sf_config.account,
            "user": sf_config.user,
            "authenticator": sf_config.authenticator,
            "private_key": private_key,
            "warehouse": sf_config.warehouse,
            "database": sf_config.database,
            "schema": sf_config.schema,
            "network_timeout": sf_config.network_timeout,
            "socket_timeout": sf_config.socket_timeout
        }


# Global configuration manager
_config_manager: Optional[ConfigManager] = None

def get_config_manager(config_file: Optional[str] = None) -> ConfigManager:
    """Get the global configuration manager instance."""
    global _config_manager
    if _config_manager is None or config_file is not None:
        _config_manager = ConfigManager(config_file)
    return _config_manager

def get_application_config(config_file: Optional[str] = None) -> ApplicationConfig:
    """Get the application configuration."""
    return get_config_manager(config_file).get_config()
