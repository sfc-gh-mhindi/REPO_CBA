"""
Snowflake Connection Management for BTEQ Migration Pipeline

Handles Snowflake connections using service account JWT authentication.
"""

import logging
import os
from pathlib import Path
from typing import Optional
import toml

logger = logging.getLogger(__name__)


class SnowflakeConnectionManager:
    """Manages Snowflake connections for the BTEQ migration pipeline."""
    
    def __init__(self, connection_name: str = "pupad_svc"):
        """
        Initialize connection manager.
        
        Args:
            connection_name: Name of connection in config file
        """
        self.connection_name = connection_name
        self.session = None
        self.connection_config = None
        
    def load_connection_config(self) -> dict:
        """Load connection configuration from TOML file or direct config."""
        
        # Direct configuration as provided (using RSA format key)
        config = {
            "account": "SFPSCOGS-DEMO_PUPAD",
            "user": "svc_dcf", 
            "authenticator": "SNOWFLAKE_JWT",
            "private_key_path": "/Users/psundaram/.snowflake/keys/dcf_service_account_key.pem",
            "warehouse": "wh_psdm_xs",
            "database": "psundaram",
            "schema": "gdw1_mig",
            "network_timeout": 1000,
            "socket_timeout": 1000
        }
        
        self.connection_config = config
        logger.info(f"Loaded connection config for {self.connection_name}")
        return config
        
    def create_snowpark_session(self):
        """Create a Snowpark session using the connection config."""
        
        try:
            from snowflake.snowpark import Session
            import snowflake.connector
            
            if not self.connection_config:
                self.load_connection_config()
            
            # Read and process private key
            private_key_path = Path(self.connection_config["private_key_path"]).expanduser()
            
            if not private_key_path.exists():
                raise FileNotFoundError(f"Private key not found: {private_key_path}")
            
            # Read and parse the private key properly
            try:
                from cryptography.hazmat.primitives import serialization
                from cryptography.hazmat.primitives.serialization import load_pem_private_key
                
                with open(private_key_path, 'rb') as key_file:
                    private_key_data = key_file.read()
                
                # Parse the private key
                private_key_obj = load_pem_private_key(private_key_data, password=None)
                
                # Serialize to DER format for Snowflake
                private_key = private_key_obj.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
                
            except ImportError:
                # Fallback: read as text
                logger.warning("cryptography library not available, using text format")
                with open(private_key_path, 'r') as key_file:
                    private_key = key_file.read().encode('utf-8')
            
            # Create connection parameters
            connection_params = {
                "account": self.connection_config["account"],
                "user": self.connection_config["user"],
                "authenticator": self.connection_config["authenticator"],
                "private_key": private_key,
                "warehouse": self.connection_config["warehouse"],
                "database": self.connection_config["database"],
                "schema": self.connection_config["schema"],
                "network_timeout": self.connection_config.get("network_timeout", 1000),
                "socket_timeout": self.connection_config.get("socket_timeout", 1000)
            }
            
            # Create Snowpark session
            self.session = Session.builder.configs(connection_params).create()
            
            logger.info(f"Successfully created Snowpark session for {self.connection_config['account']}")
            logger.info(f"Connected to warehouse: {self.connection_config['warehouse']}")
            logger.info(f"Using database: {self.connection_config['database']}.{self.connection_config['schema']}")
            
            return self.session
            
        except ImportError as e:
            logger.error(f"Snowflake libraries not available: {e}")
            raise ImportError("Snowflake connector and Snowpark libraries required")
        except Exception as e:
            logger.error(f"Failed to create Snowpark session: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test the Snowflake connection."""
        
        try:
            if not self.session:
                self.create_snowpark_session()
            
            # Test basic query
            result = self.session.sql("SELECT CURRENT_VERSION() AS VERSION").collect()
            version = result[0]['VERSION']
            
            logger.info(f"Connection successful! Snowflake version: {version}")
            return True
            
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def test_cortex_availability(self) -> bool:
        """Test if Snowflake Cortex functions are available."""
        
        try:
            if not self.session:
                self.create_snowpark_session()
            
            # Test Cortex Complete function
            test_query = """
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'llama3.1-8b',
                'Say hello in one word'
            ) AS test_response
            """
            
            result = self.session.sql(test_query).collect()
            response = result[0]['TEST_RESPONSE']
            
            logger.info(f"Cortex test successful! Response: {response}")
            return True
            
        except Exception as e:
            logger.error(f"Cortex test failed: {e}")
            return False
    
    def get_session(self):
        """Get the current Snowpark session, creating one if needed."""
        
        if not self.session:
            self.create_snowpark_session()
        
        return self.session
    
    def close(self):
        """Close the Snowpark session."""
        
        if self.session:
            self.session.close()
            self.session = None
            logger.info("Snowpark session closed")


# Global connection manager instance
_connection_manager = None

def get_connection_manager() -> SnowflakeConnectionManager:
    """Get the global connection manager instance."""
    global _connection_manager
    if _connection_manager is None:
        _connection_manager = SnowflakeConnectionManager()
    return _connection_manager

def get_snowpark_session():
    """Get a Snowpark session using the global connection manager."""
    return get_connection_manager().get_session()
