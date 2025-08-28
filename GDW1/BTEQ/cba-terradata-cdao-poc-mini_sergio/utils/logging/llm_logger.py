"""
LLM Interaction Logger for BTEQ DCF
Captures and logs all LLM requests and responses for debugging and analysis.
"""

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, asdict
import hashlib

logger = logging.getLogger(__name__)


@dataclass
class LLMInteraction:
    """Represents a single LLM interaction for logging."""
    interaction_id: str
    timestamp: datetime
    provider: str
    model: str
    request_type: str  # 'enhancement', 'analysis', 'completion', etc.
    
    # Request details
    prompt: str
    prompt_length: int
    prompt_hash: str
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    
    # Response details
    response: Optional[str] = None
    response_length: Optional[int] = None
    response_hash: Optional[str] = None
    
    # Metadata
    context_data: Optional[Dict[str, Any]] = None
    processing_time_ms: Optional[float] = None
    success: bool = True
    error_message: Optional[str] = None
    
    # Quality metrics
    quality_score: Optional[float] = None
    enhancements_detected: Optional[List[str]] = None


class LLMLogger:
    """Logger for LLM interactions with structured output and rotation."""
    
    def __init__(self, 
                 output_directory: str = "logs/llm_interactions",
                 max_file_size_mb: int = 100,
                 max_files: int = 10,
                 log_requests: bool = True,
                 log_responses: bool = True,
                 enabled: bool = True):
        """
        Initialize LLM logger.
        
        Args:
            output_directory: Directory to store log files
            max_file_size_mb: Maximum size per log file in MB
            max_files: Maximum number of log files to keep
            log_requests: Whether to log request details
            log_responses: Whether to log response details
            enabled: Whether logging is enabled
        """
        self.output_directory = Path(output_directory)
        self.max_file_size_bytes = max_file_size_mb * 1024 * 1024
        self.max_files = max_files
        self.log_requests = log_requests
        self.log_responses = log_responses
        self.enabled = enabled
        
        if self.enabled:
            self._setup_directories()
            self._setup_logging()
    
    def _setup_directories(self):
        """Create necessary directories."""
        self.output_directory.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        (self.output_directory / "detailed").mkdir(exist_ok=True)
        (self.output_directory / "summary").mkdir(exist_ok=True)
        (self.output_directory / "prompts").mkdir(exist_ok=True)
        (self.output_directory / "responses").mkdir(exist_ok=True)
    
    def _setup_logging(self):
        """Setup file logging for LLM interactions."""
        # Create unique logger for this LLMLogger instance to avoid conflicts
        import uuid
        unique_logger_name = f'llm_interactions_{uuid.uuid4().hex[:8]}'
        self.llm_logger = logging.getLogger(unique_logger_name)
        self.llm_logger.setLevel(logging.INFO)
        
        # Always create new handler for this instance
        if True:  # Always setup handlers for unique logger
            # Create rotating file handler
            from logging.handlers import RotatingFileHandler
            
            log_file = self.output_directory / "llm_interactions.log"
            handler = RotatingFileHandler(
                log_file,
                maxBytes=self.max_file_size_bytes,
                backupCount=self.max_files
            )
            
            formatter = logging.Formatter(
                '%(asctime)s - LLM - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.llm_logger.addHandler(handler)
    
    def start_interaction(self, 
                         provider: str,
                         model: str,
                         request_type: str,
                         prompt: str,
                         context_data: Optional[Dict[str, Any]] = None,
                         temperature: Optional[float] = None,
                         max_tokens: Optional[int] = None) -> str:
        """
        Start logging an LLM interaction.
        
        Returns:
            interaction_id for tracking this interaction
        """
        if not self.enabled:
            return "disabled"
        
        # Generate unique interaction ID
        timestamp = datetime.now()
        interaction_id = f"{timestamp.strftime('%Y%m%d_%H%M%S')}_{hashlib.md5(prompt.encode()).hexdigest()[:8]}"
        
        # Create interaction record
        interaction = LLMInteraction(
            interaction_id=interaction_id,
            timestamp=timestamp,
            provider=provider,
            model=model,
            request_type=request_type,
            prompt=prompt,
            prompt_length=len(prompt),
            prompt_hash=hashlib.md5(prompt.encode()).hexdigest(),
            temperature=temperature,
            max_tokens=max_tokens,
            context_data=context_data
        )
        
        # Log the request
        if self.log_requests:
            self._log_request(interaction)
        
        # Store interaction for completion logging
        self._current_interactions = getattr(self, '_current_interactions', {})
        self._current_interactions[interaction_id] = interaction
        
        return interaction_id
    
    def complete_interaction(self,
                           interaction_id: str,
                           response: Optional[str] = None,
                           success: bool = True,
                           error_message: Optional[str] = None,
                           processing_time_ms: Optional[float] = None,
                           quality_score: Optional[float] = None,
                           enhancements_detected: Optional[List[str]] = None):
        """Complete logging an LLM interaction."""
        if not self.enabled or interaction_id == "disabled":
            return
        
        # Get the stored interaction
        current_interactions = getattr(self, '_current_interactions', {})
        if interaction_id not in current_interactions:
            logger.warning(f"Unknown interaction ID: {interaction_id}")
            return
        
        interaction = current_interactions[interaction_id]
        
        # Update with response details
        if response:
            interaction.response = response
            interaction.response_length = len(response)
            interaction.response_hash = hashlib.md5(response.encode()).hexdigest()
        
        interaction.success = success
        interaction.error_message = error_message
        interaction.processing_time_ms = processing_time_ms
        interaction.quality_score = quality_score
        interaction.enhancements_detected = enhancements_detected
        
        # Log the response
        if self.log_responses:
            self._log_response(interaction)
        
        # Log the complete interaction
        self._log_complete_interaction(interaction)
        
        # Clean up
        del current_interactions[interaction_id]
    
    def _log_request(self, interaction: LLMInteraction):
        """Log the request details."""
        log_data = {
            "type": "REQUEST",
            "interaction_id": interaction.interaction_id,
            "timestamp": interaction.timestamp.isoformat(),
            "provider": interaction.provider,
            "model": interaction.model,
            "request_type": interaction.request_type,
            "prompt_length": interaction.prompt_length,
            "prompt_hash": interaction.prompt_hash,
            "temperature": interaction.temperature,
            "max_tokens": interaction.max_tokens,
            "context_keys": list(interaction.context_data.keys()) if interaction.context_data else []
        }
        
        self.llm_logger.info(f"REQUEST: {json.dumps(log_data)}")
        
        # Save detailed prompt to file with descriptive naming (phase_model_timestamp)
        if self.log_requests:
            # Create descriptive filename: phase_model_timestamp_prompt.txt
            safe_model_name = interaction.model.replace('-', '_').replace('.', '_')
            filename = f"{interaction.request_type}_{safe_model_name}_{interaction.interaction_id}_prompt.txt"
            prompt_file = self.output_directory / "prompts" / filename
            with open(prompt_file, 'w', encoding='utf-8') as f:
                f.write(f"# LLM Request\n")
                f.write(f"Interaction ID: {interaction.interaction_id}\n")
                f.write(f"Timestamp: {interaction.timestamp.isoformat()}\n")
                f.write(f"Provider: {interaction.provider}\n")
                f.write(f"Model: {interaction.model}\n")
                f.write(f"Request Type: {interaction.request_type}\n")
                f.write(f"Temperature: {interaction.temperature}\n")
                f.write(f"Max Tokens: {interaction.max_tokens}\n")
                f.write(f"\n# Context Data\n")
                if interaction.context_data:
                    f.write(json.dumps(interaction.context_data, indent=2))
                f.write(f"\n\n# Prompt\n")
                f.write(interaction.prompt)
    
    def _log_response(self, interaction: LLMInteraction):
        """Log the response details."""
        log_data = {
            "type": "RESPONSE",
            "interaction_id": interaction.interaction_id,
            "success": interaction.success,
            "response_length": interaction.response_length,
            "response_hash": interaction.response_hash,
            "processing_time_ms": interaction.processing_time_ms,
            "quality_score": interaction.quality_score,
            "enhancements_count": len(interaction.enhancements_detected) if interaction.enhancements_detected else 0,
            "error_message": interaction.error_message
        }
        
        self.llm_logger.info(f"RESPONSE: {json.dumps(log_data)}")
        
        # Save detailed response to file with descriptive naming (phase_model_timestamp)
        if self.log_responses and interaction.response:
            # Create descriptive filename: phase_model_timestamp_response.txt
            safe_model_name = interaction.model.replace('-', '_').replace('.', '_')
            filename = f"{interaction.request_type}_{safe_model_name}_{interaction.interaction_id}_response.txt"
            response_file = self.output_directory / "responses" / filename
            with open(response_file, 'w', encoding='utf-8') as f:
                f.write(f"# LLM Response\n")
                f.write(f"Interaction ID: {interaction.interaction_id}\n")
                f.write(f"Success: {interaction.success}\n")
                f.write(f"Processing Time: {interaction.processing_time_ms}ms\n")
                f.write(f"Quality Score: {interaction.quality_score}\n")
                if interaction.enhancements_detected:
                    f.write(f"Enhancements: {', '.join(interaction.enhancements_detected)}\n")
                if interaction.error_message:
                    f.write(f"Error: {interaction.error_message}\n")
                f.write(f"\n# Response\n")
                f.write(interaction.response)
    
    def _log_complete_interaction(self, interaction: LLMInteraction):
        """Log the complete interaction summary."""
        # Summary log entry
        summary_data = {
            "interaction_id": interaction.interaction_id,
            "timestamp": interaction.timestamp.isoformat(),
            "provider": interaction.provider,
            "model": interaction.model,
            "request_type": interaction.request_type,
            "prompt_length": interaction.prompt_length,
            "response_length": interaction.response_length,
            "processing_time_ms": interaction.processing_time_ms,
            "success": interaction.success,
            "quality_score": interaction.quality_score,
            "enhancements_count": len(interaction.enhancements_detected) if interaction.enhancements_detected else 0
        }
        
        self.llm_logger.info(f"COMPLETE: {json.dumps(summary_data)}")
        
        # Save detailed interaction to JSON file
        detailed_file = self.output_directory / "detailed" / f"{interaction.interaction_id}.json"
        with open(detailed_file, 'w', encoding='utf-8') as f:
            # Convert to dictionary, handling datetime serialization
            interaction_dict = asdict(interaction)
            interaction_dict['timestamp'] = interaction.timestamp.isoformat()
            json.dump(interaction_dict, f, indent=2, ensure_ascii=False)
    
    def log_simple_interaction(self,
                             provider: str,
                             model: str,
                             request_type: str,
                             prompt: str,
                             response: Optional[str] = None,
                             success: bool = True,
                             error_message: Optional[str] = None,
                             processing_time_ms: Optional[float] = None,
                             context_data: Optional[Dict[str, Any]] = None):
        """Log a complete interaction in one call."""
        interaction_id = self.start_interaction(
            provider=provider,
            model=model,
            request_type=request_type,
            prompt=prompt,
            context_data=context_data
        )
        
        self.complete_interaction(
            interaction_id=interaction_id,
            response=response,
            success=success,
            error_message=error_message,
            processing_time_ms=processing_time_ms
        )
        
        return interaction_id
    
    def get_interaction_summary(self, days: int = 7) -> Dict[str, Any]:
        """Get summary of LLM interactions over the last N days."""
        if not self.enabled:
            return {"enabled": False}
        
        try:
            summary = {
                "enabled": True,
                "days_analyzed": days,
                "total_interactions": 0,
                "successful_interactions": 0,
                "failed_interactions": 0,
                "providers": {},
                "models": {},
                "request_types": {},
                "avg_processing_time_ms": 0,
                "avg_quality_score": 0,
                "total_prompt_length": 0,
                "total_response_length": 0
            }
            
            # Read recent interaction files
            detailed_dir = self.output_directory / "detailed"
            if not detailed_dir.exists():
                return summary
            
            cutoff_time = datetime.now().timestamp() - (days * 24 * 3600)
            processing_times = []
            quality_scores = []
            
            for json_file in detailed_dir.glob("*.json"):
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        interaction_data = json.load(f)
                    
                    # Check if within time range
                    interaction_time = datetime.fromisoformat(interaction_data['timestamp']).timestamp()
                    if interaction_time < cutoff_time:
                        continue
                    
                    summary["total_interactions"] += 1
                    
                    if interaction_data.get('success', False):
                        summary["successful_interactions"] += 1
                    else:
                        summary["failed_interactions"] += 1
                    
                    # Provider stats
                    provider = interaction_data.get('provider', 'unknown')
                    summary["providers"][provider] = summary["providers"].get(provider, 0) + 1
                    
                    # Model stats
                    model = interaction_data.get('model', 'unknown')
                    summary["models"][model] = summary["models"].get(model, 0) + 1
                    
                    # Request type stats
                    req_type = interaction_data.get('request_type', 'unknown')
                    summary["request_types"][req_type] = summary["request_types"].get(req_type, 0) + 1
                    
                    # Performance metrics
                    if interaction_data.get('processing_time_ms'):
                        processing_times.append(interaction_data['processing_time_ms'])
                    
                    if interaction_data.get('quality_score'):
                        quality_scores.append(interaction_data['quality_score'])
                    
                    # Size metrics
                    summary["total_prompt_length"] += interaction_data.get('prompt_length', 0)
                    summary["total_response_length"] += interaction_data.get('response_length', 0)
                    
                except Exception as e:
                    logger.warning(f"Error reading interaction file {json_file}: {e}")
            
            # Calculate averages
            if processing_times:
                summary["avg_processing_time_ms"] = sum(processing_times) / len(processing_times)
            
            if quality_scores:
                summary["avg_quality_score"] = sum(quality_scores) / len(quality_scores)
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generating interaction summary: {e}")
            return {"enabled": True, "error": str(e)}


# Global LLM logger instance
_llm_logger: Optional[LLMLogger] = None

def get_llm_logger(config: Optional[Dict[str, Any]] = None) -> LLMLogger:
    """Get the global LLM logger instance."""
    global _llm_logger
    if _llm_logger is None:
        if config:
            _llm_logger = LLMLogger(**config)
        else:
            # Use minimal configuration that doesn't create directories by default
            _llm_logger = LLMLogger(
                output_directory="logs/llm_interactions",
                enabled=False  # Disabled by default to prevent global directory creation
            )
    return _llm_logger

def init_llm_logging(config: Dict[str, Any]):
    """Initialize LLM logging with configuration."""
    global _llm_logger
    _llm_logger = LLMLogger(**config)
    return _llm_logger
