"""
Improved LLM Logger with unified structure and better naming conventions.
Addresses duplicate folders and provides clear separation of request/response.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum
import hashlib
from datetime import datetime


class ConversionType(Enum):
    """Types of conversions for clear logging distinction."""
    BTEQ_TO_SP = "bteq2sp"           # BTEQ to Stored Procedure
    BTEQ_TO_DBT = "bteq2dbt"         # BTEQ to DBT Model
    SP_TO_DBT = "sp2dbt"             # Stored Procedure to DBT
    ANALYSIS = "analysis"             # Analysis/parsing tasks


@dataclass
class LLMRequest:
    """Raw LLM request data."""
    conversion_type: ConversionType
    procedure_name: str
    model: str
    prompt: str
    context_data: Dict[str, Any]


@dataclass
class LLMResponse:
    """Raw LLM response data."""
    conversion_type: ConversionType
    procedure_name: str
    model: str
    response: str
    success: bool
    processing_time_ms: int
    quality_score: Optional[float] = None
    error_message: Optional[str] = None


class ImprovedLLMLogger:
    """
    Improved LLM logger with:
    - Single consolidated interactions folder
    - Clear naming convention (bteq2sp/bteq2dbt)
    - Separate request/response files
    - No redundant timestamps
    """
    
    def __init__(self, output_directory: str):
        """Initialize with single LLM interactions directory."""
        self.output_directory = Path(output_directory)
        self.llm_interactions_dir = self.output_directory / "llm_interactions"
        self._setup_directories()
        
        # Setup logger
        self.logger = logging.getLogger(f'improved_llm_logger')
        self.logger.setLevel(logging.INFO)
        
    def _setup_directories(self):
        """Setup unified directory structure."""
        directories = [
            self.llm_interactions_dir,
            self.llm_interactions_dir / "requests",
            self.llm_interactions_dir / "responses", 
            self.llm_interactions_dir / "metadata",
            self.llm_interactions_dir / "summary"
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def _generate_filename_base(self, 
                               conversion_type: ConversionType,
                               procedure_name: str, 
                               model: str) -> str:
        """
        Generate descriptive filename without timestamps.
        
        Format: {conversion_type}_{procedure_name}_{model_short}
        Example: bteq2dbt_acct_baln_adj_rule_claude4
        """
        # Shorten model names
        model_short = model.replace("claude-4-sonnet", "claude4") \
                          .replace("snowflake-llama-3.3-70b", "llama33") \
                          .replace("-", "_")
        
        # Clean procedure name (lowercase, remove common suffixes)
        proc_clean = procedure_name.lower() \
                                  .replace("_isrt", "") \
                                  .replace("_updt", "") \
                                  .replace("_delt", "")
        
        return f"{conversion_type.value}_{proc_clean}_{model_short}"
    
    def log_request(self, request: LLMRequest) -> str:
        """
        Log LLM request to separate file in raw form.
        
        Returns: interaction_id for linking request/response
        """
        filename_base = self._generate_filename_base(
            request.conversion_type,
            request.procedure_name,
            request.model
        )
        
        # Generate unique interaction ID
        interaction_id = hashlib.md5(
            f"{filename_base}_{datetime.now().isoformat()}".encode()
        ).hexdigest()[:8]
        
        # Save raw request
        request_file = self.llm_interactions_dir / "requests" / f"{filename_base}_request.txt"
        
        with open(request_file, 'w', encoding='utf-8') as f:
            f.write("# LLM REQUEST\n")
            f.write(f"Interaction ID: {interaction_id}\n")
            f.write(f"Conversion Type: {request.conversion_type.value}\n")
            f.write(f"Procedure: {request.procedure_name}\n") 
            f.write(f"Model: {request.model}\n")
            f.write(f"Prompt Length: {len(request.prompt)} characters\n")
            f.write(f"Timestamp: {datetime.now().isoformat()}\n")
            f.write("\n" + "="*80 + "\n")
            f.write("RAW PROMPT:\n")
            f.write("="*80 + "\n")
            f.write(request.prompt)
        
        # Save metadata
        metadata = {
            "interaction_id": interaction_id,
            "conversion_type": request.conversion_type.value,
            "procedure_name": request.procedure_name,
            "model": request.model,
            "prompt_length": len(request.prompt),
            "context_data": request.context_data,
            "timestamp": datetime.now().isoformat()
        }
        
        metadata_file = self.llm_interactions_dir / "metadata" / f"{filename_base}_request_metadata.json"
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)
        
        self.logger.info(f"📝 Request logged: {request.conversion_type.value} - {request.procedure_name} ({request.model})")
        return interaction_id
    
    def log_response(self, response: LLMResponse, interaction_id: str):
        """Log LLM response to separate file in raw form."""
        filename_base = self._generate_filename_base(
            response.conversion_type,
            response.procedure_name,
            response.model
        )
        
        # Save raw response  
        response_file = self.llm_interactions_dir / "responses" / f"{filename_base}_response.txt"
        
        status = "SUCCESS" if response.success else "ERROR"
        
        with open(response_file, 'w', encoding='utf-8') as f:
            f.write("# LLM RESPONSE\n")
            f.write(f"Interaction ID: {interaction_id}\n")
            f.write(f"Conversion Type: {response.conversion_type.value}\n")
            f.write(f"Procedure: {response.procedure_name}\n")
            f.write(f"Model: {response.model}\n")
            f.write(f"Status: {status}\n")
            f.write(f"Processing Time: {response.processing_time_ms}ms\n")
            f.write(f"Response Length: {len(response.response)} characters\n")
            f.write(f"Quality Score: {response.quality_score}\n")
            if response.error_message:
                f.write(f"Error: {response.error_message}\n")
            f.write(f"Timestamp: {datetime.now().isoformat()}\n")
            f.write("\n" + "="*80 + "\n") 
            f.write("RAW RESPONSE:\n")
            f.write("="*80 + "\n")
            f.write(response.response)
        
        # Save response metadata
        metadata = {
            "interaction_id": interaction_id,
            "conversion_type": response.conversion_type.value,
            "procedure_name": response.procedure_name,
            "model": response.model,
            "success": response.success,
            "processing_time_ms": response.processing_time_ms,
            "response_length": len(response.response),
            "quality_score": response.quality_score,
            "error_message": response.error_message,
            "timestamp": datetime.now().isoformat()
        }
        
        metadata_file = self.llm_interactions_dir / "metadata" / f"{filename_base}_response_metadata.json"
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)
        
        self.logger.info(f"📝 Response logged: {response.conversion_type.value} - {status} ({response.processing_time_ms}ms)")
    
    def generate_summary(self) -> Dict[str, Any]:
        """Generate summary of all interactions."""
        summary = {
            "total_interactions": 0,
            "by_conversion_type": {},
            "by_model": {},
            "success_rate": 0.0,
            "average_processing_time": 0.0,
            "files": {
                "requests": [],
                "responses": [],
                "metadata": []
            }
        }
        
        # Count files by type
        for request_file in (self.llm_interactions_dir / "requests").glob("*.txt"):
            summary["files"]["requests"].append(request_file.name)
            
        for response_file in (self.llm_interactions_dir / "responses").glob("*.txt"):
            summary["files"]["responses"].append(response_file.name)
            
        for metadata_file in (self.llm_interactions_dir / "metadata").glob("*.json"):
            summary["files"]["metadata"].append(metadata_file.name)
        
        summary["total_interactions"] = len(summary["files"]["responses"])
        
        # Save summary
        summary_file = self.llm_interactions_dir / "summary" / "interactions_summary.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)
            
        return summary


# Helper functions for easy integration
def create_improved_llm_logger(output_directory: str) -> ImprovedLLMLogger:
    """Factory function to create improved logger."""
    return ImprovedLLMLogger(output_directory)


def log_bteq_to_sp_request(logger: ImprovedLLMLogger, 
                          procedure_name: str,
                          model: str, 
                          prompt: str,
                          context_data: Dict[str, Any] = None) -> str:
    """Helper for BTEQ to SP requests."""
    request = LLMRequest(
        conversion_type=ConversionType.BTEQ_TO_SP,
        procedure_name=procedure_name,
        model=model,
        prompt=prompt,
        context_data=context_data or {}
    )
    return logger.log_request(request)


def log_bteq_to_dbt_request(logger: ImprovedLLMLogger,
                           procedure_name: str, 
                           model: str,
                           prompt: str,
                           context_data: Dict[str, Any] = None) -> str:
    """Helper for BTEQ to DBT requests.""" 
    request = LLMRequest(
        conversion_type=ConversionType.BTEQ_TO_DBT,
        procedure_name=procedure_name,
        model=model,
        prompt=prompt,
        context_data=context_data or {}
    )
    return logger.log_request(request)
