"""
Processing Mode Constants and Descriptions

Clean processing mode definitions with clear logical progression.
"""
from typing import List


class ProcessingMode:
    """Clean processing mode constants with clear logical progression."""
    V1_PRSCRIP_SP = "v1_prscrip_sp"                                  # Pure prescriptive SQLGlot BTEQ→SP conversion
    V2_PRSCRIP_CLAUDE_SP = "v2_prscrip_claude_sp"                    # Prescriptive + Claude enhanced SP
    V3_PRSCRIP_CLAUDE_SP_CLAUDE_DBT = "v3_prscrip_claude_sp_claude_dbt"  # Prescriptive + Claude SP + Claude DBT
    V4_PRSCRIP_CLAUDE_LLAMA_SP = "v4_prscrip_claude_llama_sp"        # Prescriptive + Claude vs Llama comparison
    V5_CLAUDE_DBT = "v5_claude_dbt"                                  # Direct BTEQ → DBT conversion using Claude
    
    # Legacy aliases for backward compatibility
    PRESCRIPTIVE = V1_PRSCRIP_SP
    
    @classmethod
    def all_modes(cls) -> List[str]:
        """Get all available processing modes."""
        return [
            cls.V1_PRSCRIP_SP,
            cls.V2_PRSCRIP_CLAUDE_SP,
            cls.V3_PRSCRIP_CLAUDE_SP_CLAUDE_DBT,
            cls.V4_PRSCRIP_CLAUDE_LLAMA_SP,
            cls.V5_CLAUDE_DBT
        ]
        
    @classmethod
    def get_mode_description(cls, mode: str) -> str:
        """Get detailed description of processing mode."""
        descriptions = {
            cls.V1_PRSCRIP_SP: "Pure prescriptive: SQLGlot-based BTEQ→Snowflake SP conversion (fastest, no LLM)",
            cls.V2_PRSCRIP_CLAUDE_SP: "Prescriptive + Claude: Traditional pipeline enhanced by Claude-4-sonnet (1 LLM call)",
            cls.V3_PRSCRIP_CLAUDE_SP_CLAUDE_DBT: "Full Claude pipeline: Prescriptive + Claude SP + Claude DBT conversion (2 LLM calls)",
            cls.V4_PRSCRIP_CLAUDE_LLAMA_SP: "Multi-model comparison: Prescriptive + Claude vs Llama, pick best SP (2 LLM calls)",
            cls.V5_CLAUDE_DBT: "Direct DBT: BTEQ directly to DBT models using Claude (streamlined, 1 LLM call)"
        }
        return descriptions.get(mode, "Unknown processing mode")
