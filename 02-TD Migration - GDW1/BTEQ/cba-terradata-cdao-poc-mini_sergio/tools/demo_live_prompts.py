#!/usr/bin/env python3
"""
Live demonstration of prompts sent to LLMs during agentic processing.
Shows actual prompts constructed by different agents for a real BTEQ file.
"""

import sys
from pathlib import Path
import time

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def load_sample_bteq():
    """Load a sample BTEQ file for demonstration."""
    bteq_file = project_root / "current_state/bteq_bteq/ACCT_BALN_BKDT_AUDT_ISRT.bteq"
    
    if bteq_file.exists():
        with open(bteq_file, 'r') as f:
            content = f.read()
        return content, bteq_file.name
    else:
        # Fallback sample if file not found
        return """
.RUN FILE=%%BTEQ_LOGON_SCRIPT%%
.IF ERRORCODE <> 0 THEN .GOTO EXITERR

.SET QUIET OFF
.SET ECHOREQ ON
.SET FORMAT OFF
.SET WIDTH 120

DELETE FROM %%PVPATY%%.ACCT_BALN_BKDT_AUDT WHERE PROS_DT = CURRENT_DATE - 1;

.IF ERRORCODE <> 0 THEN .GOTO EXITERR

INSERT INTO %%PVPATY%%.ACCT_BALN_BKDT_AUDT
(PROS_DT, ACCT_ID, BALN_AMT, AUDT_TS, BTCH_ID)
SELECT 
    CURRENT_DATE - 1 AS PROS_DT,
    ACCT_ID,
    SUM(BALN_AMT) AS BALN_AMT,
    CURRENT_TIMESTAMP AS AUDT_TS,
    %%BTCH_ID%% AS BTCH_ID
FROM %%VPATY%%.ACCT_BALN_STG
WHERE PROS_DT = CURRENT_DATE - 1
AND STATUS_CD = 'A'
GROUP BY ACCT_ID;

.IF ERRORCODE <> 0 THEN .GOTO EXITERR

.LABEL EXITERR
.QUIT ERRORCODE
""", "sample_audit_insert.bteq"

def demo_analysis_agent_prompt():
    """Demonstrate BteqAnalysisAgent prompt construction."""
    print("🎯 BTEQ ANALYSIS AGENT - PROMPT CONSTRUCTION")
    print("=" * 60)
    
    bteq_content, filename = load_sample_bteq()
    
    print(f"📁 Processing file: {filename}")
    print(f"📊 File size: {len(bteq_content)} characters, {len(bteq_content.splitlines())} lines")
    
    # Show what analysis prompt would look like
    analysis_prompt = f"""
# BTEQ Complexity Analysis Request

## Task
Analyze the following BTEQ script and provide detailed complexity assessment and migration recommendations.

## BTEQ Script to Analyze
```sql
{bteq_content.strip()}
```

## Analysis Requirements

### 1. Complexity Assessment
- Assign complexity score (0.0-1.0 scale)
- Identify complexity factors
- Assess migration difficulty

### 2. Feature Detection
- BTEQ-specific commands (.RUN, .IF, .GOTO, .LABEL, etc.)
- Variable substitution patterns (%%VAR%%)
- Control flow complexity
- SQL complexity (joins, aggregations, window functions)
- Error handling patterns

### 3. Migration Strategy
- Recommended approach for Snowflake conversion
- Potential challenges and solutions
- Required manual review items
- Performance considerations

### 4. Business Logic Understanding
- Purpose of the script
- Data flow and transformations
- Business rules embedded in logic

## Output Format
Provide analysis in JSON format with:
- complexity_score: float (0.0-1.0)
- complexity_factors: list of strings
- detected_features: list of BTEQ features
- migration_strategy: string description
- challenges: list of potential issues
- manual_review_items: list of items needing attention
- recommendations: list of specific recommendations

Focus on actionable insights for the migration team.
"""
    
    print(f"\n🤖 ANALYSIS AGENT PROMPT:")
    print("─" * 60)
    print(analysis_prompt)
    
    return analysis_prompt, bteq_content

def demo_generation_agent_prompts():
    """Demonstrate MultiModelGenerationAgent prompt construction."""
    print("\n\n🔧 MULTI-MODEL GENERATION AGENT - PROMPT CONSTRUCTION")
    print("=" * 60)
    
    bteq_content, filename = load_sample_bteq()
    
    # Simulated analysis results
    analysis_results = {
        "complexity_score": 0.4,
        "complexity_factors": ["BTEQ control flow", "Variable substitution", "Basic aggregation"],
        "detected_features": [".IF", ".GOTO", ".LABEL", "%%VAR%%", "DELETE/INSERT", "GROUP BY"],
        "migration_strategy": "Standard conversion with error handling enhancement",
        "challenges": ["Variable substitution", "Error handling translation"],
        "recommendations": ["Use stored procedure parameters", "Implement proper exception handling"]
    }
    
    # Show generation prompts for both models
    base_generation_prompt = f"""
# BTEQ to Snowflake Stored Procedure Conversion

## Task
Convert the following BTEQ script to an optimized Snowflake stored procedure with enhanced error handling and logging.

## Original BTEQ Script
```sql
{bteq_content.strip()}
```

## Analysis Context
- **Complexity Score**: {analysis_results['complexity_score']}
- **Key Features**: {', '.join(analysis_results['detected_features'])}
- **Migration Strategy**: {analysis_results['migration_strategy']}
- **Main Challenges**: {', '.join(analysis_results['challenges'])}

## Conversion Requirements

### 1. Stored Procedure Structure
- Use proper Snowflake stored procedure syntax
- Include parameter definitions for variables
- Implement comprehensive error handling
- Add transaction management

### 2. BTEQ Command Translation
- Convert .IF/.GOTO/.LABEL to proper conditional logic
- Replace %%VAR%% with procedure parameters
- Handle ERRORCODE checks with EXCEPTION blocks
- Translate CURRENT_DATE and CURRENT_TIMESTAMP

### 3. SQL Optimization
- Optimize for Snowflake architecture
- Use appropriate data types
- Include performance hints where beneficial
- Ensure proper indexing suggestions

### 4. Error Handling & Logging
- Comprehensive EXCEPTION blocks
- Detailed error messages with context
- Audit trail logging
- Return proper status codes

## Expected Output
Generate a complete, production-ready Snowflake stored procedure that:
- Maintains original business logic
- Follows Snowflake best practices
- Includes comprehensive error handling
- Is well-documented with comments
- Can be deployed immediately
"""

    print(f"🤖 BASE GENERATION PROMPT (sent to both models):")
    print("─" * 60)
    print(base_generation_prompt)
    
    # Model-specific customizations
    claude_specific = """
### Additional Requirements for Claude-4-Sonnet:
- Focus on code quality and comprehensive error handling
- Include detailed documentation and comments
- Provide migration notes and explanations
- Suggest advanced Snowflake features where appropriate
"""
    
    llama_specific = """
### Additional Requirements for Snowflake-Llama-3.3-70B:
- Focus on performance optimization
- Use efficient Snowflake patterns
- Minimize resource usage
- Optimize for fast execution
"""
    
    print(f"\n🎭 CLAUDE-4-SONNET SPECIFIC ADDITIONS:")
    print("─" * 40)
    print(claude_specific)
    
    print(f"\n⚡ SNOWFLAKE-LLAMA-3.3-70B SPECIFIC ADDITIONS:")
    print("─" * 40)
    print(llama_specific)
    
    return base_generation_prompt, claude_specific, llama_specific

def demo_validation_agent_prompts():
    """Demonstrate ValidationAgent prompt construction."""
    print("\n\n✅ VALIDATION AGENT - PROMPT CONSTRUCTION")
    print("=" * 60)
    
    # Sample generated procedure for validation
    sample_procedure = """
CREATE OR REPLACE PROCEDURE ACCT_BALN_BKDT_AUDT_ISRT(
    BTEQ_LOGON_SCRIPT VARCHAR,
    PVPATY VARCHAR DEFAULT 'PROD_PATY',
    VPATY VARCHAR DEFAULT 'VIEW_PATY', 
    BTCH_ID INTEGER
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    error_msg STRING;
    rows_affected INTEGER;
BEGIN
    -- Delete existing audit records for previous day
    DELETE FROM IDENTIFIER(:PVPATY || '.ACCT_BALN_BKDT_AUDT') 
    WHERE PROS_DT = CURRENT_DATE - 1;
    
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    
    -- Insert new audit records
    INSERT INTO IDENTIFIER(:PVPATY || '.ACCT_BALN_BKDT_AUDT')
    (PROS_DT, ACCT_ID, BALN_AMT, AUDT_TS, BTCH_ID)
    SELECT 
        CURRENT_DATE - 1 AS PROS_DT,
        ACCT_ID,
        SUM(BALN_AMT) AS BALN_AMT,
        CURRENT_TIMESTAMP AS AUDT_TS,
        :BTCH_ID AS BTCH_ID
    FROM IDENTIFIER(:VPATY || '.ACCT_BALN_STG')
    WHERE PROS_DT = CURRENT_DATE - 1
    AND STATUS_CD = 'A'
    GROUP BY ACCT_ID;
    
    RETURN 'SUCCESS: Processed audit records';
    
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;
"""

    validation_prompt = f"""
# SQL Validation and Quality Assessment

## Task
Validate the following Snowflake stored procedure for syntax, performance, and best practices.

## Stored Procedure to Validate
```sql
{sample_procedure.strip()}
```

## Validation Requirements

### 1. Syntax Validation
- Check for valid Snowflake SQL syntax
- Verify procedure structure and declarations
- Validate parameter definitions
- Check data type usage

### 2. Performance Analysis
- Identify potential performance issues
- Suggest optimization opportunities
- Review query patterns for efficiency
- Check for proper indexing implications

### 3. Best Practices Review
- Snowflake coding standards compliance
- Error handling completeness
- Security considerations
- Maintainability factors

### 4. Specific Checks
- Parameter handling and validation
- Dynamic SQL safety (IDENTIFIER usage)
- Transaction management
- Exception handling coverage
- Return value appropriateness

## Output Format
Provide validation results with:
- syntax_valid: boolean
- performance_score: float (0.0-1.0)
- best_practices_score: float (0.0-1.0)
- issues_found: list of issues with severity
- recommendations: list of specific improvements
- optimization_suggestions: list of performance improvements

Rate each aspect and provide actionable feedback.
"""
    
    print(f"🔍 VALIDATION AGENT PROMPT:")
    print("─" * 60)
    print(validation_prompt)
    
    return validation_prompt

def demo_error_correction_prompts():
    """Demonstrate ErrorCorrectionAgent prompt construction."""
    print("\n\n🛠️ ERROR CORRECTION AGENT - PROMPT CONSTRUCTION")
    print("=" * 60)
    
    # Simulated validation issues
    validation_issues = [
        {
            "type": "performance",
            "severity": "medium", 
            "description": "DELETE without WHERE optimization could be slow",
            "location": "Line 12: DELETE statement",
            "suggestion": "Consider using TRUNCATE or partition pruning"
        },
        {
            "type": "best_practice",
            "severity": "low",
            "description": "Missing input parameter validation",
            "location": "Procedure parameters",
            "suggestion": "Add parameter validation at procedure start"
        }
    ]
    
    correction_prompt = f"""
# Error Correction and Code Improvement

## Task
Improve the following Snowflake stored procedure based on validation feedback and identified issues.

## Current Procedure
```sql
[Previous procedure code here...]
```

## Validation Issues to Address
{chr(10).join([f"**{issue['severity'].upper()} - {issue['type']}**: {issue['description']}" + 
              f" (Location: {issue['location']})" + 
              f" → Suggestion: {issue['suggestion']}" 
              for issue in validation_issues])}

## Improvement Requirements

### 1. Address Identified Issues
- Fix each validation issue with appropriate solution
- Maintain original business logic
- Ensure changes don't introduce new problems

### 2. Code Quality Enhancement
- Improve readability and maintainability
- Add comprehensive comments
- Optimize performance where possible
- Follow Snowflake best practices

### 3. Error Handling Improvement
- Enhance exception handling
- Add more specific error messages
- Improve error recovery mechanisms
- Add validation for edge cases

## Output Requirements
- Provide the complete improved stored procedure
- Explain each change made and why
- Highlight performance improvements
- Document any assumptions or trade-offs

Focus on creating production-ready, robust code.
"""
    
    print(f"🔧 ERROR CORRECTION AGENT PROMPT:")
    print("─" * 60)
    print(correction_prompt)
    
    return correction_prompt

def demo_judgment_agent_prompts():
    """Demonstrate JudgmentAgent prompt construction for response comparison."""
    print("\n\n⚖️ JUDGMENT AGENT - PROMPT CONSTRUCTION")
    print("=" * 60)
    
    # Sample responses from different models
    claude_response = """
CREATE OR REPLACE PROCEDURE ACCT_BALN_BKDT_AUDT_ISRT(...)
RETURNS STRING
LANGUAGE SQL
AS $$
BEGIN
    -- Comprehensive implementation with detailed error handling
    -- [45 lines of well-documented code]
END;
$$;
"""
    
    llama_response = """
CREATE OR REPLACE PROCEDURE ACCT_BALN_BKDT_AUDT_ISRT(...)
RETURNS STRING  
LANGUAGE SQL
AS $$
BEGIN
    -- Performance-optimized implementation
    -- [28 lines of efficient code]
END;
$$;
"""
    
    judgment_prompt = f"""
# Multi-Model Response Quality Assessment

## Task
Compare and evaluate multiple AI-generated Snowflake stored procedures for the same BTEQ conversion task.

## Response A: Claude-4-Sonnet
```sql
{claude_response.strip()}
```

## Response B: Snowflake-Llama-3.3-70B
```sql
{llama_response.strip()}
```

## Evaluation Criteria

### 1. Functional Correctness (Weight: 30%)
- Business logic preservation
- Correct SQL syntax
- Proper parameter handling
- Error handling completeness

### 2. Code Quality (Weight: 25%)
- Readability and maintainability
- Documentation and comments
- Following best practices
- Code organization

### 3. Performance (Weight: 25%)
- Execution efficiency
- Resource utilization
- Scalability considerations
- Snowflake optimization

### 4. Robustness (Weight: 20%)
- Error handling coverage
- Edge case handling
- Security considerations
- Production readiness

## Scoring Requirements
For each response provide:
- Overall score (0.0-1.0)
- Individual criterion scores
- Detailed reasoning for scores
- Specific strengths and weaknesses
- Recommendation for selection

## Selection Criteria
Determine which response is better overall and explain why, considering:
- Context of the original BTEQ complexity
- Intended use case (development vs production)
- Team capabilities and preferences
- Performance vs maintainability trade-offs
"""
    
    print(f"⚖️ JUDGMENT AGENT PROMPT:")
    print("─" * 60)
    print(judgment_prompt)
    
    return judgment_prompt

def run_live_demo():
    """Run a live demonstration showing actual prompts sent to LLMs."""
    print("🚀 LIVE AGENTIC PROMPT DEMONSTRATION")
    print("=" * 80)
    print("Showing actual prompts constructed and sent to LLMs during BTEQ migration\n")
    
    # Demonstrate each agent's prompt construction
    analysis_prompt, bteq_content = demo_analysis_agent_prompt()
    
    generation_prompt, claude_add, llama_add = demo_generation_agent_prompts()
    
    validation_prompt = demo_validation_agent_prompts()
    
    correction_prompt = demo_error_correction_prompts()
    
    judgment_prompt = demo_judgment_agent_prompts()
    
    print(f"\n\n📊 PROMPT SUMMARY")
    print("=" * 60)
    print(f"✅ Analysis Prompt: {len(analysis_prompt)} characters")
    print(f"✅ Generation Prompt: {len(generation_prompt)} characters") 
    print(f"✅ Validation Prompt: {len(validation_prompt)} characters")
    print(f"✅ Correction Prompt: {len(correction_prompt)} characters")
    print(f"✅ Judgment Prompt: {len(judgment_prompt)} characters")
    
    print(f"\n💡 KEY OBSERVATIONS:")
    print("─" * 40)
    print("• Each agent constructs specialized prompts for its role")
    print("• Prompts include rich context from previous agents")
    print("• Model-specific customizations optimize for strengths")
    print("• Progressive refinement through agent handoffs")
    print("• Comprehensive validation and quality assessment")
    
    print(f"\n🔄 TYPICAL WORKFLOW:")
    print("─" * 40)
    print("1. Analysis Agent → Complexity assessment prompt")
    print("2. Generation Agent → Model-specific generation prompts")  
    print("3. Validation Agent → Quality assessment prompt")
    print("4. Correction Agent → Improvement-focused prompt")
    print("5. Judgment Agent → Comparative evaluation prompt")
    
    return {
        'analysis': analysis_prompt,
        'generation': generation_prompt,
        'validation': validation_prompt,
        'correction': correction_prompt,
        'judgment': judgment_prompt
    }

if __name__ == "__main__":
    prompts = run_live_demo()
