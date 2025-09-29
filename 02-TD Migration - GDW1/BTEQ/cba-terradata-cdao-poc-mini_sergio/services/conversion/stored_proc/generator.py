"""Snowflake stored procedure generator from BTEQ parser output."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from datetime import datetime

# Use absolute imports to fix packaging issues
try:
    from services.parsing.bteq.tokens import ParserResult, ControlStatement, ControlType, SqlBlock
except ImportError:
    # Fallback for when running as part of package
    from services.parsing.bteq.tokens import ParserResult, ControlStatement, ControlType, SqlBlock


@dataclass
class GeneratedProcedure:
    """Result of stored procedure generation."""
    name: str
    sql: str
    parameters: List[str]
    error_handling: List[str]
    warnings: List[str]


class SnowflakeSPGenerator:
    """Converts BTEQ parser output to Snowflake stored procedures."""
    
    def __init__(self):
        self.procedure_counter = 0
        
    def generate(self, 
                 parser_result: ParserResult, 
                 procedure_name: Optional[str] = None,
                 original_bteq: Optional[str] = None) -> GeneratedProcedure:
        """Generate Snowflake stored procedure from BTEQ parser result.
        
        Args:
            parser_result: Output from BTEQ lexer
            procedure_name: Optional procedure name override
            original_bteq: Original BTEQ script for reference comments
            
        Returns:
            GeneratedProcedure with SQL and metadata
        """
        if not procedure_name:
            procedure_name = f"BTEQ_CONVERTED_PROC_{self.procedure_counter}"
            self.procedure_counter += 1
            
        warnings = []
        error_handling = []
        
        # Extract procedure parameters from control statements
        parameters = self._extract_parameters(parser_result.controls, warnings)
        
        # Generate procedure body
        body_lines = []
        
        # Add header comment
        if original_bteq:
            body_lines.extend(self._generate_header_comment(procedure_name, original_bteq))
        
        # Add variable declarations
        body_lines.extend(self._generate_declarations(parser_result, parameters))
        
        # Add error handling setup
        body_lines.extend(self._generate_error_handling_setup())
        
        # Convert control flow and SQL blocks
        procedure_logic = self._convert_bteq_logic(parser_result, warnings, error_handling)
        body_lines.extend(procedure_logic)
        
        # Add procedure wrapper
        full_procedure = self._wrap_in_procedure(
            procedure_name, 
            parameters, 
            body_lines
        )
        
        return GeneratedProcedure(
            name=procedure_name,
            sql=full_procedure,
            parameters=parameters,
            error_handling=error_handling,
            warnings=warnings
        )
    
    def _extract_parameters(self, controls: List[ControlStatement], warnings: List[str]) -> List[str]:
        """Extract procedure parameters from BTEQ control statements."""
        parameters = []
        
        for control in controls:
            if control.type == ControlType.LOGON:
                # LOGON typically contains connection info - convert to parameter
                parameters.append("CONNECTION_STRING STRING DEFAULT 'SNOWFLAKE_DEFAULT'")
                warnings.append(f"Line {control.line_no}: LOGON converted to CONNECTION_STRING parameter")
            elif control.type == ControlType.IMPORT:
                # IMPORT statements often reference files - convert to parameter
                if "IMPORT" in control.raw.upper():
                    parameters.append("INPUT_PATH STRING DEFAULT '/tmp/input'")
                    warnings.append(f"Line {control.line_no}: IMPORT converted to INPUT_PATH parameter")
            elif control.type == ControlType.EXPORT:
                # EXPORT statements for output files
                parameters.append("OUTPUT_PATH STRING DEFAULT '/tmp/output'")
                warnings.append(f"Line {control.line_no}: EXPORT converted to OUTPUT_PATH parameter")
        
        # Always add standard error handling parameters
        parameters.extend([
            "ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG'",
            "PROCESS_KEY STRING DEFAULT 'UNKNOWN_PROCESS'"
        ])
        
        return parameters
    
    def _generate_header_comment(self, procedure_name: str, original_bteq: str) -> List[str]:
        """Generate header comment with conversion metadata."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Extract first few lines of original for context
        original_lines = original_bteq.split('\n')[:10]
        original_preview = '\n'.join(f"-- {line}" for line in original_lines)
        
        return [
            "-- =============================================================================",
            f"-- Procedure: {procedure_name}",
            f"-- Generated: {timestamp}",
            "-- Source: Converted from Teradata BTEQ script",
            "-- Generator: SnowflakeSPGenerator v1.0",
            "-- =============================================================================",
            "-- Original BTEQ Preview:",
            original_preview,
            "-- =============================================================================",
            ""
        ]
    
    def _generate_declarations(self, parser_result: ParserResult, parameters: List[str]) -> List[str]:
        """Generate variable declarations for the procedure."""
        declarations = [
            "  -- Variable declarations",
            "  LET error_code INTEGER DEFAULT 0;",
            "  LET sql_state STRING DEFAULT '00000';",
            "  LET error_message STRING DEFAULT '';",
            "  LET row_count INTEGER DEFAULT 0;",
            "  LET current_step STRING DEFAULT 'INIT';",
            ""
        ]
        
        # Add variables for labels found in control statements
        labels = [c for c in parser_result.controls if c.type == ControlType.LABEL]
        if labels:
            declarations.append("  -- Label tracking variables")
            for label in labels:
                label_name = re.search(r'\.LABEL\s+(\w+)', label.raw, re.IGNORECASE)
                if label_name:
                    declarations.append(f"  LET goto_{label_name.group(1).lower()} BOOLEAN DEFAULT FALSE;")
            declarations.append("")
            
        return declarations
    
    def _generate_error_handling_setup(self) -> List[str]:
        """Generate error handling exception handlers."""
        return [
            "  -- Exception handling setup",
            "  DECLARE",
            "    general_exception EXCEPTION (-20001, 'General procedure error');",
            "  BEGIN",
            "    -- Main procedure logic starts here",
            ""
        ]
    
    def _convert_bteq_logic(self, 
                           parser_result: ParserResult, 
                           warnings: List[str], 
                           error_handling: List[str]) -> List[str]:
        """Convert BTEQ control flow and SQL blocks to stored procedure logic."""
        logic_lines = []
        
        # Combine controls and SQL blocks, sort by line number
        all_elements = []
        
        for control in parser_result.controls:
            all_elements.append(('control', control.line_no, control))
            
        for sql_block in parser_result.sql_blocks:
            all_elements.append(('sql', sql_block.start_line, sql_block))
        
        # Sort by line number to maintain original order
        all_elements.sort(key=lambda x: x[1])
        
        # Convert each element
        for element_type, line_no, element in all_elements:
            if element_type == 'control':
                converted = self._convert_control_statement(element, warnings, error_handling)
                logic_lines.extend(converted)
            else:  # sql
                converted = self._convert_sql_block(element, warnings)
                logic_lines.extend(converted)
                
        return logic_lines
    
    def _convert_control_statement(self, 
                                  control: ControlStatement, 
                                  warnings: List[str], 
                                  error_handling: List[str]) -> List[str]:
        """Convert a single control statement to Snowflake stored procedure logic."""
        lines = []
        
        if control.type == ControlType.IF_ERRORCODE:
            # .IF ERRORCODE <> 0 THEN .GOTO ERROR_EXIT
            lines.extend([
                f"    -- Line {control.line_no}: {control.raw}",
                "    IF (error_code <> 0) THEN",
                "      GOTO error_exit;", 
                "    END IF;",
                ""
            ])
            error_handling.append("IF_ERRORCODE handling implemented")
            
        elif control.type == ControlType.GOTO:
            # .GOTO LABEL_NAME
            goto_match = re.search(r'\.GOTO\s+(\w+)', control.raw, re.IGNORECASE)
            if goto_match:
                label_name = goto_match.group(1).lower()
                lines.extend([
                    f"    -- Line {control.line_no}: {control.raw}",
                    f"    GOTO {label_name};",
                    ""
                ])
            else:
                warnings.append(f"Line {control.line_no}: Could not parse GOTO statement")
                lines.extend([f"    -- WARNING: Could not convert: {control.raw}", ""])
                
        elif control.type == ControlType.LABEL:
            # .LABEL LABEL_NAME
            label_match = re.search(r'\.LABEL\s+(\w+)', control.raw, re.IGNORECASE)
            if label_match:
                label_name = label_match.group(1).lower()
                lines.extend([
                    f"    -- Line {control.line_no}: {control.raw}",
                    f"    {label_name}:",
                    ""
                ])
            else:
                warnings.append(f"Line {control.line_no}: Could not parse LABEL statement")
                
        elif control.type == ControlType.RUN:
            lines.extend([
                f"    -- Line {control.line_no}: {control.raw}",
                "    -- RUN statement: Execute accumulated SQL",
                "    -- (SQL execution handled inline in Snowflake)",
                ""
            ])
            
        elif control.type == ControlType.LOGON:
            lines.extend([
                f"    -- Line {control.line_no}: {control.raw}",
                "    -- LOGON: Connection established via procedure parameters",
                "    current_step := 'LOGON_COMPLETED';",
                ""
            ])
            
        elif control.type == ControlType.LOGOFF:
            lines.extend([
                f"    -- Line {control.line_no}: {control.raw}",
                "    -- LOGOFF: Connection cleanup handled by Snowflake",
                "    current_step := 'LOGOFF_COMPLETED';",
                ""
            ])
            
        elif control.type == ControlType.CALL_SP:
            # CALL stored_procedure(params)
            lines.extend([
                f"    -- Line {control.line_no}: {control.raw}",
                f"    CALL {control.raw.strip().replace('CALL ', '').replace('call ', '')};",
                "    -- Check for errors after stored procedure call",
                "    IF (SQLCODE <> 0) THEN",
                "      error_code := SQLCODE;",
                "      error_message := SQLERRM;",
                "      GOTO error_exit;",
                "    END IF;",
                ""
            ])
            error_handling.append("CALL_SP error checking implemented")
            
        elif control.type == ControlType.COLLECT_STATS:
            lines.extend([
                f"    -- Line {control.line_no}: {control.raw}",
                "    -- COLLECT STATS: Not applicable in Snowflake (automatic statistics)",
                ""
            ])
            warnings.append(f"Line {control.line_no}: COLLECT STATS not applicable in Snowflake")
            
        elif control.type == ControlType.OS_CMD:
            lines.extend([
                f"    -- Line {control.line_no}: {control.raw}",
                "    -- OS Command: Not supported in Snowflake stored procedures",
                "    -- Consider using external functions or stages for file operations",
                ""
            ])
            warnings.append(f"Line {control.line_no}: OS commands not supported in Snowflake stored procedures")
            
        elif control.type in [ControlType.IMPORT, ControlType.EXPORT]:
            lines.extend([
                f"    -- Line {control.line_no}: {control.raw}",
                "    -- File I/O: Consider using Snowflake stages and COPY commands",
                ""
            ])
            warnings.append(f"Line {control.line_no}: File I/O requires manual conversion to Snowflake stages")
            
        else:
            lines.extend([
                f"    -- Line {control.line_no}: UNKNOWN CONTROL - {control.raw}",
                f"    -- Manual review required",
                ""
            ])
            warnings.append(f"Line {control.line_no}: Unknown control statement type")
            
        return lines
    
    def _convert_sql_block(self, sql_block: SqlBlock, warnings: List[str]) -> List[str]:
        """Convert SQL block to stored procedure executable SQL."""
        lines = [
            f"    -- SQL Block (lines {sql_block.start_line}-{sql_block.end_line})",
            "    current_step := 'EXECUTING_SQL';",
            "    BEGIN",
        ]
        
        # Add the SQL with proper indentation and error handling
        sql_lines = sql_block.sql.split('\n')
        for sql_line in sql_lines:
            if sql_line.strip():
                lines.append(f"      {sql_line}")
        
        lines.extend([
            "      ;",
            "      -- Get row count and check for errors",
            "      row_count := SQLROWCOUNT;",
            "      IF (SQLCODE <> 0) THEN",
            "        error_code := SQLCODE;",
            "        error_message := SQLERRM;",
            "        GOTO error_exit;",
            "      END IF;",
            "    EXCEPTION",
            "      WHEN OTHER THEN",
            "        error_code := SQLCODE;",
            "        error_message := SQLERRM;", 
            "        GOTO error_exit;",
            "    END;",
            ""
        ])
        
        return lines
    
    def _wrap_in_procedure(self, 
                          procedure_name: str, 
                          parameters: List[str], 
                          body_lines: List[str]) -> str:
        """Wrap the procedure body in CREATE PROCEDURE statement."""
        
        # Format parameters
        param_list = ",\n    ".join(parameters) if parameters else ""
        param_section = f"(\n    {param_list}\n  )" if param_list else "()"
        
        # Build complete procedure
        procedure_parts = [
            f"CREATE OR REPLACE PROCEDURE {procedure_name}",
            param_section,
            "  RETURNS STRING",
            "  LANGUAGE SQL",
            "  EXECUTE AS CALLER",
            "AS",
            "$$",
            "DECLARE"
        ]
        
        # Add body
        procedure_parts.extend(body_lines)
        
        # Add standard error handling and return
        procedure_parts.extend([
            "    -- Success path",
            "    RETURN 'SUCCESS: ' || current_step || ' completed. Rows processed: ' || row_count;",
            "",
            "    -- Error handling",
            "    error_exit:",
            "      -- Log error to error table if available",
            "      INSERT INTO IDENTIFIER(:ERROR_TABLE) (PROCESS_KEY, ERROR_CODE, ERROR_MESSAGE, ERROR_TIMESTAMP)",
            "      VALUES (:PROCESS_KEY, error_code, error_message, CURRENT_TIMESTAMP());",
            "      ",
            "      RETURN 'ERROR: ' || error_message || ' (Code: ' || error_code || ')';",
            "",
            "  EXCEPTION",
            "    WHEN OTHER THEN",
            "      RETURN 'FATAL ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';",
            "END;",
            "$$;"
        ])
        
        return '\n'.join(procedure_parts)
    
    def _generate_variable_from_bteq_var(self, var_line: str) -> str:
        """Convert BTEQ variable syntax to Snowflake variable."""
        # Handle %%VAR%% syntax
        if '%%' in var_line:
            var_match = re.search(r'%%(\w+)%%', var_line)
            if var_match:
                var_name = var_match.group(1)
                return f"  LET {var_name.lower()} STRING DEFAULT '';  -- From BTEQ variable"
        return f"  -- Could not convert variable: {var_line}"


def extract_procedure_name_from_bteq(bteq_content: str, fallback: str = "CONVERTED_BTEQ_PROC") -> str:
    """Extract a meaningful procedure name from BTEQ content."""
    lines = bteq_content.split('\n')
    
    # Look for comment headers that might indicate purpose
    for line in lines[:20]:  # Check first 20 lines
        line = line.strip()
        if line.startswith('--') or line.startswith('/*'):
            # Look for procedure-like patterns
            if any(keyword in line.upper() for keyword in ['PROCEDURE', 'PROC', 'PROCESS', 'SCRIPT']):
                # Extract alphanumeric parts
                words = re.findall(r'\w+', line)
                if len(words) >= 2:
                    return '_'.join(words[1:3]).upper()  # Skip first word (comment indicator)
    
    # Look for table names in SQL statements
    for line in lines:
        if re.match(r'^\s*(INSERT|UPDATE|DELETE|MERGE)\s+', line, re.IGNORECASE):
            table_match = re.search(r'(?:INTO|FROM)\s+(\w+)', line, re.IGNORECASE)
            if table_match:
                return f"PROC_{table_match.group(1).upper()}"
    
    return fallback
