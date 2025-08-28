from __future__ import annotations

import re
from typing import List, Dict, Set, Optional

import sqlglot
from sqlglot import exp
from sqlglot.optimizer import optimize


def _preprocess_sql(sql_text: str) -> str:
    """Preprocess SQL to handle comments and other issues before parsing."""
    # Remove multi-line comments /* ... */
    sql_text = re.sub(r'/\*.*?\*/', '', sql_text, flags=re.DOTALL)
    
    # Remove single-line comments --
    sql_text = re.sub(r'--.*$', '', sql_text, flags=re.MULTILINE)
    
    # Clean up extra whitespace
    sql_text = re.sub(r'\n\s*\n', '\n', sql_text)
    sql_text = re.sub(r'^\s+', '', sql_text, flags=re.MULTILINE)
    
    return sql_text.strip()


def parse_teradata_sql(sql_text: str) -> exp.Expression:
    """Parse Teradata SQL into a SQLGlot AST.

    Raises sqlglot.errors.ParseError on failure.
    """
    preprocessed = _preprocess_sql(sql_text)
    return sqlglot.parse_one(preprocessed, read="teradata")


def _transform_nvl_to_coalesce(node: exp.Expression) -> exp.Expression:
    """Transform NVL function calls to COALESCE."""
    if isinstance(node, exp.Anonymous) and hasattr(node, 'this') and str(node.this).upper() == "NVL":
        if hasattr(node, 'expressions') and len(node.expressions) >= 2:
            return exp.Coalesce(this=node.expressions[0], expressions=node.expressions[1:])
    return node


def transpile_to_snowflake(sql_text: str) -> str:
    """Transpile Teradata SQL to Snowflake SQL with basic transforms."""
    try:
        tree = parse_teradata_sql(sql_text)
        # Apply basic transforms (can be extended)
        tree = tree.transform(_transform_nvl_to_coalesce)
        return tree.sql(dialect="snowflake")
    except Exception:
        # Fallback: basic dialect conversion without custom transforms
        try:
            preprocessed = _preprocess_sql(sql_text)
            return sqlglot.transpile(preprocessed, read="teradata", write="snowflake")[0]
        except Exception:
            # If all else fails, return original SQL
            return sql_text


def analyze_sql_metadata(sql_text: str) -> Dict[str, Set[str]]:
    """Extract metadata from SQL: tables, columns, functions used."""
    try:
        tree = parse_teradata_sql(sql_text)
        
        # Extract tables
        tables = {table.name for table in tree.find_all(exp.Table) if table.name}
        
        # Extract columns
        columns = {col.name for col in tree.find_all(exp.Column) if col.name}
        
        # Extract functions
        functions = {func.this for func in tree.find_all(exp.Anonymous) if hasattr(func, 'this')}
        functions.update({func.this for func in tree.find_all(exp.Func) if hasattr(func, 'this')})
        
        # Extract window functions
        window_funcs = {wf.this.this if hasattr(wf.this, 'this') else str(wf.this) 
                       for wf in tree.find_all(exp.Window)}
        
        return {
            "tables": tables,
            "columns": columns, 
            "functions": functions,
            "window_functions": window_funcs
        }
    except Exception:
        return {"tables": set(), "columns": set(), "functions": set(), "window_functions": set()}


def optimize_sql(sql_text: str, schema: Optional[Dict] = None) -> str:
    """Optimize SQL query using SQLGlot optimizer."""
    try:
        tree = parse_teradata_sql(sql_text)
        if schema:
            optimized = optimize(tree, schema=schema)
        else:
            optimized = optimize(tree)
        return optimized.sql(dialect="snowflake", pretty=True)
    except Exception:
        return sql_text


def validate_sql_syntax(sql_text: str) -> Dict[str, any]:
    """Validate SQL syntax and return detailed error information."""
    try:
        tree = parse_teradata_sql(sql_text)
        return {
            "valid": True,
            "error": None,
            "ast_type": type(tree).__name__
        }
    except sqlglot.errors.ParseError as e:
        return {
            "valid": False,
            "error": str(e),
            "error_type": "ParseError"
        }
    except Exception as e:
        return {
            "valid": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


def get_query_complexity_metrics(sql_text: str) -> Dict[str, int]:
    """Calculate complexity metrics for SQL query."""
    try:
        tree = parse_teradata_sql(sql_text)
        
        metrics = {
            "total_nodes": len(list(tree.walk())),
            "select_count": len(list(tree.find_all(exp.Select))),
            "join_count": len(list(tree.find_all(exp.Join))),
            "subquery_count": len(list(tree.find_all(exp.Subquery))),
            "where_conditions": len(list(tree.find_all(exp.Where))),
            "case_statements": len(list(tree.find_all(exp.Case))),
            "window_functions": len(list(tree.find_all(exp.Window))),
            "aggregate_functions": len([f for f in tree.find_all(exp.AggFunc)]),
        }
        
        return metrics
    except Exception:
        return {"total_nodes": 0, "select_count": 0, "join_count": 0, 
                "subquery_count": 0, "where_conditions": 0, "case_statements": 0,
                "window_functions": 0, "aggregate_functions": 0}


def extract_teradata_specific_features(sql_text: str) -> List[str]:
    """Identify Teradata-specific SQL features that need special handling."""
    features = []
    
    # Check for common Teradata-specific patterns
    patterns = {
        "QUALIFY clause": r"\bQUALIFY\b",
        "YEAR(4) TO MONTH intervals": r"YEAR\(\d+\)\s+TO\s+MONTH",
        "ADD_MONTHS function": r"\bADD_MONTHS\b",
        "EXTRACT with complex syntax": r"EXTRACT\s*\(\s*\w+\s+FROM",
        "ROW_NUMBER() OVER": r"ROW_NUMBER\(\)\s+OVER",
        "Teradata date arithmetic": r"\w+\s*[-+]\s*INTERVAL",
        "Variable substitution": r"%%\w+%%",
        "Teradata COLLECT STATS": r"\bCOLLECT\s+STATS\b",
    }
    
    for feature_name, pattern in patterns.items():
        if re.search(pattern, sql_text, re.IGNORECASE):
            features.append(feature_name)
    
    return features


def normalize_statements(statements: List[str]) -> List[str]:
    """Format SQL statements for consistent style in diagnostics/testing."""
    out: List[str] = []
    for s in statements:
        try:
            tree = sqlglot.parse_one(s, read="snowflake")
            out.append(tree.sql(dialect="snowflake"))
        except Exception:
            out.append(s.strip())
    return out
