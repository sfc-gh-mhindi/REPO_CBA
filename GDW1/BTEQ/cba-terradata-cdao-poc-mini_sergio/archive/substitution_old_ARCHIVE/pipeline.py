"""
BTEQ Substitution and Migration Pipeline

Orchestrates the complete pipeline from BTEQ variable substitution through 
Snowflake stored procedure generation.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import shutil

# Use absolute imports to fix packaging issues
from substitution.substitution_service import SubstitutionService
from services.parsing.bteq.parser_service import ParserService
from generator.snowflake_sp_generator import SnowflakeSPGenerator
from generator.llm_enhanced_generator import LLMEnhancedGenerator, create_llm_generation_context

logger = logging.getLogger(__name__)


class SubstitutionPipeline:
    """Pipeline for BTEQ variable substitution and Snowflake migration."""
    
    def __init__(self, 
                 config_path: str = None,
                 input_dir: str = None,
                 output_base_dir: str = None,
                 enable_llm: bool = True,
                 llm_models: Optional[List[str]] = None):
        """
        Initialize the substitution pipeline.
        
        Args:
            config_path: Path to configuration file
            input_dir: Input directory containing BTEQ files
            output_base_dir: Base output directory
            enable_llm: Whether to initialize LLM services (False for pure prescriptive mode)
        """
        self.config_path = config_path or "config.cfg"
        self.input_dir = input_dir or "references/current_state/bteq_sql"
        self.output_base_dir = output_base_dir or "output"
        self.enable_llm = enable_llm
        
        # Initialize core services (always needed)
        self.substitution_service = SubstitutionService(self.config_path)
        self.parser_service = ParserService()
        self.generator_service = SnowflakeSPGenerator()
        
        # Configure LLM models to use
        self.llm_models = llm_models or ["claude-4-sonnet", "snowflake-llama-3.3-70b"]  # Default to both
        logger.info(f"🎯 SubstitutionPipeline configured with LLM models: {self.llm_models}")
        
        # Initialize LLM services only if enabled
        self.llm_enhanced_generator = None  # Will be initialized later when run directory is available
        
        # Pipeline state
        self.current_run_dir: Optional[Path] = None
        self.pipeline_results: Dict[str, Any] = {}
    
    def _initialize_llm_generator(self, run_dir: Path):
        """Initialize LLM enhanced generator with dedicated log directory."""
        if self.enable_llm:
            # Create dedicated LLM log directory within the run directory
            # NOTE: Skip creating separate results/llm_interactions to avoid duplication
            # The improved logger will handle creating the unified llm_interactions/
            llm_log_dir = run_dir / "llm_interactions"
            # Don't create here - let improved logger handle unified structure
            # llm_log_dir.mkdir(parents=True, exist_ok=True)
            
            self.llm_enhanced_generator = LLMEnhancedGenerator(
                enable_basic_fallback=True,
                llm_log_dir=str(llm_log_dir)
            )
            logger.info(f"📝 Initialized LLM generator with log directory: {llm_log_dir}")
    
    def create_timestamped_output_dir(self) -> Path:
        """
        Create a clean output directory directly under results.
        
        Returns:
            Path to created directory
        """
        # Create results directory directly without substitution_run prefix
        run_dir = Path(self.output_base_dir)
        
        # Create directory structure
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "substituted").mkdir(exist_ok=True)
        (run_dir / "parsed").mkdir(exist_ok=True) 
        (run_dir / "snowflake_sp").mkdir(exist_ok=True)
        (run_dir / "reports").mkdir(exist_ok=True)
        (run_dir / "analysis").mkdir(exist_ok=True)
        (run_dir / "analysis" / "individual").mkdir(exist_ok=True)
        
        self.current_run_dir = run_dir
        logger.info(f"Created timestamped output directory: {run_dir}")
        
        # Initialize LLM generator with dedicated log directory
        self._initialize_llm_generator(run_dir)
        
        return run_dir
    
    def perform_substitution(self) -> Dict[str, Any]:
        """
        Perform variable substitution on all BTEQ files.
        
        Returns:
            Substitution results
        """
        if not self.current_run_dir:
            raise ValueError("Output directory not created. Call create_timestamped_output_dir() first.")
        
        substituted_dir = self.current_run_dir / "substituted"
        
        logger.info(f"Starting variable substitution from {self.input_dir}")
        
        # Perform substitution
        results = self.substitution_service.substitute_directory(
            input_dir=self.input_dir,
            output_dir=str(substituted_dir),
            file_pattern="*.sql"
        )
        
        # Generate summary
        summary = self.substitution_service.get_substitution_summary(results)
        
        # Save detailed results
        results_file = self.current_run_dir / "reports" / "substitution_results.json"
        with open(results_file, 'w') as f:
            json.dump({
                'summary': summary,
                'detailed_results': results,
                'substitution_mappings': [
                    {'old_value': m.old_value, 'new_value': m.new_value} 
                    for m in self.substitution_service.get_mappings()
                ]
            }, f, indent=2)
        
        logger.info(f"Substitution completed: {summary['successful_files']}/{summary['total_files']} files processed")
        logger.info(f"Total substitutions made: {summary['total_substitutions']}")
        
        return {
            'substituted_dir': str(substituted_dir),
            'summary': summary,
            'results': results
        }
    
    def parse_substituted_files(self, substituted_dir: str) -> Dict[str, Any]:
        """
        Parse substituted SQL files using sqlglot.
        
        Args:
            substituted_dir: Directory containing substituted files
            
        Returns:
            Parsing results
        """
        if not self.current_run_dir:
            raise ValueError("Output directory not created.")
        
        parsed_dir = self.current_run_dir / "parsed"
        
        logger.info("Starting SQL parsing of substituted files")
        
        # Get all substituted SQL files
        substituted_path = Path(substituted_dir)
        sql_files = list(substituted_path.glob("*.sql"))
        
        parsing_results = []
        
        for sql_file in sql_files:
            try:
                # Read and parse the SQL file
                with open(sql_file, 'r') as f:
                    sql_content = f.read()
                
                parse_result = self.parser_service.parse(sql_content)
                
                # Convert parse_result to dict for JSON serialization
                # parse_result has .sql (list of ParsedSql objects) and .controls
                parse_dict = {
                    'sql_blocks': [{'sql': block.original, 'start_line': block.start_line, 'end_line': block.end_line,
                                   'snowflake_sql': block.snowflake_sql, 'error': block.error,
                                   'metadata': {k: list(v) for k, v in block.metadata.items()} if block.metadata else {},
                                   'complexity_metrics': block.complexity_metrics or {},
                                   'teradata_features': block.teradata_features or [],
                                   'syntax_validation': block.syntax_validation or {},
                                   'optimized_sql': block.optimized_sql} 
                                  for block in getattr(parse_result, 'sql', [])],
                    'controls': [{'type': ctrl.type.name if hasattr(ctrl.type, 'name') else str(ctrl.type), 
                                 'raw': ctrl.raw, 'args': ctrl.args, 'line_no': ctrl.line_no} 
                                for ctrl in getattr(parse_result, 'controls', [])],
                    'errors': getattr(parse_result, 'errors', [])
                }
                
                # Generate detailed analysis markdown for LLM consumption
                analysis_result = self._generate_file_analysis(sql_file.name, parse_result)
                analysis_md = self._generate_analysis_markdown(analysis_result)
                
                # Save analysis markdown
                analysis_file = self.current_run_dir / "analysis" / "individual" / f"{sql_file.stem}_analysis.md"
                with open(analysis_file, 'w') as f:
                    f.write(analysis_md)
                
                # Save parsed output
                output_file = parsed_dir / f"{sql_file.stem}_parsed.json"
                with open(output_file, 'w') as f:
                    json.dump(parse_dict, f, indent=2)
                
                parsing_results.append({
                    'file': sql_file.name,
                    'success': True,
                    'parsed_file': str(output_file),
                    'sql_blocks_count': len(parse_dict.get('sql_blocks', [])),
                    'controls_count': len(parse_dict.get('controls', [])),
                    'errors': parse_dict.get('errors', [])
                })
                
            except Exception as e:
                logger.error(f"Error parsing {sql_file.name}: {e}")
                parsing_results.append({
                    'file': sql_file.name,
                    'success': False,
                    'error': str(e)
                })
        
        # Save parsing summary
        parsing_summary = {
            'total_files': len(sql_files),
            'successfully_parsed': len([r for r in parsing_results if r.get('success', False)]),
            'failed_files': len([r for r in parsing_results if not r.get('success', False)]),
            'total_sql_blocks': sum(r.get('sql_blocks_count', 0) for r in parsing_results if r.get('success', False)),
            'total_controls': sum(r.get('controls_count', 0) for r in parsing_results if r.get('success', False))
        }
        
        summary_file = self.current_run_dir / "reports" / "parsing_results.json"
        with open(summary_file, 'w') as f:
            json.dump({
                'summary': parsing_summary,
                'detailed_results': parsing_results
            }, f, indent=2)
        
        logger.info(f"Parsing completed: {parsing_summary['successfully_parsed']}/{parsing_summary['total_files']} files parsed")
        
        return {
            'parsed_dir': str(parsed_dir),
            'summary': parsing_summary,
            'results': parsing_results
        }
    
    def generate_snowflake_procedures(self, parsed_dir: str, substituted_dir: str) -> Dict[str, Any]:
        """
        Generate Snowflake stored procedures from parsed SQL.
        
        Args:
            parsed_dir: Directory containing parsed files
            substituted_dir: Directory containing substituted SQL files
            
        Returns:
            Generation results
        """
        if not self.current_run_dir:
            raise ValueError("Output directory not created.")
        
        sp_dir = self.current_run_dir / "snowflake_sp"
        
        logger.info("Starting Snowflake stored procedure generation")
        
        # Get all substituted SQL files
        substituted_path = Path(substituted_dir)
        sql_files = list(substituted_path.glob("*.sql"))
        
        generation_results = []
        
        for sql_file in sql_files:
            try:
                # Read the substituted SQL content
                with open(sql_file, 'r') as f:
                    sql_content = f.read()
                
                # Check if corresponding parsed file exists
                parsed_file = Path(parsed_dir) / f"{sql_file.stem}_parsed.json"
                parsed_data = {}
                if parsed_file.exists():
                    with open(parsed_file, 'r') as f:
                        parsed_data = json.load(f)
                
                # Generate Snowflake stored procedure  
                # Use absolute imports to fix packaging issues
                try:
                    from services.parsing.bteq.tokens import ParserResult, ControlStatement, SqlBlock
                except ImportError:
                    # Fallback for when running as part of package
                    from services.parsing.bteq.tokens import ParserResult, ControlStatement, SqlBlock
                
                # Create proper parser result structure
                controls = []
                sql_blocks = []
                
                # Convert parsed data to proper structures if available
                if parsed_data:
                    # Use absolute imports to fix packaging issues
                    try:
                        from services.parsing.bteq.tokens import ControlType
                    except ImportError:
                        # Fallback for when running as part of package
                        from services.parsing.bteq.tokens import ControlType
                    
                    for ctrl_data in parsed_data.get('controls', []):
                        if isinstance(ctrl_data, dict):
                            try:
                                # Convert type string back to enum
                                ctrl_type = ControlType[ctrl_data.get('type', 'UNKNOWN')]
                                controls.append(ControlStatement(
                                    type=ctrl_type,
                                    raw=ctrl_data.get('raw', ''),
                                    args=ctrl_data.get('args'),
                                    line_no=ctrl_data.get('line_no')
                                ))
                            except (KeyError, ValueError):
                                pass  # Skip invalid control statements
                    
                    for block_data in parsed_data.get('sql_blocks', []):
                        if isinstance(block_data, dict) and 'sql' in block_data:
                            sql_blocks.append(SqlBlock(
                                sql=block_data.get('sql', ''),
                                start_line=block_data.get('start_line', 0),
                                end_line=block_data.get('end_line', 0)
                            ))
                
                # If no parsed data available, fall back to processing the raw SQL content
                if not sql_blocks and sql_content:
                    # Create a basic SQL block from the entire content for generation
                    sql_blocks.append(SqlBlock(
                        sql=sql_content,
                        start_line=1,
                        end_line=len(sql_content.split('\n'))
                    ))
                
                parser_result = ParserResult(
                    controls=controls,
                    sql_blocks=sql_blocks
                )
                
                sp_result = self.generator_service.generate(
                    parser_result=parser_result,
                    procedure_name=sql_file.stem,
                    original_bteq=sql_content
                )
                
                # Save generated stored procedure
                sp_output_file = sp_dir / f"{sql_file.stem}.sql"
                with open(sp_output_file, 'w') as f:
                    f.write(sp_result.sql)
                
                # Save metadata
                metadata_file = sp_dir / f"{sql_file.stem}_metadata.json"
                metadata = {
                    'name': sp_result.name,
                    'parameters': sp_result.parameters,
                    'error_handling': sp_result.error_handling,
                    'warnings': sp_result.warnings,
                    'original_file': sql_file.name
                }
                with open(metadata_file, 'w') as f:
                    json.dump(metadata, f, indent=2)
                
                generation_results.append({
                    'file': sql_file.name,
                    'success': True,
                    'sp_file': str(sp_output_file),
                    'metadata_file': str(metadata_file),
                    'complexity_score': len(sp_result.warnings),  # Use warnings count as complexity indicator
                    'migration_notes': sp_result.warnings + sp_result.error_handling
                })
                
            except Exception as e:
                logger.error(f"Error generating SP for {sql_file.name}: {e}")
                generation_results.append({
                    'file': sql_file.name,
                    'success': False,
                    'error': str(e)
                })
        
        # Save generation summary
        generation_summary = {
            'total_files': len(sql_files),
            'successfully_generated': len([r for r in generation_results if r.get('success', False)]),
            'failed_files': len([r for r in generation_results if not r.get('success', False)]),
            'average_complexity': sum(r.get('complexity_score', 0) for r in generation_results if r.get('success', False)) / max(1, len([r for r in generation_results if r.get('success', False)]))
        }
        
        summary_file = self.current_run_dir / "reports" / "generation_results.json"
        with open(summary_file, 'w') as f:
            json.dump({
                'summary': generation_summary,
                'detailed_results': generation_results
            }, f, indent=2)
        
        logger.info(f"Generation completed: {generation_summary['successfully_generated']}/{generation_summary['total_files']} procedures generated")
        
        return {
            'sp_dir': str(sp_dir),
            'summary': generation_summary,
            'results': generation_results
        }
    
    def run_complete_pipeline(self) -> Dict[str, Any]:
        """
        Run the complete substitution and migration pipeline.
        
        Returns:
            Complete pipeline results
        """
        logger.info("Starting complete BTEQ substitution and migration pipeline")
        
        try:
            # Step 1: Create timestamped output directory
            output_dir = self.create_timestamped_output_dir()
            
            # Step 2: Perform variable substitution
            substitution_results = self.perform_substitution()
            
            # Step 3: Parse substituted files
            parsing_results = self.parse_substituted_files(
                substitution_results['substituted_dir']
            )
            
            # Step 4: Generate enhanced Snowflake stored procedures
            generation_results = self.generate_enhanced_snowflake_procedures(
                parsing_results['parsed_dir'],
                substitution_results['substituted_dir']
            )
            
            # Compile overall results
            pipeline_results = {
                'pipeline_success': True,
                'output_directory': str(output_dir),
                'timestamp': datetime.now().isoformat(),
                'substitution': substitution_results,
                'parsing': parsing_results,
                'generation': generation_results,
                'summary': {
                    'total_files_processed': substitution_results['summary']['total_files'],
                    'successful_substitutions': substitution_results['summary']['successful_files'],
                    'successful_parsing': parsing_results['summary']['successfully_parsed'],
                    'successful_generation': generation_results['summary']['successfully_generated'],
                    'total_substitutions_made': substitution_results['summary']['total_substitutions']
                }
            }
            
            # Save complete pipeline results
            pipeline_summary_file = output_dir / "pipeline_summary.json"
            with open(pipeline_summary_file, 'w') as f:
                json.dump(pipeline_results, f, indent=2)
            
            logger.info("Pipeline completed successfully")
            logger.info(f"Results saved to: {output_dir}")
            
            self.pipeline_results = pipeline_results
            return pipeline_results
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            error_results = {
                'pipeline_success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            
            if self.current_run_dir:
                error_file = self.current_run_dir / "pipeline_error.json"
                with open(error_file, 'w') as f:
                    json.dump(error_results, f, indent=2)
            
            raise
    
    def _generate_file_analysis(self, file_name: str, parse_result) -> Dict[str, Any]:
        """Generate analysis data structure for a single file."""
        try:
            return {
                "file_name": file_name,
                "success": True,
                "controls": [
                    {
                        "type": c.type.name,
                        "line": c.line_no,
                        "raw": c.raw,
                    }
                    for c in getattr(parse_result, 'controls', [])
                ],
                "sql_blocks": [
                    {
                        "start_line": s.start_line,
                        "end_line": s.end_line,
                        "error": s.error,
                        "has_sql": s.snowflake_sql is not None,
                        "original_sql": s.original,
                        "snowflake_sql": s.snowflake_sql,
                        "metadata": {k: list(v) for k, v in s.metadata.items()} if s.metadata else {},
                        "complexity_metrics": s.complexity_metrics or {},
                        "teradata_features": s.teradata_features or [],
                        "syntax_validation": s.syntax_validation or {},
                        "syntax_valid": s.syntax_validation.get("valid", False) if s.syntax_validation else False,
                        "optimized_sql": s.optimized_sql,
                    }
                    for s in getattr(parse_result, 'sql', [])
                ]
            }
        except Exception as e:
            return {
                "file_name": file_name,
                "success": False,
                "error": str(e),
                "controls": [],
                "sql_blocks": []
            }
    
    def _generate_analysis_markdown(self, result: Dict[str, Any]) -> str:
        """Generate individual file analysis markdown for LLM consumption."""
        file_name = result["file_name"]
        
        md = f"""# {file_name} - BTEQ Analysis

## File Overview
- **File Name**: {file_name}
- **Analysis Status**: {'✅ Success' if result['success'] else '❌ Failed'}
- **Control Statements**: {len(result.get('controls', []))}
- **SQL Blocks**: {len(result.get('sql_blocks', []))}

"""
        
        if not result["success"]:
            md += f"""## Error Details
❌ **Processing Failed**: {result.get('error', 'Unknown error')}

This file requires manual review and potentially different parsing approach.
"""
            return md
        
        # Control flow analysis
        if result["controls"]:
            md += """## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
"""
            for control in result["controls"]:
                truncated = control['raw'][:80] + ('...' if len(control['raw']) > 80 else '')
                md += f"| {control['line']} | {control['type']} | `{truncated}` |\n"
        
        # SQL blocks analysis
        if result["sql_blocks"]:
            md += """
## SQL Blocks Analysis

"""
            for i, sql_block in enumerate(result["sql_blocks"], 1):
                complexity_score = sql_block.get("complexity_metrics", {}).get("total_nodes", 0)
                md += f"""### SQL Block {i} (Lines {sql_block['start_line']}-{sql_block['end_line']})
- **Complexity Score**: {complexity_score}
- **Has Valid SQL**: {'✅' if sql_block['has_sql'] else '❌'}
- **Conversion Successful**: {'✅' if not sql_block['error'] else '❌'}
- **Syntax Validation**: {'✅ Valid' if sql_block['syntax_valid'] else '❌ Invalid'}
- **Teradata Features**: {len(sql_block.get('teradata_features', []))}

"""
                
                # Original SQL
                if sql_block['original_sql']:
                    md += f"""#### 📝 Original Teradata SQL:
```sql
{sql_block['original_sql']}
```

"""
                
                # Converted SQL
                if sql_block['snowflake_sql']:
                    md += f"""#### ❄️ Converted Snowflake SQL:
```sql
{sql_block['snowflake_sql']}
```

"""
                
                # Syntax validation
                md += f"""#### 🔍 Syntax Validation Details:
- **Valid**: {'✅' if sql_block['syntax_valid'] else '❌'}

"""
                
                # Teradata features
                if sql_block.get('teradata_features'):
                    md += "#### 🎯 Teradata Features Detected:\n"
                    for feature in sql_block['teradata_features']:
                        md += f"- {feature}\n"
                    md += "\n"
                
                # Optimized SQL
                if sql_block.get('optimized_sql'):
                    md += f"""#### ⚡ Optimized SQL:
```sql
{sql_block['optimized_sql']}
```

"""
                
                # Metadata
                if sql_block.get('metadata'):
                    md += "#### 📊 SQL Metadata:\n"
                    metadata = sql_block['metadata']
                    if metadata.get('tables'):
                        md += f"- **Tables**: {', '.join(metadata['tables'])}\n"
                    if metadata.get('columns'):
                        md += f"- **Columns**: {', '.join(metadata['columns'])}\n"
                    if metadata.get('functions'):
                        md += f"- **Functions**: {', '.join(metadata['functions'])}\n"
                    md += "\n"
        
        # Migration recommendations
        control_count = len(result.get("controls", []))
        sql_count = len(result.get("sql_blocks", []))
        
        # Calculate total complexity
        total_complexity = sum(
            sql_block.get("complexity_metrics", {}).get("total_nodes", 0)
            for sql_block in result["sql_blocks"]
        )
        
        # Determine migration strategy
        if sql_count == 0:
            strategy = "dbt Macro"
        elif total_complexity < 50:
            strategy = "Simple Model"
        elif total_complexity < 200:
            strategy = "Incremental Model"
        else:
            strategy = "Complex Model"
        
        md += f"""## Migration Recommendations

### Suggested Migration Strategy
**{strategy}** - Based on complexity score ({total_complexity}) and SQL patterns

### DCF Mapping
- **Error Handling**: Convert `.IF ERRORCODE` patterns to DCF error handling macros
- **File Operations**: Replace IMPORT/EXPORT with dbt seeds or external tables
- **Statistics**: Replace COLLECT STATS with Snowflake post-hooks
- **Stored Procedures**: Map CALL statements to DCF stored procedure macros

---

*Generated by BTEQ Parser Service - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
        
        return md
    
    def generate_enhanced_snowflake_procedures(self, parsed_dir, substituted_dir) -> Dict[str, Any]:
        """Generate Snowflake stored procedures using LLM context (if enabled) or basic rules."""
        
        if self.enable_llm:
            logger.info("Starting enhanced Snowflake stored procedure generation")
        else:
            logger.info("Starting basic rule-based Snowflake stored procedure generation")
        
        # Ensure paths are Path objects
        parsed_dir = Path(parsed_dir)
        substituted_dir = Path(substituted_dir)
        
        # Create output directory
        sp_dir = self.current_run_dir / "snowflake_sp"
        sp_dir.mkdir(exist_ok=True)
        
        generation_results = {
            "generated_procedures": [],
            "errors": [],
            "summary": {
                "successfully_generated": 0,
                "failed_generation": 0,
                "llm_enhanced": 0,
                "template_fallback": 0
            }
        }
        
        # Process each parsed file
        for parsed_file in parsed_dir.glob("*_parsed.json"):
            file_stem = parsed_file.stem.replace("_parsed", "")
            procedure_name = file_stem.upper()
            
            try:
                logger.info(f"Generating enhanced procedure for {file_stem}")
                
                # Load parsing results
                with open(parsed_file, 'r') as f:
                    parsed_data = json.load(f)
                
                # Load original BTEQ content
                original_bteq_file = substituted_dir / f"{file_stem}.sql"
                if original_bteq_file.exists():
                    with open(original_bteq_file, 'r') as f:
                        original_bteq = f.read()
                else:
                    original_bteq = "# Original BTEQ content not available"
                
                # Load analysis markdown if available
                analysis_file = self.current_run_dir / "analysis" / "individual" / f"{file_stem}_analysis.md"
                if analysis_file.exists():
                    with open(analysis_file, 'r') as f:
                        analysis_markdown = f.read()
                else:
                    analysis_markdown = "# Analysis not available"
                
                parser_result = self._recreate_parser_result(parsed_data)
                
                # STEP 1: Always generate basic prescriptive procedure first
                logger.info(f"🔧 Generating prescriptive (rule-based) procedure for {procedure_name}")
                basic_result = self.generator_service.generate(parser_result, procedure_name, original_bteq)
                basic_sql = basic_result.sql.replace("{procedure_name}", procedure_name)
                
                # Save prescriptive version
                prescriptive_file = sp_dir / f"{file_stem}_prescriptive.sql"
                with open(prescriptive_file, 'w') as f:
                    f.write(basic_sql)
                logger.info(f"✅ Saved prescriptive procedure: {prescriptive_file.name}")
                
                # Track the preferred procedure and versions generated
                preferred_procedure = "prescriptive"
                preferred_file = prescriptive_file
                preferred_result = basic_result
                versions_generated = ["prescriptive"]
                
                # STEP 2: Generate LLM enhanced versions if enabled (multiple models)
                if self.enable_llm and self.llm_enhanced_generator:
                    logger.info(f"🤖 Generating multi-model LLM-enhanced procedures for {procedure_name}")
                    
                    # Create LLM generation context using basic procedure
                    llm_context = create_llm_generation_context(
                        original_bteq=original_bteq,
                        analysis_markdown=analysis_markdown,
                        basic_procedure=basic_sql,
                        procedure_name=procedure_name,
                        parser_result=parser_result
                    )
                    
                    try:
                        # Generate enhanced procedures with configured models
                        logger.info(f"🤖 Using configured LLM models: {self.llm_models}")
                        multi_results = self.llm_enhanced_generator.generate_enhanced_multi_model(llm_context, self.llm_models)
                        
                        # Save each LLM version and track quality scores
                        llm_results = {}
                        best_quality = 0
                        best_model = None
                        best_result = None
                        
                        for model, result in multi_results.items():
                            if result is not None:
                                # Save model-specific version
                                model_safe_name = model.replace("-", "_").replace(".", "_")
                                enhanced_sql = result.sql.replace("{procedure_name}", procedure_name)
                                enhanced_file = sp_dir / f"{file_stem}_llm_{model_safe_name}.sql"
                                
                                with open(enhanced_file, 'w') as f:
                                    f.write(enhanced_sql)
                                logger.info(f"✅ Saved {model} procedure: {enhanced_file.name} (quality: {result.quality_score:.2f})")
                                
                                # Track this model version
                                versions_generated.append(f"llm_{model_safe_name}")
                                llm_results[model] = {
                                    'result': result,
                                    'file': enhanced_file,
                                    'quality': result.quality_score
                                }
                                
                                # Track best model
                                if result.quality_score > best_quality:
                                    best_quality = result.quality_score
                                    best_model = model
                                    best_result = result
                                
                                generation_results['summary']['llm_enhanced'] += 1
                        
                        # Determine preferred procedure based on quality
                        if best_result and best_quality > 0.7:
                            preferred_procedure = f"llm_{best_model.replace('-', '_').replace('.', '_')}"
                            preferred_file = llm_results[best_model]['file']
                            preferred_result = best_result
                            logger.info(f"🎯 PREFERRED PROCEDURE: {best_model} (quality: {best_quality:.2f})")
                        else:
                            # If all LLM results are low quality, prefer prescriptive
                            if best_result:
                                logger.info(f"⚠️  Best LLM quality below threshold ({best_quality:.2f}), preferring prescriptive")
                            else:
                                logger.warning("❌ All LLM models failed, preferring prescriptive")
                            
                    except Exception as e:
                        logger.error(f"❌ Multi-model LLM enhancement failed for {procedure_name}: {e}")
                        logger.info(f"🎯 PREFERRED PROCEDURE: Prescriptive (All LLM failed)")
                        generation_results['summary']['template_fallback'] += 1
                
                else:
                    logger.info(f"🎯 PREFERRED PROCEDURE: Prescriptive (LLM disabled)")
                    generation_results['summary']['template_fallback'] += 1
                
                # STEP 3: Create final combined/preferred version for convenience
                final_sql = preferred_result.sql
                final_file = sp_dir / f"{file_stem}.sql"
                with open(final_file, 'w') as f:
                    f.write(final_sql)
                logger.info(f"📄 Saved preferred procedure as: {final_file.name} ({preferred_procedure} version)")
                
                # Use preferred result for metadata
                enhanced_result = preferred_result
                
                # Save comprehensive metadata
                metadata = {
                    "procedure_name": enhanced_result.name,
                    "parameters": enhanced_result.parameters,
                    "warnings": enhanced_result.warnings,
                    "llm_enhancements": getattr(enhanced_result, 'llm_enhancements', []),
                    "quality_score": getattr(enhanced_result, 'quality_score', 0.0),
                    "migration_notes": getattr(enhanced_result, 'migration_notes', []),
                    "generation_timestamp": datetime.now().isoformat(),
                    "versions_generated": versions_generated,
                    "preferred_version": preferred_procedure,
                    "preferred_file": str(preferred_file.name),
                    "available_files": self._build_available_files_dict(file_stem, versions_generated)
                }
                
                metadata_file = sp_dir / f"{file_stem}_metadata.json"
                with open(metadata_file, 'w') as f:
                    json.dump(metadata, f, indent=2)
                
                generation_results["generated_procedures"].append({
                    "file": file_stem,
                    "procedure_name": enhanced_result.name,
                    "procedure_file": str(final_file),
                    "metadata_file": str(metadata_file),
                    "quality_score": getattr(enhanced_result, 'quality_score', 0.0),
                    "llm_enhanced": len(getattr(enhanced_result, 'llm_enhancements', [])) > 0,
                    "versions_generated": versions_generated,
                    "preferred_version": preferred_procedure
                })
                
                generation_results["summary"]["successfully_generated"] += 1
                # Note: llm_enhanced counter already incremented above at line 744, don't double-count
                
            except Exception as e:
                logger.error(f"Error generating enhanced procedure for {file_stem}: {e}")
                generation_results["errors"].append({
                    "file": file_stem,
                    "error": str(e)
                })
                generation_results["summary"]["failed_generation"] += 1
        
        logger.info(f"Enhanced generation completed: {generation_results['summary']['successfully_generated']} procedures generated")
        logger.info(f"LLM enhanced: {generation_results['summary']['llm_enhanced']}, Template fallback: {generation_results['summary']['template_fallback']}")
        
        return generation_results
    
    def _generate_basic_procedure_for_context(self, parsed_data: Dict, procedure_name: str, original_bteq: str) -> str:
        """Generate basic procedure for LLM context comparison."""
        
        try:
            # Recreate parser result
            parser_result = self._recreate_parser_result(parsed_data)
            
            # Generate basic procedure
            basic_result = self.generator_service.generate(
                parser_result, procedure_name, original_bteq
            )
            
            return basic_result.sql
            
        except Exception as e:
            logger.warning(f"Failed to generate basic procedure for context: {e}")
            return f"-- Basic procedure generation failed: {e}"
    
    def _recreate_parser_result(self, parsed_data: Dict):
        """Recreate ParserResult from JSON data."""
        
        # Use absolute imports to fix packaging issues
        try:
            from services.parsing.bteq.tokens import ParserResult, ControlStatement, SqlBlock, ControlType
        except ImportError:
            # Fallback for when running as part of package
            from services.parsing.bteq.tokens import ParserResult, ControlStatement, SqlBlock, ControlType
        
        controls = []
        sql_blocks = []
        
        # Recreate controls
        for ctrl_data in parsed_data.get('controls', []):
            if isinstance(ctrl_data, dict):
                try:
                    ctrl_type = ControlType[ctrl_data.get('type', 'UNKNOWN')]
                    controls.append(ControlStatement(
                        type=ctrl_type,
                        raw=ctrl_data.get('raw', ''),
                        args=ctrl_data.get('args'),
                        line_no=ctrl_data.get('line_no')
                    ))
                except (KeyError, ValueError):
                    pass
        
        # Recreate SQL blocks
        for block_data in parsed_data.get('sql_blocks', []):
            if isinstance(block_data, dict) and 'sql' in block_data:
                sql_blocks.append(SqlBlock(
                    sql=block_data.get('sql', ''),
                    start_line=block_data.get('start_line', 0),
                    end_line=block_data.get('end_line', 0)
                ))
        
        return ParserResult(
            controls=controls,
            sql_blocks=sql_blocks
        )
    
    def _build_available_files_dict(self, file_stem: str, versions_generated: List[str]) -> Dict[str, str]:
        """Build dictionary of available files for metadata."""
        available_files = {"prescriptive": f"{file_stem}_prescriptive.sql"}
        
        for version in versions_generated:
            if version.startswith("llm_"):
                available_files[version] = f"{file_stem}_{version}.sql"
            elif version == "llm_enhanced":  # Legacy single LLM version
                available_files["llm_enhanced"] = f"{file_stem}_llm_enhanced.sql"
        
        available_files["preferred"] = f"{file_stem}.sql"
        return available_files
    
    def get_pipeline_summary(self) -> str:
        """
        Get a human-readable summary of the pipeline results.
        
        Returns:
            Formatted summary string
        """
        if not self.pipeline_results:
            return "No pipeline results available. Run the pipeline first."
        
        results = self.pipeline_results
        summary = results.get('summary', {})
        
        return f"""
=== BTEQ Substitution & Migration Pipeline Summary ===

Pipeline Status: {'✅ SUCCESS' if results.get('pipeline_success') else '❌ FAILED'}
Execution Time: {results.get('timestamp')}
Output Directory: {results.get('output_directory')}

📊 Processing Summary:
- Total Files: {summary.get('total_files_processed', 0)}
- Variable Substitutions: {summary.get('total_substitutions_made', 0)}
- Successfully Substituted: {summary.get('successful_substitutions', 0)}
- Successfully Parsed: {summary.get('successful_parsing', 0)}
- Snowflake SPs Generated: {summary.get('successful_generation', 0)}

🔄 Stage Success Rates:
- Substitution: {summary.get('successful_substitutions', 0)}/{summary.get('total_files_processed', 0)}
- Parsing: {summary.get('successful_parsing', 0)}/{summary.get('total_files_processed', 0)}
- Generation: {summary.get('successful_generation', 0)}/{summary.get('total_files_processed', 0)}
"""
