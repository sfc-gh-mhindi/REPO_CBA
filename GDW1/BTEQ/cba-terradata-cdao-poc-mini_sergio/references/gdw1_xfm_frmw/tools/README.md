# GDW1 Migration Tools

This directory contains utilities for analyzing DataStage job definitions, testing dbt implementations, and supporting the GDW1 migration process.

## Tools

### `split_datastage_xml.py`

**Purpose**: Splits the monolithic CCODS.xml DataStage export into individual XML files, one per job.

**Usage**:
```bash
cd tools
python3 split_datastage_xml.py [options]
```

**Options**:
- `--input PATH`: Path to input DataStage XML file (default: `../../current_state/GDW1_Datastage_POC_JobDesignXML/CCODS.xml`)
- `--output PATH`: Output directory for individual job files (default: `../curr_stte/ccods`)

**Example**:
```bash
# Basic usage (uses defaults)
python3 split_datastage_xml.py

# Custom paths
python3 split_datastage_xml.py --input /path/to/datastage.xml --output /path/to/output
```

**Output**:
- Individual XML files for each DataStage job
- `README.md` index file listing all extracted jobs
- Clean, readable XML format for easier analysis

**What it extracts**:
- Job properties and parameters
- Stage definitions and connections  
- Transformation logic
- Error handling configuration
- Job orchestration flow

## Output Structure

After running the script, you'll have:

```
curr_stte/
└── ccods/                                 # CCODS DataStage jobs
    ├── README.md                          # Job index and documentation
    ├── SQ10COMMONPreprocess.xml           # Preprocessing job
    ├── SQ20BCFINSGValidateFiles.xml       # File validation job
    ├── SQ40BCFINSGXfmPlanBalnSegmMstr.xml # Transformation job
    ├── SQ60BCFINSGLdPlnBalSegMstr.xml     # Loading job
    ├── SQ70COMMONLdErr.xml                # Error handling job
    ├── SQ80COMMONAHLPostprocess.xml       # Postprocessing job
    ├── CCODSLdErr.xml                     # Error loading job
    ├── GDWUtilProcessMetaDataFL.xml       # Metadata processing job
    ├── LdBCFINSGPlanBalnSegmMstr.xml      # Plan balance segment loader
    ├── RunStreamStart.xml                 # Stream initialization job
    ├── ValidateBcFinsg.xml                # BCFINSG validation job
    └── XfmPlanBalnSegmMstrFromBCFINSG.xml # BCFINSG transformation job
```

## Benefits

1. **Easier Analysis**: Work with individual jobs instead of a 3.7MB monolithic file
2. **Better Navigation**: Find specific jobs quickly
3. **Improved Tooling**: Use standard XML tools on smaller, focused files
4. **Migration Planning**: Analyze jobs individually for dbt conversion
5. **Documentation**: Clear index of all available jobs

### fix_mermaid_diagrams.py

**Purpose**: Fixes broken Mermaid diagrams in documentation by removing problematic classDef styling that causes rendering issues across different Mermaid renderers.

**Usage**:
```bash
cd gdw1_xfm/gdw1_dbt/tools
python3 fix_mermaid_diagrams.py
```

**What it fixes**:
- Removes `classDef` styling definitions that break in some renderers
- Removes `class` assignments that apply complex styling 
- Preserves core diagram structure and flow logic
- Ensures universal compatibility across Mermaid environments

**Example**:
```bash
$ python3 fix_mermaid_diagrams.py
Found 14 markdown files to check
Processing: SQ20BCFINSGValidateFiles_README.md
  -> Fixing Mermaid diagrams
...
Fixed 11 files with Mermaid diagram issues
All diagrams should now render properly!
```

This tool ensures all BCFINSG documentation diagrams render consistently across different platforms and tools.

## Next Steps

Once you have individual job files, you can:

1. **Analyze Job Dependencies**: Study the orchestration flow between sequence jobs
2. **Extract Business Logic**: Focus on transformation logic in parallel jobs
3. **Map Data Lineage**: Trace data flow through the pipeline
4. **Plan dbt Migration**: Convert DataStage logic to dbt models
5. **Document Processes**: Create detailed documentation for each job

## Requirements

- Python 3.6+
- Access to the CCODS.xml DataStage export file
- Write permissions to the output directory

## Troubleshooting

**Error: Input file not found**
- Verify the path to CCODS.xml is correct
- Check file permissions

**Error: Cannot create output directory**
- Verify write permissions
- Check disk space

**Error: XML Parse Error**
- Verify the input file is valid XML
- Check if the file is complete/not corrupted

## Test Scripts

### `test_validate_header.sql`

**Purpose**: Test script for the generic header validation macro (`validate_header`). Provides comprehensive testing and verification of the header tracker framework.

**Usage**:
```bash
# Execute via snow CLI
snow sql -f tools/test_validate_header.sql --connection pupad_svc
```

**Features**:
- Tests DCF business date table integration
- Validates header tracker status and metadata  
- Provides test setup for business dates
- Analyzes date matching and validation readiness
- Generates test commands for macro execution

**What it checks**:
- DCF business date configuration
- Header tracker table status
- JSON metadata extraction
- Date validation logic
- Processing status lifecycle

