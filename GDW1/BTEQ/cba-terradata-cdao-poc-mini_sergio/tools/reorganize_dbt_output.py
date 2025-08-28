#!/usr/bin/env python3
"""
Reorganize DBT Consolidated Output

Creates a timestamped, well-organized directory structure for DBT conversion results.
"""

import os
import shutil
from pathlib import Path
from datetime import datetime
import json

def reorganize_dbt_output():
    """Reorganize the consolidated DBT output into a timestamped, structured directory."""
    
    # Setup paths
    base_dir = Path(__file__).parent
    source_dir = base_dir / "output" / "consol_dbt"
    
    # Create timestamped directory name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_dir = base_dir / "output" / f"consol_dbt_{timestamp}"
    
    print(f"🏗️  Creating organized DBT structure: {dest_dir.name}")
    
    # Create organized subdirectories
    subdirs = {
        "dbt_models": dest_dir / "dbt_models",
        "references": dest_dir / "references", 
        "metadata": dest_dir / "metadata",
        "summaries": dest_dir / "summaries"
    }
    
    for subdir in subdirs.values():
        subdir.mkdir(parents=True, exist_ok=True)
    
    # Track what gets moved
    file_counts = {key: 0 for key in subdirs.keys()}
    
    if not source_dir.exists():
        print(f"❌ Source directory not found: {source_dir}")
        return
        
    # Process each file in source directory
    for file_path in source_dir.iterdir():
        if file_path.is_file():
            filename = file_path.name
            
            # Determine destination based on filename pattern
            if filename == "batch_conversion_summary.json":
                # Keep batch summary at root level
                dest_path = dest_dir / filename
                shutil.copy2(file_path, dest_path)
                print(f"📋 Batch Summary: {filename}")
                
            elif filename.endswith("_reference.sql"):
                # Reference stored procedures
                dest_path = subdirs["references"] / filename
                shutil.copy2(file_path, dest_path)
                file_counts["references"] += 1
                print(f"🔗 Reference: {filename}")
                
            elif filename.endswith("_metadata.json"):
                # Metadata files
                dest_path = subdirs["metadata"] / filename
                shutil.copy2(file_path, dest_path)
                file_counts["metadata"] += 1
                print(f"📊 Metadata: {filename}")
                
            elif filename.endswith("_migration_summary.json"):
                # Individual migration summaries
                dest_path = subdirs["summaries"] / filename
                shutil.copy2(file_path, dest_path)
                file_counts["summaries"] += 1
                print(f"📄 Summary: {filename}")
                
            elif filename.endswith(".sql"):
                # Main DBT model files
                dest_path = subdirs["dbt_models"] / filename
                shutil.copy2(file_path, dest_path)
                file_counts["dbt_models"] += 1
                print(f"🎯 DBT Model: {filename}")
                
            else:
                # Any other files - copy to root
                dest_path = dest_dir / filename
                shutil.copy2(file_path, dest_path)
                print(f"📁 Other: {filename}")
    
    # Create a README file explaining the structure
    readme_content = f"""# DBT Conversion Results - {timestamp}

## 📁 Directory Structure

```
consol_dbt_{timestamp}/
├── README.md                          # This file
├── batch_conversion_summary.json      # Overall batch processing summary
├── dbt_models/                        # DBT model files (.sql)
├── references/                        # Reference stored procedures (.sql)  
├── metadata/                          # Conversion metadata (.json)
└── summaries/                         # Individual migration summaries (.json)
```

## 📊 File Counts

- **DBT Models**: {file_counts['dbt_models']} files
- **References**: {file_counts['references']} files  
- **Metadata**: {file_counts['metadata']} files
- **Summaries**: {file_counts['summaries']} files

## 🎯 DBT Models

The `dbt_models/` directory contains the converted DBT models ready for use in a DBT project:

```yaml
# Example dbt_project.yml structure
model-paths: ["models"]

models:
  your_project:
    acct_baln:
      materialized: table
      tags: ["account_balance", "backdated"]
```

## 🔗 References

The `references/` directory contains the generated Snowflake stored procedures that were used as reference during DBT conversion.

## 📊 Metadata & Summaries

- `metadata/`: Detailed conversion metadata including LLM model comparisons, quality scores, etc.
- `summaries/`: Individual file processing summaries with timing and status information.

## 📋 Batch Summary

See `batch_conversion_summary.json` for complete batch processing statistics.

---
Generated on: {datetime.now().isoformat()}
Total files processed: {sum(file_counts.values())}
"""
    
    readme_path = dest_dir / "README.md"
    readme_path.write_text(readme_content)
    
    # Create an index file listing all DBT models
    dbt_models_dir = subdirs["dbt_models"]
    if dbt_models_dir.exists():
        dbt_files = list(dbt_models_dir.glob("*.sql"))
        dbt_files.sort()
        
        index_content = {
            "timestamp": datetime.now().isoformat(),
            "total_dbt_models": len(dbt_files),
            "dbt_models": [
                {
                    "filename": f.name,
                    "model_name": f.stem,
                    "file_size_bytes": f.stat().st_size,
                    "last_modified": datetime.fromtimestamp(f.stat().st_mtime).isoformat()
                }
                for f in dbt_files
            ]
        }
        
        index_path = dest_dir / "dbt_models_index.json"
        with open(index_path, 'w') as f:
            json.dump(index_content, f, indent=2)
    
    print(f"\n🎉 Reorganization Complete!")
    print(f"📁 New structure: {dest_dir}")
    print(f"📊 File counts:")
    for category, count in file_counts.items():
        print(f"   • {category.replace('_', ' ').title()}: {count}")
    
    print(f"\n📋 Additional files created:")
    print(f"   • README.md")
    print(f"   • dbt_models_index.json")
    
    # Optionally remove old directory (commented out for safety)
    # print(f"\n🗑️  To remove old directory: rm -rf {source_dir}")
    
    return dest_dir

if __name__ == "__main__":
    reorganize_dbt_output()
