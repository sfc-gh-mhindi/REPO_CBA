#!/usr/bin/env python3
"""
Batch BTEQ to DBT Conversion Script

Processes all ACCT_BALN* files through v3_dbt_conversion and consolidates results.
"""

import os
import sys
import subprocess
import glob
from pathlib import Path
import shutil
import json
from datetime import datetime
import time

def run_dbt_conversion(input_file):
    """Run DBT conversion for a single file."""
    print(f"🔄 Processing: {input_file}")
    start_time = time.time()
    
    try:
        result = subprocess.run([
            sys.executable, "main.py",
            "--input", input_file,
            "--mode", "v3_dbt_conversion"
        ], capture_output=True, text=True, timeout=300)  # 5 minute timeout
        
        processing_time = time.time() - start_time
        
        if result.returncode == 0:
            print(f"✅ SUCCESS: {input_file} ({processing_time:.1f}s)")
            return True, processing_time, None
        else:
            print(f"❌ FAILED: {input_file} - {result.stderr}")
            return False, processing_time, result.stderr
            
    except subprocess.TimeoutExpired:
        print(f"⏰ TIMEOUT: {input_file} (5+ minutes)")
        return False, 300, "Timeout after 5 minutes"
    except Exception as e:
        print(f"💥 ERROR: {input_file} - {str(e)}")
        return False, 0, str(e)

def find_latest_dbt_files(output_dir):
    """Find DBT files from the most recent migration run."""
    migration_runs = glob.glob(str(output_dir / "migration_run_*"))
    if not migration_runs:
        return []
    
    # Get the most recent run
    latest_run = max(migration_runs, key=lambda x: os.path.getctime(x))
    dbt_dir = Path(latest_run) / "results" / "dbt"
    
    if dbt_dir.exists():
        dbt_files = list(dbt_dir.glob("*.sql"))
        metadata_files = list(dbt_dir.glob("*.json"))
        return dbt_files + metadata_files
    return []

def consolidate_dbt_files(source_files, consol_dir, file_stem):
    """Copy DBT files to consolidation directory with proper naming."""
    copied_files = []
    
    for file_path in source_files:
        if file_path.name.endswith(('.sql', '.json')):
            # Use original file stem for naming
            if 'metadata' in file_path.name:
                new_name = f"{file_stem}_metadata.json"
            elif 'reference' in file_path.name:
                new_name = f"{file_stem}_reference.sql"  
            elif 'migration_summary' in file_path.name:
                new_name = f"{file_stem}_migration_summary.json"
            else:
                # Main DBT model
                new_name = f"{file_stem}.sql"
            
            dest_path = consol_dir / new_name
            shutil.copy2(file_path, dest_path)
            copied_files.append(dest_path)
            print(f"📄 Copied: {file_path.name} → {new_name}")
    
    return copied_files

def main():
    """Main batch processing function."""
    print("🚀 Starting Batch BTEQ to DBT Conversion")
    print("=" * 60)
    
    # Setup paths
    base_dir = Path(__file__).parent
    bteq_dir = base_dir / "references" / "current_state" / "bteq_sql"
    output_dir = base_dir / "output"
    consol_dir = base_dir / "output" / "consol_dbt"
    
    # Ensure consolidation directory exists
    consol_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all ACCT_BALN* files
    acct_baln_files = list(bteq_dir.glob("ACCT_BALN*.sql"))
    acct_baln_files.sort()
    
    print(f"📊 Found {len(acct_baln_files)} ACCT_BALN* files to process")
    print()
    
    # Process each file
    results = {}
    total_start_time = time.time()
    
    for i, file_path in enumerate(acct_baln_files, 1):
        file_stem = file_path.stem.lower()
        print(f"[{i}/{len(acct_baln_files)}] Processing {file_path.name}")
        
        # Run DBT conversion
        success, proc_time, error = run_dbt_conversion(str(file_path))
        
        if success:
            # Find and consolidate DBT files
            dbt_files = find_latest_dbt_files(output_dir)
            if dbt_files:
                copied_files = consolidate_dbt_files(dbt_files, consol_dir, file_stem)
                results[file_stem] = {
                    "status": "success",
                    "processing_time": proc_time,
                    "dbt_files_generated": len([f for f in copied_files if f.suffix == '.sql']),
                    "total_files_copied": len(copied_files)
                }
                print(f"📁 Consolidated {len(copied_files)} files to consol_dbt/")
            else:
                results[file_stem] = {
                    "status": "partial_success",
                    "processing_time": proc_time,
                    "note": "Conversion completed but no DBT files found"
                }
        else:
            results[file_stem] = {
                "status": "failed",
                "processing_time": proc_time,
                "error": error
            }
        
        print("-" * 60)
    
    # Generate summary report
    total_time = time.time() - total_start_time
    successful = sum(1 for r in results.values() if r["status"] == "success")
    failed = sum(1 for r in results.values() if r["status"] == "failed")
    
    summary = {
        "batch_run_timestamp": datetime.now().isoformat(),
        "total_files_processed": len(acct_baln_files),
        "successful_conversions": successful,
        "failed_conversions": failed,
        "total_processing_time_seconds": total_time,
        "average_time_per_file": total_time / len(acct_baln_files),
        "consolidation_directory": str(consol_dir),
        "detailed_results": results
    }
    
    # Save summary report
    summary_file = consol_dir / "batch_conversion_summary.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print("🎉 BATCH CONVERSION COMPLETED!")
    print("=" * 60)
    print(f"📊 SUMMARY:")
    print(f"   • Total files: {len(acct_baln_files)}")
    print(f"   • Successful: {successful} ✅")
    print(f"   • Failed: {failed} ❌")
    print(f"   • Total time: {total_time:.1f}s")
    print(f"   • Average time: {total_time/len(acct_baln_files):.1f}s per file")
    print(f"📁 Consolidated files saved to: {consol_dir}")
    print(f"📋 Summary report: {summary_file}")
    
    if failed > 0:
        print("\n❌ FAILED FILES:")
        for file_stem, result in results.items():
            if result["status"] == "failed":
                print(f"   • {file_stem}: {result.get('error', 'Unknown error')}")

if __name__ == "__main__":
    main()
