#!/usr/bin/env python3
"""
Example Usage: BTEQ to DBT Converter
====================================

This script demonstrates how to use the BTEQ to DBT converter
with the existing GDW1 BTEQ files.
"""

import subprocess
import sys
from pathlib import Path

def run_conversion():
    """Run the BTEQ to DBT conversion with example parameters"""
    
    # Define paths (adjust as needed)
    script_dir = Path(__file__).parent
    converter_script = script_dir / "bteq_to_dbt_converter.py"
    source_dir = script_dir / "Original Files"
    target_dir = script_dir / "Generated-DBT-Project"
    
    # Conversion parameters
    project_name = "GDW1_BTEQ_Migration"
    snowflake_account = "your_snowflake_account"
    database_prefix = "PSUND_MIGR"
    schema_prefix = "P_D_DCF_001_STD_0"
    
    print("🚀 BTEQ to DBT Converter - Example Usage")
    print("=" * 50)
    print(f"Source Directory: {source_dir}")
    print(f"Target Directory: {target_dir}")
    print(f"Project Name: {project_name}")
    print()
    
    # Verify source directory exists
    if not source_dir.exists():
        print(f"❌ Error: Source directory not found: {source_dir}")
        print("Please ensure the 'Original Files' directory exists.")
        return 1
    
    # Check if converter script exists
    if not converter_script.exists():
        print(f"❌ Error: Converter script not found: {converter_script}")
        print("Please ensure 'bteq_to_dbt_converter.py' is in the same directory.")
        return 1
    
    # Build command
    cmd = [
        sys.executable,
        str(converter_script),
        "--source", str(source_dir),
        "--target", str(target_dir),
        "--project-name", project_name,
        "--snowflake-account", snowflake_account,
        "--database-prefix", database_prefix,
        "--schema-prefix", schema_prefix
    ]
    
    print("Executing conversion command:")
    print(" ".join(cmd))
    print()
    
    try:
        # Run the conversion
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        
        print("✅ Conversion completed successfully!")
        print("\nOutput:")
        print(result.stdout)
        
        # Show next steps
        print("\n🎉 Next Steps:")
        print(f"1. cd '{target_dir}'")
        print("2. Review dbt_project.yml and update variables")
        print("3. Set environment variables for Snowflake connection:")
        print("   export SNOWFLAKE_ACCOUNT=your_account")
        print("   export SNOWFLAKE_USER=your_user")
        print("   export SNOWFLAKE_PASSWORD=your_password")
        print("   export SNOWFLAKE_ROLE=your_role")
        print("   export SNOWFLAKE_WAREHOUSE=your_warehouse")
        print("4. Install DBT dependencies: dbt deps")
        print("5. Test connection: dbt debug")
        print("6. Run models: dbt run")
        
        return 0
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Conversion failed with exit code {e.returncode}")
        if e.stdout:
            print("Standard Output:")
            print(e.stdout)
        if e.stderr:
            print("Standard Error:")
            print(e.stderr)
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1

def main():
    """Main function"""
    print(__doc__)
    
    # Ask for confirmation
    response = input("Do you want to run the BTEQ to DBT conversion? (y/N): ")
    if response.lower() not in ['y', 'yes']:
        print("Conversion cancelled.")
        return 0
    
    return run_conversion()

if __name__ == '__main__':
    exit(main()) 