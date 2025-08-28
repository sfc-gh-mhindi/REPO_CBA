#!/usr/bin/env python3
"""
BTEQ to DBT Automation Setup
=============================

Quick setup script to prepare the BTEQ to DBT automation environment.
"""

import subprocess
import sys
from pathlib import Path

def check_python_version():
    """Check if Python version is compatible"""
    min_version = (3, 7)
    current_version = sys.version_info[:2]
    
    if current_version < min_version:
        print(f"❌ Python {min_version[0]}.{min_version[1]}+ required, but {current_version[0]}.{current_version[1]} found")
        return False
    
    print(f"✅ Python {current_version[0]}.{current_version[1]} - Compatible")
    return True

def install_requirements():
    """Install required Python packages"""
    requirements_file = Path(__file__).parent / "requirements.txt"
    
    if not requirements_file.exists():
        print("⚠️  Requirements file not found, creating minimal requirements...")
        with open(requirements_file, 'w') as f:
            f.write("PyYAML>=6.0\n")
    
    print("📦 Installing required packages...")
    try:
        subprocess.run([
            sys.executable, "-m", "pip", "install", "-r", str(requirements_file)
        ], check=True, capture_output=True, text=True)
        print("✅ Requirements installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install requirements: {e}")
        print("Please install manually: pip install PyYAML")
        return False

def check_files():
    """Check if required files exist"""
    required_files = [
        "bteq_to_dbt_converter.py",
        "run_conversion_example.py",
        "AUTOMATION_README.md"
    ]
    
    script_dir = Path(__file__).parent
    missing_files = []
    
    for file in required_files:
        file_path = script_dir / file
        if file_path.exists():
            print(f"✅ {file} - Found")
        else:
            print(f"❌ {file} - Missing")
            missing_files.append(file)
    
    return len(missing_files) == 0

def check_source_directory():
    """Check if source directory exists"""
    source_dir = Path(__file__).parent / "Original Files"
    
    if source_dir.exists():
        file_count = len(list(source_dir.glob("*.sql"))) + len(list(source_dir.glob("*.bteq"))) + len(list(source_dir.glob("*")))
        print(f"✅ Original Files directory found ({file_count} files)")
        return True
    else:
        print("⚠️  'Original Files' directory not found")
        print("   Create this directory and add your BTEQ files, or specify a different source path")
        return False

def show_next_steps():
    """Show next steps for using the automation"""
    print("\n🎉 Setup Complete! Next Steps:")
    print("=" * 50)
    print("1. Quick Test (using Original Files):")
    print("   python run_conversion_example.py")
    print()
    print("2. Custom Conversion:")
    print("   python bteq_to_dbt_converter.py \\")
    print("     --source /path/to/your/bteq/files \\")
    print("     --target /path/to/new/dbt/project \\")
    print("     --project-name \"Your Project Name\"")
    print()
    print("3. View Documentation:")
    print("   open AUTOMATION_README.md")
    print()
    print("4. Get Help:")
    print("   python bteq_to_dbt_converter.py --help")

def main():
    """Main setup function"""
    print("🚀 BTEQ to DBT Automation Setup")
    print("=" * 40)
    print()
    
    # Check Python version
    if not check_python_version():
        return 1
    print()
    
    # Install requirements
    if not install_requirements():
        print("⚠️  Continue anyway, but some features might not work")
    print()
    
    # Check required files
    print("📁 Checking required files...")
    if not check_files():
        print("❌ Some required files are missing")
        return 1
    print()
    
    # Check source directory (optional)
    print("📂 Checking source directory...")
    check_source_directory()
    print()
    
    # Show next steps
    show_next_steps()
    
    return 0

if __name__ == '__main__':
    exit(main()) 