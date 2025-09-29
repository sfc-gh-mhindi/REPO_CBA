#!/usr/bin/env python3
"""
DataStage XML Job Splitter

This script reads the CCODS.xml DataStage export file and splits it into 
individual XML files, one per job, for easier analysis and processing.

Usage:
    python split_datastage_xml.py

Author: Data Architecture Team
Date: 2024
"""

import os
import xml.etree.ElementTree as ET
from pathlib import Path
import argparse
from typing import List, Tuple
import sys


def setup_directories(output_dir: str) -> None:
    """Create necessary output directories"""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    print(f"✓ Output directory created: {output_dir}")


def validate_input_file(input_file: str) -> bool:
    """Validate that input XML file exists and is readable"""
    if not os.path.exists(input_file):
        print(f"❌ Error: Input file not found: {input_file}")
        return False
    
    if not os.access(input_file, os.R_OK):
        print(f"❌ Error: Cannot read input file: {input_file}")
        return False
    
    # Check file size (warn if very large)
    file_size = os.path.getsize(input_file) / (1024 * 1024)  # MB
    print(f"📁 Input file size: {file_size:.1f} MB")
    
    if file_size > 100:
        print("⚠️  Warning: Large file detected. Processing may take time...")
    
    return True


def extract_jobs_from_xml(input_file: str) -> List[Tuple[str, str]]:
    """
    Extract individual job definitions from DataStage XML
    
    Returns:
        List of tuples: (job_name, job_xml_content)
    """
    jobs = []
    
    try:
        # Parse XML file
        print("🔄 Parsing XML file...")
        tree = ET.parse(input_file)
        root = tree.getroot()
        
        # Find all Job elements
        job_elements = root.findall('.//Job')
        print(f"📊 Found {len(job_elements)} jobs in XML")
        
        for job_elem in job_elements:
            # Get job identifier/name
            job_name = job_elem.get('Identifier', 'UnknownJob')
            
            # Convert job element back to XML string
            job_xml = ET.tostring(job_elem, encoding='unicode', method='xml')
            
            # Clean up and format XML
            job_xml = f'<?xml version="1.0" encoding="UTF-8"?>\n{job_xml}'
            
            jobs.append((job_name, job_xml))
            print(f"  ✓ Extracted job: {job_name}")
    
    except ET.ParseError as e:
        print(f"❌ XML Parse Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error extracting jobs: {e}")
        sys.exit(1)
    
    return jobs


def save_job_files(jobs: List[Tuple[str, str]], output_dir: str) -> None:
    """Save individual job XML files"""
    
    successful_saves = 0
    
    for job_name, job_xml in jobs:
        try:
            # Create safe filename
            safe_filename = "".join(c for c in job_name if c.isalnum() or c in ('-', '_'))
            output_file = os.path.join(output_dir, f"{safe_filename}.xml")
            
            # Write job XML to file
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(job_xml)
            
            successful_saves += 1
            print(f"  💾 Saved: {output_file}")
            
        except Exception as e:
            print(f"❌ Error saving job {job_name}: {e}")
    
    print(f"\n✅ Successfully saved {successful_saves} job files")


def create_job_index(jobs: List[Tuple[str, str]], output_dir: str) -> None:
    """Create an index file listing all extracted jobs"""
    
    index_content = """# DataStage Jobs Index

This directory contains individual XML files for each DataStage job extracted from CCODS.xml

## Jobs List

| Job Name | File | Description |
|----------|------|-------------|
"""
    
    # Sort jobs alphabetically
    sorted_jobs = sorted(jobs, key=lambda x: x[0])
    
    for job_name, _ in sorted_jobs:
        safe_filename = "".join(c for c in job_name if c.isalnum() or c in ('-', '_'))
        index_content += f"| `{job_name}` | `{safe_filename}.xml` | DataStage Job Definition |\n"
    
    index_content += f"""
## Statistics

- Total Jobs: {len(jobs)}
- Generated: {import_datetime()}

## Usage

Each XML file contains the complete DataStage job definition including:
- Job properties and parameters
- Stage definitions and connections
- Transformation logic
- Error handling configuration

You can analyze individual jobs using XML parsers or text editors.
"""
    
    index_file = os.path.join(output_dir, "README.md")
    
    try:
        with open(index_file, 'w', encoding='utf-8') as f:
            f.write(index_content)
        print(f"📋 Created job index: {index_file}")
    except Exception as e:
        print(f"⚠️  Warning: Could not create index file: {e}")


def import_datetime():
    """Import datetime and return current timestamp"""
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def main():
    """Main execution function"""
    
    # Setup argument parser
    parser = argparse.ArgumentParser(
        description="Split DataStage XML export into individual job files"
    )
    parser.add_argument(
        '--input', 
        default='../../current_state/GDW1_Datastage_POC_JobDesignXML/CCODS.xml',
        help='Path to input DataStage XML file (default: ../../current_state/GDW1_Datastage_POC_JobDesignXML/CCODS.xml)'
    )
    parser.add_argument(
        '--output',
        default='../curr_stte/ccods',
        help='Output directory for individual job files (default: ../curr_stte/ccods)'
    )
    
    args = parser.parse_args()
    
    # Convert to absolute paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = os.path.join(script_dir, args.input)
    output_dir = os.path.join(script_dir, args.output)
    
    print("🚀 DataStage XML Job Splitter")
    print("=" * 50)
    print(f"📥 Input file: {input_file}")
    print(f"📤 Output directory: {output_dir}")
    print()
    
    # Validate input file
    if not validate_input_file(input_file):
        sys.exit(1)
    
    # Setup output directory
    setup_directories(output_dir)
    
    # Extract jobs from XML
    jobs = extract_jobs_from_xml(input_file)
    
    if not jobs:
        print("❌ No jobs found in XML file")
        sys.exit(1)
    
    # Save individual job files
    print(f"\n💾 Saving {len(jobs)} job files...")
    save_job_files(jobs, output_dir)
    
    # Create job index
    print("\n📋 Creating job index...")
    create_job_index(jobs, output_dir)
    
    print(f"\n🎉 Processing complete!")
    print(f"   Jobs saved to: {output_dir}")
    print(f"   Check README.md for job listing")


if __name__ == "__main__":
    main()