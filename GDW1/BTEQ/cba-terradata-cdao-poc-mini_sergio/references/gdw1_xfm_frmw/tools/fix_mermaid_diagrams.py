#!/usr/bin/env python3
"""
Fix Mermaid diagrams by removing problematic classDef styling.

This script removes classDef and class styling from Mermaid diagrams
to ensure compatibility across different renderers.
"""

import os
import re
import glob

def fix_mermaid_diagram(content):
    """Remove classDef styling from Mermaid diagram content."""
    lines = content.split('\n')
    result_lines = []
    in_mermaid = False
    
    for line in lines:
        # Check if we're entering or leaving a mermaid block
        if line.strip() == '```mermaid':
            in_mermaid = True
            result_lines.append(line)
        elif line.strip() == '```' and in_mermaid:
            in_mermaid = False
            result_lines.append(line)
        elif in_mermaid:
            # Skip classDef and class lines
            if (line.strip().startswith('classDef ') or 
                line.strip().startswith('class ') or
                line.strip() == ''):
                # Skip empty lines and styling lines within mermaid blocks
                if line.strip() != '':
                    continue
            result_lines.append(line)
        else:
            result_lines.append(line)
    
    return '\n'.join(result_lines)

def main():
    """Fix all Mermaid diagrams in BCFINSG documentation."""
    docs_dir = '../_docs/BCFINSG'
    
    if not os.path.exists(docs_dir):
        print(f"Error: {docs_dir} does not exist")
        return
    
    # Find all markdown files
    md_files = glob.glob(os.path.join(docs_dir, '*.md'))
    
    print(f"Found {len(md_files)} markdown files to check")
    
    fixed_count = 0
    for md_file in md_files:
        print(f"Processing: {os.path.basename(md_file)}")
        
        # Read the file
        with open(md_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check if it contains classDef
        if 'classDef' in content:
            print(f"  -> Fixing Mermaid diagrams")
            
            # Fix the content
            fixed_content = fix_mermaid_diagram(content)
            
            # Write back the fixed content
            with open(md_file, 'w', encoding='utf-8') as f:
                f.write(fixed_content)
            
            fixed_count += 1
        else:
            print(f"  -> No classDef styling found")
    
    print(f"\nFixed {fixed_count} files with Mermaid diagram issues")
    print("All diagrams should now render properly!")

if __name__ == '__main__':
    main()