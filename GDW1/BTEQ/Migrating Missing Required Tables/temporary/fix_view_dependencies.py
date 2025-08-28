#!/usr/bin/env python3

import re
from collections import defaultdict, deque

def extract_schemas_and_views():
    """Extract all schemas and view definitions from the SQL file"""
    
    with open('tmp5-SF Views.sql', 'r') as f:
        content = f.read()
    
    # Extract all CREATE VIEW statements
    view_pattern = r'CREATE OR REPLACE VIEW\s+PS_GDW1_BTEQ\.([A-Z_0-9]+)\.([A-Z_0-9]+)'
    views = re.findall(view_pattern, content)
    
    schemas = set()
    for schema, view_name in views:
        schemas.add(schema)
    
    print(f"Found schemas: {sorted(schemas)}")
    print(f"Found {len(views)} views")
    
    return schemas, content

def analyze_view_dependencies(content):
    """Analyze dependencies between views"""
    
    # Split content into individual view definitions
    view_sections = re.split(r'(CREATE OR REPLACE VIEW\s+PS_GDW1_BTEQ\.[A-Z_0-9]+\.[A-Z_0-9]+)', content)
    
    view_definitions = {}
    view_dependencies = defaultdict(set)
    
    current_view = None
    for i, section in enumerate(view_sections):
        if re.match(r'CREATE OR REPLACE VIEW\s+PS_GDW1_BTEQ\.[A-Z_0-9]+\.[A-Z_0-9]+', section):
            # Extract view name
            match = re.search(r'CREATE OR REPLACE VIEW\s+PS_GDW1_BTEQ\.([A-Z_0-9]+)\.([A-Z_0-9]+)', section)
            if match:
                current_view = f"{match.group(1)}.{match.group(2)}"
                view_definitions[current_view] = section
        elif current_view and i + 1 < len(view_sections):
            # This is the view body
            view_body = view_sections[i + 1]
            view_definitions[current_view] += view_body
            
            # Find dependencies on other PS_GDW1_BTEQ views
            dep_pattern = r'FROM\s+PS_GDW1_BTEQ\.([A-Z_0-9]+)\.([A-Z_0-9]+)'
            dependencies = re.findall(dep_pattern, view_body, re.IGNORECASE)
            
            for dep_schema, dep_view in dependencies:
                dep_full_name = f"{dep_schema}.{dep_view}"
                if dep_full_name in view_definitions or dep_full_name != current_view:
                    view_dependencies[current_view].add(dep_full_name)
                    print(f"  {current_view} depends on {dep_full_name}")
    
    return view_definitions, view_dependencies

def topological_sort(view_definitions, dependencies):
    """Sort views in dependency order using topological sort"""
    
    # Create adjacency list and in-degree count
    graph = defaultdict(list)
    in_degree = defaultdict(int)
    all_views = set(view_definitions.keys())
    
    # Initialize in-degree
    for view in all_views:
        in_degree[view] = 0
    
    # Build graph and calculate in-degrees
    for view, deps in dependencies.items():
        for dep in deps:
            if dep in all_views:  # Only consider dependencies that are in our view set
                graph[dep].append(view)
                in_degree[view] += 1
    
    # Topological sort using Kahn's algorithm
    queue = deque([view for view in all_views if in_degree[view] == 0])
    sorted_views = []
    
    while queue:
        current = queue.popleft()
        sorted_views.append(current)
        
        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    
    # Check for cycles
    if len(sorted_views) != len(all_views):
        print("Warning: Circular dependencies detected!")
        remaining_views = all_views - set(sorted_views)
        print(f"Views not processed due to cycles: {remaining_views}")
        sorted_views.extend(remaining_views)  # Add remaining views at the end
    
    return sorted_views

def rebuild_sql_file(schemas, view_definitions, sorted_views):
    """Rebuild the SQL file with proper schema creation and view ordering"""
    
    new_content = []
    
    # Header
    new_content.append("-- Snowflake Views converted from Teradata")
    new_content.append("-- Generated automatically with dependency ordering")
    new_content.append("USE ROLE ACCOUNTADMIN;")
    new_content.append("USE DATABASE PS_GDW1_BTEQ;")
    new_content.append("")
    
    # Create schemas
    new_content.append("-- Create all required schemas")
    for schema in sorted(schemas):
        new_content.append(f"CREATE SCHEMA IF NOT EXISTS PS_GDW1_BTEQ.{schema};")
    new_content.append("")
    
    new_content.append("-- ===============================")
    new_content.append("-- VIEWS IN DEPENDENCY ORDER")
    new_content.append("-- ===============================")
    new_content.append("")
    
    # Add views in dependency order
    for view_name in sorted_views:
        if view_name in view_definitions:
            full_definition = view_definitions[view_name]
            new_content.append(f"-- {view_name}")
            new_content.append(full_definition)
            new_content.append("")
    
    return '\n'.join(new_content)

def main():
    """Main function to fix view dependencies"""
    
    print("Analyzing tmp5-SF Views.sql...")
    
    # Extract schemas and content
    schemas, content = extract_schemas_and_views()
    
    # Analyze dependencies
    print("\nAnalyzing view dependencies...")
    view_definitions, dependencies = analyze_view_dependencies(content)
    
    print(f"\nFound {len(view_definitions)} view definitions")
    print(f"Found dependencies:")
    for view, deps in dependencies.items():
        if deps:
            print(f"  {view} -> {', '.join(deps)}")
    
    # Sort views topologically
    print("\nSorting views in dependency order...")
    sorted_views = topological_sort(view_definitions, dependencies)
    
    print(f"View creation order:")
    for i, view in enumerate(sorted_views, 1):
        print(f"  {i:2d}. {view}")
    
    # Rebuild SQL file
    print("\nRebuilding SQL file...")
    new_content = rebuild_sql_file(schemas, view_definitions, sorted_views)
    
    # Write updated file
    with open('tmp5-SF Views.sql', 'w') as f:
        f.write(new_content)
    
    print("✅ Successfully updated tmp5-SF Views.sql with:")
    print(f"   - {len(schemas)} schema creation statements")
    print(f"   - {len(sorted_views)} views in dependency order")

if __name__ == "__main__":
    main() 