#!/usr/bin/env python3

import re
from collections import defaultdict, deque

def parse_views_from_file():
    """Parse individual view definitions from the SQL file"""
    
    with open('tmp5-SF Views.sql', 'r') as f:
        content = f.read()
    
    # Find all view definitions using a more robust approach
    views = {}
    schemas = set()
    dependencies = defaultdict(set)
    
    # Split by CREATE OR REPLACE VIEW to get individual views
    parts = re.split(r'(CREATE OR REPLACE VIEW\s+PS_GDW1_BTEQ\.[A-Z_0-9]+\.[A-Z_0-9]+)', content)
    
    for i in range(1, len(parts), 2):  # Every odd index contains the CREATE statement
        if i + 1 < len(parts):
            create_statement = parts[i]
            view_body = parts[i + 1]
            
            # Extract view name
            match = re.search(r'CREATE OR REPLACE VIEW\s+PS_GDW1_BTEQ\.([A-Z_0-9]+)\.([A-Z_0-9]+)', create_statement)
            if match:
                schema = match.group(1)
                view_name = match.group(2)
                full_view_name = f"{schema}.{view_name}"
                
                schemas.add(schema)
                
                # Get the complete view definition until the next view or end
                # Find the end of this view (semicolon followed by optional comments until next CREATE or end)
                view_end_pattern = r';[\s\S]*?(?=CREATE OR REPLACE VIEW|$)'
                view_match = re.search(view_end_pattern, view_body)
                
                if view_match:
                    complete_view = create_statement + view_body[:view_match.end()]
                else:
                    complete_view = create_statement + view_body
                
                views[full_view_name] = complete_view
                
                # Find dependencies on PS_GDW1_BTEQ views
                dep_patterns = [
                    r'FROM\s+PS_GDW1_BTEQ\.([A-Z_0-9]+)\.([A-Z_0-9]+)',
                    r'JOIN\s+PS_GDW1_BTEQ\.([A-Z_0-9]+)\.([A-Z_0-9]+)',
                ]
                
                for pattern in dep_patterns:
                    deps = re.findall(pattern, view_body, re.IGNORECASE)
                    for dep_schema, dep_view in deps:
                        dep_name = f"{dep_schema}.{dep_view}"
                        if dep_name != full_view_name:  # Don't add self-dependency
                            dependencies[full_view_name].add(dep_name)
    
    return views, schemas, dependencies

def topological_sort_views(views, dependencies):
    """Sort views in dependency order"""
    
    # Build graph
    graph = defaultdict(list)
    in_degree = defaultdict(int)
    all_views = set(views.keys())
    
    # Initialize in-degree
    for view in all_views:
        in_degree[view] = 0
    
    # Build dependency graph
    for view, deps in dependencies.items():
        for dep in deps:
            if dep in all_views:  # Only consider dependencies within our view set
                graph[dep].append(view)
                in_degree[view] += 1
    
    # Kahn's algorithm for topological sort
    queue = deque([view for view in all_views if in_degree[view] == 0])
    sorted_views = []
    
    while queue:
        current = queue.popleft()
        sorted_views.append(current)
        
        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    
    # Handle cycles by adding remaining views
    if len(sorted_views) != len(all_views):
        remaining = all_views - set(sorted_views)
        print(f"Warning: Circular dependencies or missing views: {remaining}")
        sorted_views.extend(sorted(remaining))
    
    return sorted_views

def rebuild_file_with_order(views, schemas, sorted_views, dependencies):
    """Rebuild the SQL file with proper ordering"""
    
    lines = []
    
    # Header
    lines.append("-- Snowflake Views converted from Teradata")
    lines.append("-- Generated automatically with dependency ordering")
    lines.append("USE ROLE ACCOUNTADMIN;")
    lines.append("USE DATABASE PS_GDW1_BTEQ;")
    lines.append("")
    
    # Schema creation
    lines.append("-- Create all required schemas")
    for schema in sorted(schemas):
        lines.append(f"CREATE SCHEMA IF NOT EXISTS PS_GDW1_BTEQ.{schema};")
    lines.append("")
    
    # Views in dependency order
    lines.append("-- ===============================")
    lines.append("-- VIEWS IN DEPENDENCY ORDER")
    lines.append("-- ===============================")
    lines.append("")
    
    for i, view_name in enumerate(sorted_views, 1):
        if view_name in views:
            # Add comment showing dependencies
            if view_name in dependencies and dependencies[view_name]:
                deps_list = sorted(dependencies[view_name])
                lines.append(f"-- {i:2d}. {view_name} (depends on: {', '.join(deps_list)})")
            else:
                lines.append(f"-- {i:2d}. {view_name}")
            
            lines.append(views[view_name])
            lines.append("")
    
    return '\n'.join(lines)

def main():
    """Main function"""
    
    print("Parsing views from tmp5-SF Views.sql...")
    views, schemas, dependencies = parse_views_from_file()
    
    print(f"Found {len(views)} views across {len(schemas)} schemas")
    print(f"Schemas: {sorted(schemas)}")
    
    print("\nDependencies found:")
    for view, deps in dependencies.items():
        if deps:
            print(f"  {view} depends on: {', '.join(sorted(deps))}")
    
    print("\nSorting views by dependencies...")
    sorted_views = topological_sort_views(views, dependencies)
    
    print("\nView creation order:")
    for i, view in enumerate(sorted_views, 1):
        deps = dependencies.get(view, set())
        if deps:
            print(f"  {i:2d}. {view} (depends on: {', '.join(sorted(deps))})")
        else:
            print(f"  {i:2d}. {view}")
    
    print("\nRebuilding SQL file...")
    new_content = rebuild_file_with_order(views, schemas, sorted_views, dependencies)
    
    with open('tmp5-SF Views.sql', 'w') as f:
        f.write(new_content)
    
    print("✅ Successfully updated tmp5-SF Views.sql")
    print(f"   - Added {len(schemas)} schema creation statements")
    print(f"   - Ordered {len(views)} views by dependencies")

if __name__ == "__main__":
    main() 