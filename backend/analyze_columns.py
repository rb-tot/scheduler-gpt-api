#!/usr/bin/env python3
"""
Database Column Usage Analyzer
Scans all code files to find which job_pool columns are actually being used
"""

import os
import re
from pathlib import Path
from collections import defaultdict

# List of columns from job_pool table
JOB_POOL_COLUMNS = [
    'work_order', 'site_name', 'site_address', 'site_city', 'site_state',
    'latitude', 'longitude', 'jp_status', 'jp_priority', 'due_date',
    'sow_1', 'flag_missing_due_date', 'flag_past_due', 'flag_estimated_time',
    'tank_test_only', 'is_recurring_site', 'night_test', 'days_til_due',
    'tech_count', 'zone_3', 'region', 'duration', 'cluster_id',
    'created_at', 'updated_at', 'geom', 'site_id', 'est_hours',
    'is_night', 'cluster_label'
]

# File patterns to search
FILE_PATTERNS = ['*.py', '*.html', '*.js', '*.sql']

def find_column_usage(directory, columns):
    """
    Search for column usage in all code files
    """
    usage = defaultdict(list)
    files_searched = 0
    
    # Convert to Path object
    search_dir = Path(directory)
    
    # Search through files
    for pattern in FILE_PATTERNS:
        for file_path in search_dir.rglob(pattern):
            # Skip node_modules, venv, etc
            if any(skip in str(file_path) for skip in ['node_modules', 'venv', '.git', '__pycache__']):
                continue
                
            files_searched += 1
            
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                    file_name = str(file_path.relative_to(search_dir))
                    
                    # Search for each column
                    for column in columns:
                        # Different patterns to match column usage
                        patterns = [
                            rf'\b{column}\b',  # Exact word match
                            rf'["\']]{column}["\']',  # In quotes
                            rf'\.{column}\b',  # Object property
                            rf'\[[\"\']]{column}[\"\']]\]',  # Dictionary/array access
                        ]
                        
                        for pattern in patterns:
                            if re.search(pattern, content, re.IGNORECASE):
                                # Find line numbers where it appears
                                lines = content.split('\n')
                                for i, line in enumerate(lines, 1):
                                    if re.search(pattern, line, re.IGNORECASE):
                                        usage[column].append({
                                            'file': file_name,
                                            'line': i,
                                            'context': line.strip()[:100]  # First 100 chars
                                        })
                                break  # Found in this file, move to next column
                                
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
    
    return usage, files_searched

def generate_report(usage, files_searched):
    """
    Generate a usage report
    """
    print("=" * 80)
    print("JOB_POOL COLUMN USAGE ANALYSIS")
    print("=" * 80)
    print(f"\nFiles searched: {files_searched}")
    print(f"Total columns: {len(JOB_POOL_COLUMNS)}")
    
    # Categorize columns
    used_columns = {}
    unused_columns = []
    
    for column in JOB_POOL_COLUMNS:
        if column in usage and usage[column]:
            used_columns[column] = usage[column]
        else:
            unused_columns.append(column)
    
    # Report unused columns
    print(f"\n{'='*40}")
    print(f"UNUSED COLUMNS ({len(unused_columns)})")
    print(f"{'='*40}")
    for col in sorted(unused_columns):
        print(f"  ❌ {col}")
    
    # Report used columns with details
    print(f"\n{'='*40}")
    print(f"USED COLUMNS ({len(used_columns)})")
    print(f"{'='*40}")
    
    # Sort by usage frequency
    sorted_used = sorted(used_columns.items(), key=lambda x: len(x[1]), reverse=True)
    
    for column, occurrences in sorted_used:
        # Get unique files
        unique_files = set(occ['file'] for occ in occurrences)
        print(f"\n✅ {column}")
        print(f"   Used in {len(unique_files)} files, {len(occurrences)} occurrences")
        
        # Show first few examples
        for occ in occurrences[:3]:
            print(f"   - {occ['file']}:{occ['line']}")
            print(f"     {occ['context']}")
        
        if len(occurrences) > 3:
            print(f"   ... and {len(occurrences) - 3} more occurrences")
    
    # Summary recommendations
    print(f"\n{'='*40}")
    print("RECOMMENDATIONS")
    print(f"{'='*40}")
    
    if unused_columns:
        print("\nColumns that appear safe to remove:")
        for col in unused_columns:
            print(f"  - {col}")
    
    # Identify computational columns (likely temporary)
    computational_patterns = ['flag_', 'days_til_', 'tech_count', 'cluster_']
    computational_cols = []
    
    for col in JOB_POOL_COLUMNS:
        if any(pattern in col for pattern in computational_patterns):
            if col in used_columns:
                computational_cols.append(col)
    
    if computational_cols:
        print("\nComputational/temporary columns still in use (consider if needed):")
        for col in computational_cols:
            print(f"  - {col}")
    
    # Save detailed report
    with open('column_usage_report.txt', 'w') as f:
        f.write("DETAILED COLUMN USAGE REPORT\n")
        f.write("="*80 + "\n\n")
        
        f.write(f"Files searched: {files_searched}\n")
        f.write(f"Total columns: {len(JOB_POOL_COLUMNS)}\n")
        f.write(f"Used columns: {len(used_columns)}\n")
        f.write(f"Unused columns: {len(unused_columns)}\n\n")
        
        f.write("UNUSED COLUMNS:\n")
        for col in sorted(unused_columns):
            f.write(f"  - {col}\n")
        
        f.write("\n\nDETAILED USAGE BY COLUMN:\n")
        for column, occurrences in sorted_used:
            f.write(f"\n{column}:\n")
            for occ in occurrences:
                f.write(f"  {occ['file']}:{occ['line']} - {occ['context']}\n")
    
    print("\nDetailed report saved to: column_usage_report.txt")

if __name__ == "__main__":
    # Get the directory to search
    search_directory = input("Enter the directory path to search (or press Enter for current directory): ").strip()
    if not search_directory:
        search_directory = "."
    
    if not os.path.exists(search_directory):
        print(f"Error: Directory '{search_directory}' does not exist")
        exit(1)
    
    print(f"\nSearching in: {os.path.abspath(search_directory)}")
    print("This may take a moment...\n")
    
    # Find usage
    usage, files_searched = find_column_usage(search_directory, JOB_POOL_COLUMNS)
    
    # Generate report
    generate_report(usage, files_searched)
