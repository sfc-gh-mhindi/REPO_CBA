#!/usr/bin/env python3
"""
Recalibrate DMVA calculator coefficients using actual execution data.
Uses all 3 tables from DMVA Exec Results.csv with correct values.
"""

import numpy as np
from scipy.optimize import minimize

# Actual data from CSV
tables = [
    {
        'name': 'DERV_ACCT_PATY',
        'size_gb': 118.96,
        'rows': 1_063_490_694,
        'columns': 11,
        'td_checksum_actual': 5,
        'td_unload_actual': 2,
        'sf_load_actual': 2,
        'sf_store_actual': 1,
        'total_actual': 7,
        'num_aggregations': 5,  # Assuming default (all enabled)
        'max_select_columns': 1000,
        'partitions_est': 10  # Estimated based on table size
    },
    {
        'name': 'ACCT_BASE',
        'size_gb': 116.02,
        'rows': 900_156_213,
        'columns': 135,
        'td_checksum_actual': 2,
        'td_unload_actual': 1,
        'sf_load_actual': 2,
        'sf_store_actual': 2,
        'total_actual': 6,
        'num_aggregations': 5,  # Assuming default (all enabled)
        'max_select_columns': 1000,
        'partitions_est': 10  # Estimated
    },
    {
        'name': 'PLAN_BALN_SEGM_MSTR',
        'size_gb': 43.18,
        'rows': 544_455_967,
        'columns': 29,
        'td_checksum_actual': 20,
        'td_unload_actual': 6,
        'sf_load_actual': 10,
        'sf_store_actual': 3,
        'total_actual': 22,
        'num_aggregations': 1,  # Only distinct_count from notes
        'max_select_columns': 154,
        'partitions_est': 5  # Estimated
    }
]

def calculate_measure_queries(column_count, num_aggregations, max_select_columns):
    """Calculate number of measure queries based on DMVA logic."""
    measure_items_per_column = 1 + num_aggregations
    columns_per_query = max(int(max_select_columns / measure_items_per_column), 1)
    num_queries = int(np.ceil(column_count / columns_per_query))
    return num_queries

def estimate_with_params(table, params):
    """Estimate migration time with given parameters."""
    (checksum_base, checksum_per_query, checksum_per_col_agg,
     unload_base, unload_per_gb,
     load_base, load_per_gb,
     store_base, store_per_partition,
     pipeline_eff) = params
    
    # Checksum/Measure time
    num_queries = calculate_measure_queries(
        table['columns'], 
        table['num_aggregations'], 
        table['max_select_columns']
    )
    checksum = (checksum_base + 
                (num_queries * checksum_per_query) + 
                (table['columns'] * table['num_aggregations'] * checksum_per_col_agg))
    
    # Unload time
    unload = unload_base + (table['size_gb'] * unload_per_gb)
    # Adjust for parallelism (up to 4x)
    if table['partitions_est'] > 1:
        parallel_factor = min(table['partitions_est'], 4)
        unload = unload / np.sqrt(parallel_factor)
    
    # Load time
    load = load_base + (table['size_gb'] * load_per_gb)
    
    # Store checksum time
    store = store_base + (table['partitions_est'] * store_per_partition)
    
    # Total with pipeline parallelism
    data_movement = max(unload, load)
    total = checksum + (data_movement + store) * pipeline_eff
    
    return checksum, unload, load, store, total

def objective_function(params):
    """
    Minimize sum of squared errors across all phases and tables.
    """
    total_error = 0
    
    for table in tables:
        checksum_est, unload_est, load_est, store_est, total_est = estimate_with_params(table, params)
        
        # Calculate errors for each phase
        checksum_error = (checksum_est - table['td_checksum_actual'])**2
        unload_error = (unload_est - table['td_unload_actual'])**2
        load_error = (load_est - table['sf_load_actual'])**2
        store_error = (store_est - table['sf_store_actual'])**2
        total_error_component = (total_est - table['total_actual'])**2
        
        # Weight total time error more heavily
        total_error += (checksum_error + unload_error + load_error + store_error + 
                       2 * total_error_component)
    
    return total_error

# Initial guess based on previous coefficients
x0 = np.array([
    2.0,   # checksum_base
    3.0,   # checksum_per_query
    0.08,  # checksum_per_col_agg
    0.5,   # unload_base
    0.1,   # unload_per_gb
    1.0,   # load_base
    0.17,  # load_per_gb
    1.0,   # store_base
    0.4,   # store_per_partition
    0.55   # pipeline_efficiency
])

# Bounds for parameters (all must be positive, efficiency between 0 and 1)
bounds = [
    (0.1, 10),    # checksum_base
    (0.1, 20),    # checksum_per_query
    (0.001, 1),   # checksum_per_col_agg
    (0.1, 5),     # unload_base
    (0.001, 0.5), # unload_per_gb (0.001-0.5 min/GB = 2-1000 GB/min)
    (0.1, 5),     # load_base
    (0.001, 0.5), # load_per_gb
    (0.1, 5),     # store_base
    (0.001, 2),   # store_per_partition
    (0.3, 1.0)    # pipeline_efficiency
]

print("Starting calibration with actual data...")
print("\nInput data:")
for t in tables:
    print(f"\n{t['name']}:")
    print(f"  Size: {t['size_gb']} GB, Rows: {t['rows']:,}, Cols: {t['columns']}")
    print(f"  Actual: TD Chk={t['td_checksum_actual']}, Unload={t['td_unload_actual']}, "
          f"Load={t['sf_load_actual']}, Store={t['sf_store_actual']}, Total={t['total_actual']}")
    print(f"  Config: {t['num_aggregations']} aggs, max_select={t['max_select_columns']}")

# Optimize
result = minimize(objective_function, x0, method='L-BFGS-B', bounds=bounds,
                 options={'maxiter': 1000, 'ftol': 1e-8})

print("\n" + "="*80)
print("CALIBRATION RESULTS")
print("="*80)

if result.success:
    params = result.x
    print("\nOptimized coefficients:")
    print(f"  checksum_base_mins = {params[0]:.3f}")
    print(f"  checksum_per_query_mins = {params[1]:.3f}")
    print(f"  checksum_per_column_factor = {params[2]:.3f}")
    print(f"  unload_base_mins = {params[3]:.3f}")
    print(f"  unload_mins_per_gb = {params[4]:.3f}  ({1/params[4]:.1f} GB/min)")
    print(f"  load_base_mins = {params[5]:.3f}")
    print(f"  load_mins_per_gb = {params[6]:.3f}  ({1/params[6]:.1f} GB/min)")
    print(f"  store_checksum_base_mins = {params[7]:.3f}")
    print(f"  store_checksum_per_partition_mins = {params[8]:.3f}")
    print(f"  pipeline_efficiency = {params[9]:.3f}")
    
    print("\n" + "="*80)
    print("VALIDATION")
    print("="*80)
    
    for table in tables:
        checksum_est, unload_est, load_est, store_est, total_est = estimate_with_params(table, params)
        
        print(f"\n{table['name']}:")
        print(f"  TD Checksum:  Est={checksum_est:5.1f}  Actual={table['td_checksum_actual']:5.1f}  "
              f"Error={abs(checksum_est - table['td_checksum_actual']):5.1f} mins")
        print(f"  TD Unload:    Est={unload_est:5.1f}  Actual={table['td_unload_actual']:5.1f}  "
              f"Error={abs(unload_est - table['td_unload_actual']):5.1f} mins")
        print(f"  SF Load:      Est={load_est:5.1f}  Actual={table['sf_load_actual']:5.1f}  "
              f"Error={abs(load_est - table['sf_load_actual']):5.1f} mins")
        print(f"  SF Store:     Est={store_est:5.1f}  Actual={table['sf_store_actual']:5.1f}  "
              f"Error={abs(store_est - table['sf_store_actual']):5.1f} mins")
        print(f"  Total:        Est={total_est:5.1f}  Actual={table['total_actual']:5.1f}  "
              f"Error={abs(total_est - table['total_actual']):5.1f} mins  "
              f"({100 - abs(total_est - table['total_actual'])/table['total_actual']*100:.1f}% accurate)")
    
    avg_error = sum([
        abs(estimate_with_params(t, params)[4] - t['total_actual']) / t['total_actual'] 
        for t in tables
    ]) / len(tables) * 100
    
    print(f"\n{'='*80}")
    print(f"Average accuracy: {100 - avg_error:.1f}%")
    print(f"{'='*80}")
    
else:
    print(f"\nOptimization failed: {result.message}")
    print("Using initial parameters for comparison...")
    params = x0

print("\n\nCopy these values to dmva_calibrated_calculator.py __init__():")
print(f"""
    def __init__(self):
        # Calibrated coefficients from actual DMVA execution data
        # Calibrated on: DERV_ACCT_PATY, ACCT_BASE, PLAN_BALN_SEGM_MSTR
        
        self.checksum_base_mins = {params[0]:.3f}
        self.checksum_per_query_mins = {params[1]:.3f}
        self.checksum_per_column_factor = {params[2]:.3f}
        
        self.unload_base_mins = {params[3]:.3f}
        self.unload_mins_per_gb = {params[4]:.3f}  # ~{1/params[4]:.1f} GB/min
        
        self.load_base_mins = {params[5]:.3f}
        self.load_mins_per_gb = {params[6]:.3f}  # ~{1/params[6]:.1f} GB/min
        
        self.store_checksum_base_mins = {params[7]:.3f}
        self.store_checksum_per_partition_mins = {params[8]:.3f}
        
        self.pipeline_efficiency = {params[9]:.3f}
        
        self.teradata_aggregate_limit = 77
        self.teradata_aggregate_warning = 64
""")
