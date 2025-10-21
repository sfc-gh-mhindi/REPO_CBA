#!/usr/bin/env python3
"""
DMVA Teradata Migration Performance Calculator

Estimates migration time, resource requirements, and optimal configurations
for migrating Teradata tables to Snowflake using DMVA.
"""

import math

class DMVAPerformanceCalculator:
    def __init__(self):
        # Default performance characteristics
        self.teradata_throughput_mbs = 100  # MB/s per unload session
        self.network_bandwidth_mbps = 1000  # 1 Gbps
        self.parquet_compression_ratio = 0.6
        self.disk_safety_factor = 2.0
        
        # Snowflake ingest rates (GB/min) by warehouse size
        self.snowflake_ingest_rates = {
            'XS': 1.5,
            'S': 3,
            'M': 6,
            'L': 12,
            'XL': 24,
            '2XL': 48,
            '3XL': 96,
            '4XL': 192
        }
        
        # VM resource estimates (memory per worker in GB)
        self.memory_per_worker_gb = 2
        self.max_teradata_sessions = 50
        
    def calculate_disk_space(self, table_size_gb, num_concurrent_tables=2):
        """Calculate required VM disk space for staging files."""
        required_gb = (table_size_gb * self.parquet_compression_ratio * 
                      num_concurrent_tables * self.disk_safety_factor)
        return math.ceil(required_gb)
    
    def calculate_unload_time(self, table_size_gb, num_partitions=1):
        """Calculate Teradata unload time in minutes."""
        table_size_mb = table_size_gb * 1024
        # Parallel unload if multiple partitions
        effective_throughput = self.teradata_throughput_mbs * min(num_partitions, 4)
        unload_time_mins = table_size_mb / (effective_throughput * 60)
        return math.ceil(unload_time_mins)
    
    def calculate_transfer_time(self, table_size_gb):
        """Calculate network transfer time in minutes."""
        table_size_mb = table_size_gb * 1024
        bandwidth_mbps = self.network_bandwidth_mbps / 8  # Convert to MB/s
        transfer_time_mins = table_size_mb / (bandwidth_mbps * 60)
        return math.ceil(transfer_time_mins)
    
    def calculate_load_time(self, table_size_gb, warehouse_size='S'):
        """Calculate Snowflake load time in minutes."""
        ingest_rate = self.snowflake_ingest_rates.get(warehouse_size, 3)
        load_time_mins = table_size_gb / ingest_rate
        return math.ceil(load_time_mins)
    
    def calculate_measure_time(self, num_columns, num_queries, avg_query_time_secs=30):
        """Calculate measure_partitions time in minutes."""
        total_time_secs = num_queries * avg_query_time_secs
        return math.ceil(total_time_secs / 60)
    
    def estimate_num_measure_queries(self, num_columns, num_aggregations, max_select_columns=76):
        """Estimate number of measure queries based on parameters."""
        # From system.py logic:
        # measure_column_count = 1 (column_name) + num_aggregations
        measure_column_count = 1 + num_aggregations
        columns_per_query = max(math.floor(max_select_columns / measure_column_count), 1)
        num_queries = math.ceil(num_columns / columns_per_query)
        return num_queries
    
    def recommend_worker_pool_size(self, vm_cpus, vm_memory_gb):
        """Recommend optimal worker pool size based on VM resources."""
        # Conservative approach
        by_cpu = vm_cpus
        by_memory = math.floor(vm_memory_gb / self.memory_per_worker_gb)
        by_connections = math.floor(self.max_teradata_sessions / 2)
        
        optimal_size = min(by_cpu, by_memory, by_connections)
        return max(optimal_size, 4)  # Minimum 4 workers
    
    def recommend_warehouse_size(self, total_data_tb, time_window_hours=24):
        """Recommend Snowflake warehouse size based on data volume."""
        required_rate_gb_per_min = (total_data_tb * 1024) / (time_window_hours * 60)
        
        for size, rate in self.snowflake_ingest_rates.items():
            if rate >= required_rate_gb_per_min:
                return size
        
        return '4XL'  # Largest if nothing else fits
    
    def calculate_migration(self, table_size_gb, num_columns, row_count, 
                          num_partitions=1, num_aggregations=1, 
                          warehouse_size='S', max_select_columns=76):
        """Complete migration time calculation for a table."""
        
        # Calculate individual components
        unload_time = self.calculate_unload_time(table_size_gb, num_partitions)
        transfer_time = self.calculate_transfer_time(table_size_gb)
        load_time = self.calculate_load_time(table_size_gb, warehouse_size)
        
        # Measure time
        num_measure_queries = self.estimate_num_measure_queries(
            num_columns, num_aggregations, max_select_columns
        )
        measure_time = self.calculate_measure_time(num_columns, num_measure_queries)
        
        # Critical path (bottleneck)
        data_movement_time = max(unload_time, transfer_time, load_time)
        total_time = measure_time + data_movement_time
        
        # Identify bottleneck
        bottleneck = 'Teradata Unload'
        if transfer_time == max(unload_time, transfer_time, load_time):
            bottleneck = 'Network Transfer'
        elif load_time == max(unload_time, transfer_time, load_time):
            bottleneck = 'Snowflake Load'
        
        return {
            'measure_time_mins': measure_time,
            'num_measure_queries': num_measure_queries,
            'unload_time_mins': unload_time,
            'transfer_time_mins': transfer_time,
            'load_time_mins': load_time,
            'total_time_mins': total_time,
            'total_time_hours': round(total_time / 60, 2),
            'bottleneck': bottleneck,
            'disk_space_needed_gb': self.calculate_disk_space(table_size_gb)
        }
    
    def print_report(self, table_name, results):
        """Print a formatted performance report."""
        print("\n" + "=" * 80)
        print(f"DMVA PERFORMANCE ESTIMATE: {table_name}")
        print("=" * 80)
        
        print(f"\n📊 Measure Phase:")
        print(f"   Queries to run: {results['num_measure_queries']}")
        print(f"   Estimated time: {results['measure_time_mins']} minutes")
        
        print(f"\n📤 Data Movement Phase:")
        print(f"   Teradata unload: {results['unload_time_mins']} minutes")
        print(f"   Network transfer: {results['transfer_time_mins']} minutes")
        print(f"   Snowflake load: {results['load_time_mins']} minutes")
        print(f"   Bottleneck: {results['bottleneck']}")
        
        print(f"\n⏱️  Total Migration Time:")
        print(f"   {results['total_time_mins']} minutes ({results['total_time_hours']} hours)")
        
        print(f"\n💾 VM Disk Space Required:")
        print(f"   {results['disk_space_needed_gb']} GB")
        
        print("\n" + "=" * 80)


def main():
    """Example usage with realistic scenarios."""
    calc = DMVAPerformanceCalculator()
    
    # Example 1: Your 135-column table scenario
    print("\nSCENARIO 1: Large Wide Table (Your Case)")
    print("-" * 80)
    print("Table: 135 columns, 100 GB, 10M rows")
    print("Config: Only distinct_count, max_select_columns=76")
    
    results1 = calc.calculate_migration(
        table_size_gb=100,
        num_columns=135,
        row_count=10_000_000,
        num_partitions=5,
        num_aggregations=1,  # Only distinct_count
        warehouse_size='S',
        max_select_columns=76
    )
    calc.print_report("WIDE_TABLE_135_COLS", results1)
    
    # Example 2: Multiple tables
    print("\n\nSCENARIO 2: Multiple Table Migration")
    print("-" * 80)
    
    tables = [
        ("CUSTOMER", 50, 25, 5_000_000, 2),
        ("ORDERS", 150, 80, 50_000_000, 10),
        ("PRODUCTS", 30, 40, 1_000_000, 1),
        ("TRANSACTIONS", 500, 120, 100_000_000, 20)
    ]
    
    total_time = 0
    total_disk = 0
    
    for table_name, table_size_gb, num_cols, row_count, num_parts in tables:
        results = calc.calculate_migration(
            table_size_gb=table_size_gb,
            num_columns=num_cols,
            row_count=row_count,
            num_partitions=num_parts,
            num_aggregations=1,
            warehouse_size='M',
            max_select_columns=76
        )
        total_time += results['total_time_mins']
        total_disk = max(total_disk, results['disk_space_needed_gb'])
        
        print(f"\n{table_name}: {table_size_gb}GB, {num_cols} cols")
        print(f"  Time: {results['total_time_mins']} mins | Disk: {results['disk_space_needed_gb']} GB")
    
    print(f"\n{'=' * 80}")
    print(f"TOTAL SEQUENTIAL TIME: {total_time} minutes ({round(total_time/60, 2)} hours)")
    print(f"MAX DISK SPACE NEEDED: {total_disk} GB")
    
    # Worker pool recommendation
    print(f"\n\n📋 RESOURCE RECOMMENDATIONS")
    print("-" * 80)
    
    vm_specs = [
        ("Minimum", 8, 32),
        ("Recommended", 16, 64),
        ("High-Performance", 32, 128)
    ]
    
    for spec_name, cpus, memory in vm_specs:
        pool_size = calc.recommend_worker_pool_size(cpus, memory)
        parallel_time = math.ceil(total_time / pool_size)
        print(f"\n{spec_name} VM ({cpus} CPUs, {memory}GB RAM):")
        print(f"  Optimal worker pool: {pool_size} workers")
        print(f"  Parallel migration time: ~{parallel_time} minutes ({round(parallel_time/60, 2)} hours)")
    
    # Warehouse recommendation
    total_data_tb = sum(t[1] for t in tables) / 1024
    print(f"\n\nSnowflake Warehouse Recommendation:")
    print(f"  Total data: {round(total_data_tb, 2)} TB")
    for hours in [4, 8, 24]:
        wh_size = calc.recommend_warehouse_size(total_data_tb, hours)
        print(f"  For {hours}-hour window: {wh_size} warehouse")


if __name__ == "__main__":
    main()
