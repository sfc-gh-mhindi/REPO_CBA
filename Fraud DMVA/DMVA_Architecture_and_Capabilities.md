# DMVA Architecture & Capabilities Documentation

## System Overview

**DMVA (Data Migration Validation & Automation)** is an enterprise-grade data migration framework that orchestrates large-scale data movement from source systems (Teradata, or other enterprise data warehouses) to Snowflake, with comprehensive validation and monitoring capabilities. The framework creates Glue-managed Iceberg tables natively integrated with Snowflake.

---

## Architecture Flow Diagram

### Mermaid Architecture Diagram

```mermaid
flowchart TD
    %% Styling
    classDef ec2Class fill:#ff9900,stroke:#232f3e,stroke-width:2px,color:#fff
    classDef teradataClass fill:#00a651,stroke:#2d5016,stroke-width:2px,color:#fff
    classDef s3Class fill:#569a31,stroke:#2d5016,stroke-width:2px,color:#fff
    classDef snowflakeClass fill:#29b5e8,stroke:#1a73e8,stroke-width:2px,color:#fff
    classDef glueClass fill:#fd79a8,stroke:#e84393,stroke-width:2px,color:#fff

    %% Teradata Source (Standalone Left)
    subgraph Teradata["🏢 Teradata<br/>Source data warehouse"]
        TDTables["📊 Teradata Tables<br/>Enterprise production data"]
        NOSProcess["📤 NOS Process<br/>Native object store export"]
    end

    %% EC2 DMVA Core
    subgraph EC2["🔧 EC2 DMVA<br/>Orchestration platform"]
        Dispatcher["📊 DMVA Dispatcher<br/>Task coordination"]
        Workers["👷 Worker Pools<br/>Parallel processing"]
    end

    %% Snowflake Target Platform
    subgraph SF["❄️ Snowflake<br/>Target data platform"]
        DMVAMeta["🗂️ DMVA Metadata Tables<br/>System configuration"]
        DMVARuntime["⚙️ DMVA Runtime Tables<br/>Operational workflows"]
        IcebergCatalog["🧊 Iceberg Catalog Database<br/>External catalog integration"]
    end

    %% S3 Storage
    subgraph S3["☁️ AWS S3<br/>Object storage"]
        ParquetFiles["📦 Parquet Files<br/>Columnar data format"]
    end

    %% AWS Glue
    GlueCatalog["🗄️ AWS Glue Catalog<br/>Metadata management"]

    %% Data Flow
    TDTables --> NOSProcess
    NOSProcess --> ParquetFiles
    
    %% DMVA Control
    Dispatcher --> TDTables
    Dispatcher --> DMVAMeta
    Workers --> NOSProcess
    
    %% Iceberg Integration
    IcebergCatalog --> GlueCatalog
    IcebergCatalog --> ParquetFiles
    
    %% Metadata Flow
    DMVAMeta --> IcebergCatalog
    DMVARuntime --> Workers

    %% Apply Styles
    class Teradata,TDTables,NOSProcess teradataClass
    class EC2,Dispatcher,Workers ec2Class
    class SF,DMVAMeta,DMVARuntime,IcebergCatalog snowflakeClass
    class S3,ParquetFiles s3Class
    class GlueCatalog glueClass
```

---

## Prerequisites

- **EC2 Instance**: Hosts the DMVA dispatcher and worker processes that orchestrate the entire migration workflow. A t2.2xlarge instance (8 vCPUs, 32 GB RAM) with approximately 100 GB of EBS storage is recommended as a starting point, with more storage required based on data volume
- **Snowflake Schema**: Dedicated schema to store DMVA metadata tables and operational data, housing system configuration, task orchestration data, validation results, and runtime operational workflows

---

## Capabilities

### Metadata Configuration and Discovery

**Configuration**
- **Source System Connections**: Flexible configuration of source system connection details with support for multiple database types, authentication methods, and connection pooling options
- **Replication Scope Management**: Granular control over migration scope including database-level, schema-level, and table-level inclusion/exclusion rules with pattern matching and filtering capabilities
- **Migration Parameters**: Configurable data extraction parameters, partition strategies, and parallel processing settings

**Dynamic Metadata Sourcing**
- **Structural Metadata Extraction**: Automated discovery and cataloging of database schemas, table definitions, column metadata, data types, constraints, and indexes
- **Table Inventory**: Real-time cataloging of tables, views, primary keys, and foreign key relationships
- **Delta Processing**: Handles incremental metadata updates and new objects during migration cycles

**Snow Convert for DDL Conversion**
- **Schema Transformation**: Snowflake's Snow Convert utility can be used to deterministically transform source database table structures to Snowflake-native tables or Iceberg tables, providing automated DDL conversion and schema mapping

### Data Extraction and Storage

**NOS (Native Object Store) Integration**
- **Direct Export**: Leverages Teradata's Native Object Store functionality to export data directly to S3 without intermediate storage
- **Parquet Format**: Data is extracted into Parquet format, providing optimal columnar storage for efficient ingestion into Snowflake
- **Partitioned Extraction**: Intelligent partitioning of large tables for optimal performance and parallel processing
- **Error Handling**: Robust retry mechanisms and error recovery for failed extraction processes

**Snowflake Integration via Catalog-Linked Databases**
- **Glue-Managed Iceberg Tables**: Creates AWS Glue-managed Iceberg tables natively accessible through Snowflake
- **External Catalog Integration**: Seamless integration between Snowflake and AWS Glue catalog for metadata management
- **Direct Data Access**: Tables accessible directly in Snowflake through catalog-linked database connections

### Data Validation and Quality Assurance

**Data Validation Types**
- **Row Count Validation**: Compares total row counts between source and target tables at both table and partition levels
- **Checksum Validation**: Generates and compares checksums using configurable hash functions (MD5 by default) for data integrity verification
- **Column-Level Measures**: Calculates and compares statistical measures including:
  - Null counts per column
  - Distinct counts per column (optional)
  - Min/max values for numeric and date columns (optional)
  - Sum values for numeric columns (optional)
- **Partition-Based Validation**: Supports validation at partition level using various partitioning strategies:
  - Whole table validation (default for smaller tables)
  - Integer-based partitioning (modulus on numeric columns)
  - Date-based partitioning (by month, year, etc.)
  - Substring-based partitioning (for character columns)

**Validation Flexibility**
- **Configurable Checksum Methods**: Tables can use different partitioning strategies based on their structure and size
- **Validate-Only Mode**: Tables can be marked for validation-only without migration for pre-migrated data
- **Selective Column Validation**: Ability to include/exclude specific columns from validation measures
- **Custom Filters**: Support for source and target filters to handle data transformation scenarios

### Monitoring and Reporting

**Migration Progress Reporting**
- **Data Volume Tracking**: Real-time monitoring of data volumes replicated, including table-by-table progress
- **Success Rate Metrics**: Comprehensive reporting on migration success rates, failed transfers, and retry statistics
- **Performance Analytics**: Throughput metrics, processing times, and resource utilization tracking
- **Validation Results**: Detailed reporting on validation outcomes, including checksum verification results and data quality scores

---

*This document provides a comprehensive overview of DMVA's architecture and capabilities. For implementation details and configuration guides, refer to the technical documentation in the `/documentation` directory.*
