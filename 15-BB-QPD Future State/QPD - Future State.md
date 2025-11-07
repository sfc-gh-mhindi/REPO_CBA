# QPD Future State Architecture: Gem Outline
## Snowflake-Based Data Platform Transformation

---

## Table of Contents

1. [Introduction and Executive Summary](#introduction-and-executive-summary)
   - 1.1 [Background](#11-background)
   - 1.2 [Purpose of this Document](#12-purpose-of-this-document)
   - 1.3 [Executive Summary](#13-executive-summary)
   - 1.4 [Current Challenges](#14-current-challenge)

2. [Current State Architecture Review](#2-current-state-architecture-review)
   - 2.1 [Current Architecture Diagram](#21-current-architecture-diagram)
   - 2.2 [Data Sources Analysis](#22-data-sources-analysis)
   - 2.3 [Target System (QPD)](#23-target-system-qpd)
   - 2.4 [Consumption Analysis](#24-consumption-analysis)
   - 2.5 [Current State Pain Points](#25-current-state-pain-points)

3. [Guiding Principles](#3-guiding-principles)

4. [Future State Architecture Diagram](#4-future-state-architecture-diagram)
   - 4.1 [Conceptual Architecture](#41-conceptual-architecture)
   - 4.2 [Detailed Architecture Components](#42-detailed-architecture-components)
     - 4.2.1 [Storage Layer](#421-storage-layer)
     - 4.2.2 [Ingestion Layer (EL)](#422-ingestion-layer-el)
       - [DARE Data Source](#dare-data-source)
       - [Illion Data Source](#illion-data-source)
       - [ACES Data Source](#aces-data-source)
       - [GDW Data Source](#gdw-data-source)
       - [Omnia Data Source](#omnia-data-source)
       - [CSV Files Data Source](#csv-files-data-source)
       - [AI Models Data Source](#ai-models-data-source)
     - 4.2.3 [Transformation Layer (T)](#423-transformation-layer-t)
     - 4.2.4 [Consumption Layer](#424-consumption-layer)
     - 4.2.5 [Orchestration](#425-orchestration)
   - 4.3 [Detailed Component Mapping](#43-detailed-component-mapping)

5. [Security, Governance, and Operations](#5-security-governance-and-operations)
   - 5.1 [Data Governance](#51-data-governance)
   - 5.2 [Monitoring & Alerting Capabilities](#52-monitoring--alerting-capabilities)

6. [Use Case Scenarios and Architecture Application](#6-use-case-scenarios-and-architecture-application)
   - 6.1 [Smart Mini Data Load (DARE → Alteryx → QPD → Tableau)](#61-smart-mini-data-load-dare--alteryx--qpd--tableau)
   - 6.2 [Illion Bureau Data Load](#62-illion-bureau-data-load)
   - 6.3 [Direct Debit Monitoring Tool](#63-direct-debit-monitoring-tool)
   - 6.4 [Watchlist Integration](#64-watchlist-integration)
   - 6.5 [Cashflow Model Output](#65-cashflow-model-output)
   - 6.6 [Customer Value Management (CVM) Insights to Service Domain](#66-customer-value-management-cvm-insights-to-service-domain)
   - 6.7 [BB Data Quality Platform](#67-bb-data-quality-platform)

7. [Document Information](#document-information)

---

## Introduction and Executive Summary

### 1.1 Background

To align with our cloud-first strategy and simplify our data platforms, we are gradually transitioning away from legacy systems such as Teradata. While Teradata has served multiple teams effectively over the years, it has become increasingly expensive and less compatible with modern cloud-native architectures.

As an initial step, we successfully migrated Teradata to AWS. However, a significant portion of the ongoing cost is attributed to sandpit environments, particularly QPD sandpits, which are widely used for testing and analysis across teams. These environments consume substantial storage and compute resources, and many are either unmanaged or no longer actively used.

To address this, we are initiating a phased migration of QPD sandpit workloads to more efficient platforms such as Snowflake and AWS-native services. This transition is expected to reduce operational costs, enhance data security, and streamline platform governance.

The current QPD (Quantitative Portfolio Decisions) system architecture includes:

**Data Sources:**
- SQL Database (DARE)
- Illion Files
- ACES Watchlist Entries
- CSV Files
- Teradata (GDW)
- Parquet Files (Omnia)
- AI Models (Sagemaker)

**Transformation Tools:**
- Alteryx for data preparation and blending
- SQL Scripts for custom transformations
- SSIS for data integration
- R-Connect for statistical analysis and data movement

This fragmented approach, combined with the legacy Teradata infrastructure, has created significant challenges including high total cost of ownership (TCO), performance bottlenecks, scalability limitations, maintenance complexity, and limited support for modern analytics workloads.

### 1.2 Purpose of this Document

This document defines the target future state architecture for QPD, outlining the migration from the current Teradata-based system to a modern cloud data platform. It establishes the required platform capabilities, data flow architecture, and quantifiable business benefits. Additionally, this document translates and maps the use cases validated in the Proof of Concept (PoC) to the future state architecture, demonstrating how these scenarios will be supported in the target Snowflake environment. The document serves as a blueprint for stakeholders to understand the transformation scope, approach, and expected outcomes.

### 1.3 Executive Summary

The proposed solution involves migrating QPD to Snowflake Cloud Data Platform, implementing a modern ELT (Extract, Load, Transform) approach that leverages cloud-native capabilities. The transformation will retire legacy systems including Alteryx, SSIS, and R-Connect for core data movement, replacing them with Snowflake-native features, dbt for transformations, and Fivetran for data integration. This approach will deliver significant cost savings, improved performance, enhanced scalability, and enable self-service analytics capabilities.

### 1.4 Current Challenges

The existing architecture faces several critical challenges:

- **Data Accumulation**: Many sandpits contain years of historical data that users rely on for continuity. Snowflake's architecture requires a clear strategy for migrating this data while preserving analytical workflows
- **Cross-Domain Complexity**: Sandpit datasets often span multiple domains (e.g., Consumer Finance, Customer Service, Wealth). Snowflake's domain-aligned governance model necessitates careful segmentation and integration planning
- **Functional Dependency**: Sandpit workflows are tightly coupled with historical data. Migrating without this context risks disrupting business-critical insights and reporting
- **Data Ownership and Stewardship**: Teradata sandpits lack federated ownership. Snowflake's governance framework requires clearly defined data stewardship to support access controls, lineage, and accountability
- **Consumer Enablement**: Analytical consumers including CEE, Tableau users, and analysts expect seamless access to data in the new platform. Snowflake must support these consumption patterns without compromising performance or governance

---

## 2. Current State Architecture Review

### 2.1 Current Architecture Diagram

```mermaid
graph TB
    subgraph "Data Sources"
        A[SQL Database - DARE]
        B[Illion Files]
        C[ACES Watchlist Entries]
        D[CSV Files]
        E[Teradata - GDW]
        F[Parquet Files - Omnia]
        G[AI Models - Sagemaker]
    end
    
    subgraph "Transformation Layer"
        H[Alteryx]
        I[SQL Scripts]
        J[SSIS]
        K[R-Connect]
    end
    
    subgraph "Target"
        L[Teradata QPD]
    end
    
    subgraph "Consumption"
        M[Tableau Dashboards]
        N[APIs]
        O[R-Connect Analytics]
        P[Analytical Reporting]
        Q[AI Models]
    end
    
    A --> H
    B --> H
    B --> I
    C --> I
    D --> J
    E --> K
    F --> K
    G --> L
    
    H --> L
    I --> L
    J --> L
    K --> L
    
    L --> M
    L --> N
    L --> O
    L --> P
    L --> Q
```

### 2.2 Data Sources Analysis

| **Data Source** | **Format** | **Transformation Tool** |
|-----------------|------------|-------------------------|
| SQL Database (DARE) | Relational | Alteryx |
| Illion Files | [Format] | Alteryx + SQL Scripts |
| ACES Watchlist Entries | [Format] | SQL Scripts |
| CSV Files | CSV | SSIS |
| Teradata (GDW) | Relational | R-Connect |
| Parquet Files (Omnia) | Parquet | R-Connect |
| AI Models (Sagemaker) | Model Outputs | Direct Load |

### 2.3 Target System (QPD)

QPD serves as an analytical sandpit built on Teradata technology, designed to facilitate a wide range of production reporting, analytics, and control functions. Numerous key processes, including NBCs and Model Score calculations, depend on the comprehensive and reliable data available within the QPD Sandpit environment.

### 2.4 Consumption Analysis

Current downstream consumers include:

- **Tableau Dashboards**
- **APIs**
- **R-Connect Analytics**
- **Analytical Reporting**
- **AI Models**

### 2.5 Current State Pain Points

<!-- - **Performance Issues**: Query performance bottlenecks and resource contention
- **Scalability Limitations**: Fixed infrastructure unable to handle growing data volumes and user demands
- **Maintenance Complexity**: Multiple disparate tools requiring specialized expertise and complex coordination
- **Cost Concerns**: High licensing costs, infrastructure overhead, and limited cost optimization capabilities
- **Technology Debt**: Legacy systems constraining innovation and preventing adoption of modern analytics capabilities
- **Data Quality Issues**: Inconsistent governance frameworks and quality controls across multiple transformation tools -->

- **Diverse Platforms**: Multiple platforms make it difficult to sustain a common skillset amongst staff
- **Coupled Compute and Storage**: Fixed infrastructure architecture limiting independent scaling and cost optimization
- **Technology Fragmentation**: Different technologies being used for ingestion and transformation creating operational complexity
- **Cross-Domain Data Access**: Data residing in different domains creates overhead to pull into QPD

---

## 3. Guiding Principles

- **Cloud-Native**: Prioritize fully managed, scalable cloud services that eliminate infrastructure management overhead
- **ELT First**: Favor Extract, Load, Transform approach leveraging cloud data warehouse compute power over traditional ETL
- **Decoupled Compute and Storage**: Ensure performance optimization and cost efficiency through independent scaling
- **Self-Service**: Enable easier data access and analytics capabilities for business users, analysts, and data scientists
- **Data Governance**: Incorporate security, data quality, and lineage tracking by design across all data flows
- **Scalability**: Design for elastic scalability to handle varying workloads and data volumes
- **Cost Efficiency**: Optimize for cost-effective operations with usage-based pricing models
- **Real-time Capabilities**: Support both real-time streaming and batch processing requirements
- **Platform Unification**: Consolidate disparate tools and systems into a unified Snowflake-based platform to streamline data ingestion, transformation, and analytics capabilities

---

## 4. Future State Architecture Diagram

### 4.1 Conceptual Architecture

```mermaid
graph LR
    subgraph "Data Sources"
        A1[SQL Database - DARE]
        A2[Illion Files]
        A3[ACES Watchlist Entries]
        A4[CSV Files]
        A5[Teradata - GDW]
        A6[Parquet Files - Omnia]
        A7[AI Models - Sagemaker]
    end
    
    subgraph "Ingestion Layer"
        B1[Cloud Data Integration Service]
        B2[Streaming Ingestion]
        B3[Batch Ingestion]
        B4[API Gateway]
    end
    
    subgraph "Storage Layer"
        C1[Raw Data Lake]
        C2[Curated Data Lake]
        C3[Data Warehouse]
        C4[Feature Store]
    end
    
    subgraph "Processing Layer"
        D1[ETL/ELT Engine]
        D2[Stream Processing]
        D3[ML Pipeline]
        D4[Data Quality]
    end
    
    subgraph "Consumption Layer"
        E1[BI Dashboards]
        E2[API Services]
        E3[Analytics Workbench]
        E4[ML Models]
        E5[Data Products]
    end
    
    subgraph "Governance & Management"
        F1[Data Catalog]
        F2[Lineage Tracking]
        F3[Security & Access Control]
        F4[Monitoring & Alerting]
    end
    
    A1 --> B1
    A2 --> B2
    A3 --> B3
    A4 --> B3
    A5 --> B1
    A6 --> B2
    A7 --> B4
    
    B1 --> C1
    B2 --> C2
    B3 --> C3
    B4 --> C4
    
    C1 --> D1
    C2 --> D2
    C3 --> D3
    C4 --> D4
    
    D1 --> E1
    D2 --> E2
    D3 --> E3
    D4 --> E4
    E1 --> E5
    
    F1 --> C1
    F1 --> C2
    F1 --> C3
    F2 --> D1
    F2 --> D2
    F3 --> E1
    F3 --> E2
    F4 --> D1
    F4 --> D2
```

### 3.2 Detailed Architecture Components

#### 3.2.1 Storage Layer

The architecture implements a two-database approach:
1. **QPD Database**: Internal usage with standard managed schemas for raw and transformed data
2. **QPD Catalog Linked Database**: External catalog integration for gold layer data products

```mermaid
graph TB
    subgraph "QPD Database"
        subgraph "Landing Layer"
            subgraph "External Landing"
                S3[AWS S3 Bucket<br/>Automated Sources]
            end
            subgraph "Internal Landing"
                SF_Stage[Snowflake Internal Stage<br/>Manual Sources]
            end
        end
        
        subgraph "Raw Data Zone (Bronze)"
            Bronze[Native Snowflake Tables<br/>Schema-on-read]
        end
        
        subgraph "Curated Data Zone (Silver)"
            Silver[Native Snowflake Tables<br/>Cleansed & Standardized]
        end
    end
    
    subgraph "QPD Catalog Linked Database"
        subgraph "Data Warehouse (Gold)"
            Gold[Externally Managed Iceberg Tables<br/>AWS Glue Catalog]
        end
    end
    
    S3 --> Bronze
    SF_Stage --> Bronze
    Bronze --> Silver
    Silver --> Gold
```

**Landing Layer:**
- **Purpose**: Landing zone for data sources to push data files to QPD for ingestion
- **Implementation**: Two-tier landing architecture supporting different data ingestion patterns
- **Storage Types**: 
  1. **External Landing (AWS S3 Bucket)**: For externally pushed files from automated data sources
  2. **Internal Landing (Snowflake Internal Stage)**: For manually submitted data files (e.g., ACES watchlist entries)
- Provides secure, scalable file storage with lifecycle management policies and flexible ingestion capabilities

```mermaid
graph LR
    subgraph "Landing Layer"
        subgraph "External Landing"
            S3[AWS S3 Bucket]
        end
        subgraph "Internal Landing" 
            SF_Stage[Snowflake Internal Stage]
        end
    end
    
    subgraph "Data Sources"
        Auto[Automated Sources<br/>Illion, DARE, etc.]
        Manual[Manual Sources<br/>ACES Watchlist]
    end
    
    subgraph "Raw Data Zone"
        Bronze[Bronze Tables<br/>QPD Database]
    end
    
    Auto --> S3
    Manual --> SF_Stage
    S3 --> Bronze
    SF_Stage --> Bronze
```

**Raw Data Zone (Bronze):**
- **Purpose**: Landing zone for raw, unprocessed data in original formats with schema-on-read approach for maximum flexibility and data preservation
- **Implementation**: Schema in QPD database that receives data from both Landing Layer sublayers: external stage pointing to AWS S3 bucket for automated sources, and Snowflake internal stage for manual file submissions
- **Table Types**: Native Snowflake tables
- Cost-effective storage with automated lifecycle management

**Curated Data Zone (Silver):**
- **Purpose**: Cleansed and standardized data with enforced schema and quality rules, optimized for downstream consumption with improved query performance
- **Implementation**: Schema of native tables within QPD database for business rule applications and data enrichment processes
- **Table Types**: Native Snowflake tables
- Supports complex transformations and data quality validations

**Data Warehouse (Gold):**
- **Purpose**: Business-ready analytical data models optimized for specific use cases with dimensional modeling and aggregated datasets for reporting and analytics
- **Implementation**: Resides on catalog linked database enabling external ecosystem integration and advanced analytics workflows
- **Table Types**: Externally managed Iceberg tables on AWS Glue catalog linked database
- High-performance compute resources for complex analytical workloads and cross-platform data sharing

#### 3.2.2 Ingestion Layer (EL)

The ingestion layer supports three different types of data ingestion requirements:
- **Manual**: User-driven data uploads requiring human intervention
- **Periodic**: Scheduled data ingestion on regular intervals (daily, weekly, monthly)
- **Real-time**: Immediate data availability without traditional ingestion processes

The selection of appropriate ingestion methodology for each data source depends on several key factors including the nature of the data source, the variety of data types it contains, and its specific periodicity requirements. Different sources require tailored approaches to ensure optimal performance, reliability, and alignment with business requirements.

```mermaid
graph TB
    subgraph "Data Sources & Ingestion Methods"
        subgraph "Periodic Sources"
            DARE[DARE SQL Database<br/>→ Alteryx Repointing]
            Illion[Illion Files<br/>→ Alteryx Repointing]
            CSV[CSV Files<br/>→ SSIS Repointing]
            AI[AI Models SageMaker<br/>→ Direct Repointing]
        end
        
        subgraph "Manual Sources"
            ACES[ACES Watchlist<br/>→ Streamlit UI App]
        end
        
        subgraph "Immediately Accessible"
            GDW[GDW Tables<br/>→ Glue Catalog Linked]
            Omnia[Omnia Parquet<br/>→ External Tables]
        end
    end
    
    subgraph "Target Layers"
        Landing[AWS S3 Landing Layer]
        Raw[Snowflake QPD Raw Layer]
        Direct[Direct Access<br/>No Ingestion Required]
    end
    
    DARE --> Raw
    Illion --> Landing
    CSV --> Landing
    AI --> Raw
    ACES --> Raw
    Landing --> Raw
    GDW --> Direct
    Omnia --> Direct
```

##### DARE Data Source
**Type**: Periodic

**Ingestion Options**:


**Option 1: Alteryx Repointing**
```mermaid
graph LR
    DARE[DARE SQL Database] --> Alteryx[Alteryx Workflow]
    Alteryx --> SF_Raw[Snowflake QPD Raw Layer]
```

**Option 2: OpenFlow Integration**
```mermaid
graph LR
    DARE[DARE Azure SQL] --> OpenFlow[OpenFlow ETL]
    OpenFlow --> SF_Raw[Snowflake QPD Raw Layer]
```

##### Illion Data Source
**Type**: Periodic
**Ingestion Approach**: Repoint Alteryx to write to AWS S3 External Landing layer, then copy to Snowflake QPD raw layer

```mermaid
graph LR
    Illion[Illion Files] --> Alteryx[Alteryx Workflow]
    Alteryx --> S3_Landing[AWS S3 External Landing]
    S3_Landing --> SF_Raw[Snowflake QPD Raw Layer]
```

##### ACES Data Source
**Type**: Manual
**Implementation**: Streamlit application for user file uploads

```mermaid
graph LR
    User[Business User] --> Streamlit[Streamlit UI App]
    Streamlit --> SF_Internal[Snowflake Internal Stage<br/>Landing Layer]
    SF_Internal --> Snowpipe[Snowflake Pipe Objects]
    Snowpipe --> SF_Raw[Snowflake QPD Raw Layer]
```

Upon file upload and submission through the Streamlit interface, files are automatically copied into the Snowflake Internal Stage in the landing layer. From there, Snowflake Pipe objects detect when a file has been added and automatically copy it into the Snowflake QPD Raw Layer table for subsequent processing.

##### GDW Data Source
**Type**: Immediately accessible (no ingestion needed)
**Implementation**: As part of the greenfield initiative, GDW tables will be available as AWS Glue catalog linked externally managed Iceberg tables. QPD only needs to raise a request to access required tables. Per the HLSA design for greenfield, GDW externally managed Iceberg tables will be created in the requestor's database (QPD), pointing to the correct storage location in GDW.

##### Omnia Data Source
**Type**: Immediately accessible (no ingestion needed)
**Implementation**: As part of the greenfield initiative, Omnia tables (OTC parquet files) will be available as external tables pointing to their current AWS S3 location. QPD only needs to raise a request to access required tables.

##### CSV Files Data Source
**Type**: Periodic
**Ingestion Options**:

**Option 1: SSIS Repointing**
```mermaid
graph LR
    CSV[CSV Files] --> SSIS[SSIS Package]
    SSIS --> S3_Landing[AWS S3 External Landing]
    S3_Landing --> SF_Raw[Snowflake QPD Raw Layer]
```

**Option 2: OpenFlow Integration**
```mermaid
graph LR
    CSV[CSV Files Location] --> OpenFlow[OpenFlow ETL]
    OpenFlow --> S3_Landing[AWS S3 External Landing]
    S3_Landing --> SF_Raw[Snowflake QPD Raw Layer]
```

**Option 3: S3 External Landing**
```mermaid
graph LR
    CSV[CSV Files] --> S3_Landing[AWS S3 External Landing]
    S3_Landing --> SF_Raw[Snowflake QPD Raw Layer]
```

##### AI Models Data Source
**Type**: Periodic
**Implementation**: Repoint AWS SageMaker output to write directly to Snowflake QPD raw layer instead of Teradata

```mermaid
graph LR
    SageMaker[AWS SageMaker] --> SF_Raw[Snowflake QPD Raw Layer]
```

#### 3.2.3 Transformation Layer (T)

**ELT Tool/Framework:**
- **dbt (Data Build Tool)**: Primary transformation framework for SQL-based data modeling
- **Snowflake SQL**: Native stored procedures and functions for complex business logic
- **Python/Scala**: Custom transformations using Snowflake's native programming capabilities

**QPD Structure:**
- **Bronze/Silver/Gold Architecture**: Layered approach ensuring data quality progression
- **Data Vault Modeling**: Scalable and auditable data warehouse design for historical tracking
- **Dimensional Models**: Star schema design optimized for analytical queries and reporting

#### 3.2.4 Consumption Layer

**Tableau/Reporting:**
- Direct connectivity to Snowflake with native optimization and caching
- Self-service analytics capabilities with governed data access
- Real-time dashboard updates and interactive exploration

**APIs:**
- **Snowflake SQL API**: Direct database connectivity for application integration
- **REST API Gateway**: Service layer for external application access with proper authentication and rate limiting
- **GraphQL Endpoints**: Flexible data querying for modern application architectures

**AI Models/Data Science:**
- **Snowflake Notebooks**: Integrated Jupyter-style environment for data science workflows
- **Hex/External Notebooks**: Integration with external data science platforms
- **MLOps Pipeline**: Automated model training, validation, and deployment workflows
- **Feature Store**: Centralized repository for ML features with versioning and lineage

#### 3.2.5 Orchestration

**Snowflake Tasks:**
- Scheduled job execution for automated data pipeline operations
- Native task scheduling for data ingestion, transformation, and quality checks
- Task dependency management for complex workflow orchestration
- Automated retry mechanisms and error handling
- Resource optimization through intelligent task scheduling

### 3.3 Detailed Component Mapping

| **Current Tool/System** | **Snowflake Capability** | **Migration Approach** |
|------------------------|---------------------------|------------------------|
| Alteryx | dbt + Snowflake SQL + Python | Workflow conversion and optimization |
| SSIS | Fivetran + Snowflake Connectors | ETL package migration to ELT patterns |
| R-Connect | Snowflake R Integration + Notebooks | R script modernization and cloud execution |
| Teradata QPD | Snowflake Data Warehouse | Direct migration with performance optimization |
| SQL Scripts | Snowflake SQL + Stored Procedures | Code conversion and cloud optimization |
| File Processing | Snowflake Stages + Tasks | Automated file ingestion and processing |

---

## 5. Security, Governance, and Operations

### 5.1 Data Governance

**Metadata Management:**
- **Data Catalog**: Automated discovery and cataloging using Snowflake Information Schema
- **Business Glossary**: Centralized definitions and business context for data assets
- **Data Classification**: Automated sensitive data discovery and classification

**Data Quality:**
- **Quality Checks**: Automated data validation rules and quality scorecards
- **Data Profiling**: Continuous monitoring of data distribution and anomaly detection
- **Quality Metrics**: Business-defined KPIs for data quality measurement and reporting

**Lineage Tracking:**
- **End-to-End Lineage**: Complete traceability from source systems to consumption points
- **Impact Analysis**: Understanding of downstream effects for data model changes
- **Compliance Reporting**: Automated generation of lineage reports for regulatory requirements

### 5.2 Monitoring & Alerting Capabilities

**Monitoring:**
- **Performance Monitoring**: Real-time query performance and resource utilization tracking
- **Cost Monitoring**: Granular cost allocation and chargeback capabilities by department/project
- **Data Quality Monitoring**: Continuous validation of data freshness, completeness, and accuracy

**Cost Management/Optimization:**
- **Auto-Scaling**: Dynamic compute resource scaling based on workload demands
- **Resource Scheduling**: Automated warehouse suspension and resumption based on usage patterns
- **Cost Allocation**: Detailed cost tracking and optimization recommendations by business unit

**Performance Tuning:**
- **Query Optimization**: Automated query performance analysis and optimization recommendations
- **Clustering**: Intelligent data clustering for improved query performance
- **Caching**: Result set caching and materialized views for frequently accessed data

---

## 6. Use Case Scenarios and Architecture Application

### 6.1 Smart Mini Data Load (DARE → Alteryx → QPD → Tableau)

**Use Case:** Smart Mini Data Load used for merchant migration and mobile user analysis, this flow transforms DARE SQL Server data via Alteryx and loads it into QPD for Tableau dashboarding. It supports weekly refreshes and validation of transactional counts.

**Current State:**
- **Source:** DARE SQL Server tables
- **Tool:** Alteryx workflows
- **Target:** QPD tables
- **Output:** Tableau dashboards for merchant migration and mobile user analysis
- **Load Frequency:** Weekly

**Future State Architecture:**
- **Ingestion:** Fivetran connector for DARE SQL Server with automated CDC
- **Transformation:** dbt models replacing Alteryx workflows for data preparation
- **Storage:** Snowflake tables with optimized clustering for query performance
- **Consumption:** Tableau connected directly to Snowflake with live connectivity
- **Benefits:** Reduced processing time, automated data quality checks, real-time insights

### 6.2 Illion Bureau Data Load

**Use Case:** Monthly Credit Bureau Reporting - Monthly bureau files from Illion are ingested and transformed using SQL and Alteryx, then loaded into QPD for credit risk dashboards. This supports regulatory and financial insights with ~90K records per month.

**Current State:**
- **Source:** Illion files - External
- **Tool:** SQL + Alteryx
- **Target:** ILLION_TXN_DATA_LOAD in QPD
- **Output:** Tableau dashboards for bureau-level insights
- **Size Estimate:** ~79,985 to 89,023 records per month
- **Load Frequency:** Monthly

**Future State Architecture:**
- **Ingestion:** Snowflake External Stages with automated file detection via Snowpipe
- **Transformation:** dbt models for data cleansing and business rule application
- **Storage:** Snowflake tables with time-travel capabilities for audit compliance
- **Consumption:** Enhanced Tableau dashboards with real-time refresh capabilities
- **Benefits:** Automated processing, improved data lineage, reduced manual intervention

### 6.3 Direct Debit Monitoring Tool

**Use Case:** DDMT (Direct Debit Monitoring Tool) is a standalone internal tool used by frontline teams to monitor direct debit facility utilisation, identify breaches, and support annual reviews. It consumes data loaded into QPD tables via SSIS packages, which processes encrypted APCA names, CommBiz limits, and claims data from shared folders.

**Current State:**
- **Source:** Raw files (claims data, APCA names, DD limits) manually placed in shared folders
- **Tool:** SSIS package loads data into QPD tables
- **Target:** QPD tables used by DDMT tool
- **Output:** DDMT for monitoring and annual review support
- **Load Frequency:** Manual/periodic uploads

**Future State Architecture:**
- **Ingestion:** Snowflake External Stages with automated file processing via Tasks
- **Transformation:** dbt models for data validation and encryption handling
- **Storage:** Secure Snowflake tables with row-level security and audit logging
- **Consumption:** Modernized DDMT tool with direct Snowflake connectivity or API layer
- **Benefits:** Automated file processing, enhanced security, improved audit capabilities

### 6.4 Watchlist Integration

**Use Case:** The Watchlist Integration supports the Customer Experience Engine (CEE) by enabling conflict checks and risk classification workflows. The ACES Watchlist, a critical component of credit risk oversight, is manually loaded into QPD to support consolidated reporting and downstream analytics.

**Current State:**
- **Source:** ACES Watchlist entries manually curated by business units and submitted monthly
- **Tool:** Files received from ACES team, validated and manually loaded into QPD by BB Data Office
- **Target:** QPD tables for consolidated reporting
- **Output:** Monthly Watchlist Reports for credit risk teams and business units to monitor flagged borrowers, trigger escalations to Group Credit Structuring (GCS), and support regulatory and internal audit requirements
- **Load Frequency:** Weekly

**Future State Architecture:**
- **Ingestion:** Automated API integration with ACES system or secure file transfer to Snowflake stages
- **Transformation:** dbt models for data validation, conflict resolution, and business rule application
- **Storage:** Snowflake tables with secure access controls and complete audit trail
- **Consumption:** Real-time dashboards and automated alerting for risk teams, API integration with CEE
- **Benefits:** Reduced manual processing, real-time risk monitoring, enhanced compliance capabilities

### 6.5 Cashflow Model Output

**Use Case:** The CVM Cashflow Forecast initiative is part of the broader MEP (Model Execution Pipeline) framework. It aims to operationalise credit and debit cashflow forecasting models using AWS SageMaker, Glue ETL, and Teradata QPD. These forecasts are consumed by platforms like Bankers Workbench (BWB) to support customer engagement and advisory.

**Current State:**
- **Tables:** Final table for structured credit forecasts & Final table for structured debit forecasts
- **Tool:** QPD for table creation, testing, and structured storage; AWS SageMaker for model scoring and output generation; AWS Glue for ETL and writeback to Teradata; GitHub for managing config.yaml and container PRs
- **Output:** Structured cashflow forecasts with quantile predictions (10%, 50%, 90%) for credit and debit transactions
- **Enables:** Cashflow shortfall alerts, Predictive advisory & Integration into Bankers Workbench for customer engagement
- **Load Frequency:** Staging Tables loaded when new SageMaker outputs are available; Final Tables auto-refreshed post-staging via Glue ETL; ETL Runtime ~40 minutes per batch; Scoring Frequency: Weekly or model-triggered, aligned with retraining cycles

**Future State Architecture:**
- **ML Pipeline:** Snowflake native ML capabilities with Snowpark for model execution and scoring
- **Data Storage:** Snowflake tables with native support for semi-structured data and time-travel capabilities
- **Transformation:** dbt models for data processing and quantile calculations replacing Glue ETL
- **Integration:** Direct API connectivity to Bankers Workbench eliminating complex ETL processes
- **Benefits:** Simplified architecture, reduced latency, enhanced model monitoring, improved scalability

### 6.6 Customer Value Management (CVM) Insights to Service Domain

**Use Case:** CVM (Customer Value Management) delivers actionable insights such as payaway patterns, predicted needs, and customer engagement scores from QPD to the Service Domain (SD). These insights are consumed by downstream participants like Bankers Workbench (BWB) to support personalised customer engagement and AI model feedback loops.

**Current State:**
- **Source:** QPD tables in the BB Datamart
- **Tool:** API layer extracts data from QPD and pushes it into Aurora DB within the CBI Service Domain. Job status checks are in place to monitor successful delivery
- **Target:** Aurora DB in the CBI Service Domain, which feeds into the Banker Workbench (BWB) and other downstream systems
- **Output:** Top 20 CVM insights, historical engagement patterns, and payaway behaviour used by bankers to drive customer conversations and engagement strategies
- **Load Frequency:** Weekly refresh of CVM insights to ensure recency and relevance for customer interactions

**Future State Architecture:**
- **Data Sharing:** Snowflake secure data sharing capabilities eliminating complex API extractions
- **Real-time Processing:** Snowflake Streams for real-time insight generation and updates
- **Integration:** Direct Snowflake connectivity to Service Domain applications via native connectors
- **Analytics:** Enhanced CVM scoring models using Snowflake's native ML functions
- **Benefits:** Real-time insights, simplified data pipeline, improved customer experience, enhanced model feedback loops

### 6.7 BB Data Quality Platform

**Use Case:** Data Quality Tool aims to offer the following business outcomes for Business Banking: ability to assess data by data producers and data stewards using self-service functionality; expose data with issues for remediation to frontline, data producers and data stewards; track and monitor data quality in Omnia and GDW v2.

**Current State:**
- **Source:** GDW Sandpit (1 table), Omnia Sandpit (2 tables)
- **Tool:** Scoop (for data movement), SMTP Email Relay (for notifications), Tableau & R-Shiny Tool (for visualisation and remediation)
- **Target:** GDW Target Tables, Omnia Target Tables: XYZ
- **Output:** DQ Exception records with metadata and context, Email notifications to RMs and Data Stewards, Dashboards with RAG status, Exception counts & Drill-down capabilities, R-Shiny portal for remediation with row-level security
- **Load Frequency:** Daily

**Future State Architecture:**
- **Data Quality Framework:** Snowflake native data quality functions and Great Expectations integration
- **Monitoring:** Automated data quality scoring with Snowflake Tasks and alerting capabilities
- **Visualization:** Modern Tableau dashboards with real-time data quality metrics connected directly to Snowflake
- **Self-Service:** Snowflake's Information Schema and Account Usage views for data steward access
- **Benefits:** Automated quality monitoring, real-time exception detection, simplified remediation workflows, enhanced data governance

---

## Document Information

| **Attribute** | **Details** |
|---------------|-------------|
| **Document Title** | QPD Future State Architecture: Gem Outline |
| **Version** | 1.0 |
| **Date** | November 2024 |
| **Target Platform** | Snowflake Cloud Data Platform |
| **Migration Approach** | Phased ELT-First Architecture |

