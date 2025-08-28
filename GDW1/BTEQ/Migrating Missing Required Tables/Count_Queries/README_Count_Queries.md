# Count Queries for CBA Missing Tables

This directory contains count queries for Teradata tables corresponding to the Snowflake Iceberg tables created in `CBA01.1 - Missing Tables.sql`.

## Overview

Each file contains a `count(1)` query for the Teradata counterpart of the tables being migrated to Snowflake. These queries can be used to:

1. Verify data completeness after migration
2. Compare record counts between source (Teradata) and target (Snowflake) systems
3. Validate the migration process

## File Naming Convention

Files are named using the pattern: `{sequence_number}_{table_name}_count.sql`

## Table Mappings

The queries reference the original Teradata table names based on the mappings found in the comments of the original DDL file:

- `<sc-table>` comments show the original table structure
- `/* TD_TABLE -> SNOWFLAKE_TABLE */` comments show the mapping

## Usage

Execute these queries against your Teradata environment to get baseline counts before migration, then compare with counts from the corresponding Snowflake tables after migration.

## Important Note

Some tables may have evolved or been renamed in Teradata. Verify table names and schemas in your specific Teradata environment before executing these queries. 