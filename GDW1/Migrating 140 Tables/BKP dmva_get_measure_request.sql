CREATE OR REPLACE FUNCTION DMVA_GET_MEASURE_REQUEST("SYSTEM_TYPE" VARCHAR(16777216), "DATABASE_NAME" VARCHAR(16777216), "SCHEMA_NAME" VARCHAR(16777216), "OBJECT_NAME" VARCHAR(16777216), "COLUMNS" ARRAY, "WHERE_CLAUSE" VARCHAR(16777216) DEFAULT null, "DEFAULTS" OBJECT DEFAULT null, "SYSTEM_OVERRIDES" OBJECT DEFAULT null, "OBJECT_OVERRIDES" OBJECT DEFAULT null)
RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'run'
IMPORTS = (
    '@DMVA_PYTHON_CODE/system.py',
    '@DMVA_PYTHON_CODE/column.py',
    '@DMVA_PYTHON_CODE/const.py',
    '@DMVA_PYTHON_CODE/enums.py',
    '@DMVA_PYTHON_CODE/system_snowflake.py',
    '@DMVA_PYTHON_CODE/system_oracle.py',
    '@DMVA_PYTHON_CODE/system_teradata.py',
    '@DMVA_PYTHON_CODE/system_postgres.py',
    '@DMVA_PYTHON_CODE/system_netezza.py',
    '@DMVA_PYTHON_CODE/system_redshift.py',
    '@DMVA_PYTHON_CODE/system_sqlserver.py',
    '@DMVA_PYTHON_CODE/system_cloudera.py'
)
AS
$$
import const
from system import System

def run(system_type, database_name, schema_name, object_name, columns, where_clause, defaults, system_overrides, object_overrides):
    defaults, system_overrides, object_overrides = [ {} if x is None else x for x in [ defaults, system_overrides, object_overrides ] ]
    where_clause = '1 = 1' if where_clause is None else where_clause
    return { const.MeasureSqls: System.get_system(system_type, { **defaults, **system_overrides, **object_overrides }).get_measure_sqls(database_name, schema_name, object_name, columns, where_clause) }
$$;
