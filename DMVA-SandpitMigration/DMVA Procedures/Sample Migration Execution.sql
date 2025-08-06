use role r_dev_npd_d12_gdwmig;
use database npd_d12_dmn_gdwmig;
use schema migration_tracking;
use warehouse wh_usr_npd_d12_gdwmig_001;



create or replace ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_STG_001_STD_0.SANDPIT_DEMO_TBL001 (
    CODE1         NUMBER(38,0),
    CODE2         NUMBER(38,0),
    CODE3         NUMBER(38,0),
    DESC1         VARCHAR(134217728),
    DESC2         VARCHAR(134217728),
    DESC3         VARCHAR(134217728)
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'EXTV_S3_CDAO_GDWMIG_TDSF_DEV_RW'
  BASE_LOCATION = 'SANDPIT_DEMO';
 
create or replace ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_STG_001_STD_0.SANDPIT_DEMO_TBL002 (
    CODE1 NUMBER(38,0),
    DESC1 VARCHAR(134217728) ,
    DT1 DATE
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'EXTV_S3_CDAO_GDWMIG_TDSF_DEV_RW'
  BASE_LOCATION = 'SANDPIT_DEMO';

select * from "NPD_D12_DMN_GDWMIG_IBRG_V"."P_V_STG_001_STD_0"."SANDPIT_DEMO_TBL001";
select * from "NPD_D12_DMN_GDWMIG_IBRG_V"."P_V_STG_001_STD_0"."SANDPIT_DEMO_TBL002";


--without chunking
CALL P_MIGRATE_TERADATA_TABLE(
    'B_D52_D_TMP_001_STD_0', -- source_database_name (Teradata schema)
    'SANDPIT_DEMO_TBL001', -- source_table_name
    
    'NPD_D12_DMN_GDWMIG_IBRG_V', -- target_database_name
    'P_V_STG_001_STD_0', -- target_schema_name
    'SANDPIT_DEMO_TBL001' -- target_table_name
);

--with chunking
CALL P_MIGRATE_TERADATA_TABLE(
    'B_D52_D_TMP_001_STD_0', -- source_database_name (Teradata schema)
    'SANDPIT_DEMO_TBL001', -- source_table_name
    
    'NPD_D12_DMN_GDWMIG_IBRG_V', -- target_database_name
    'P_V_STG_001_STD_0', -- target_schema_name
    'SANDPIT_DEMO_TBL001', -- target_table_name
    
    'Y', -- with_chunking_yn (enable chunking)
    'CODE1', -- chunking_column
    '1000000', -- chunking_value
    'by_integer' -- chunking_data_type
);
 
CALL P_MIGRATE_TERADATA_TABLE(
    'B_D52_D_TMP_001_STD_0', -- source_database_name (Teradata schema)
    'SANDPIT_DEMO_TBL002', -- source_table_name
    
    'NPD_D12_DMN_GDWMIG_IBRG_V', -- target_database_name
    'P_V_STG_001_STD_0', -- target_schema_name
    'SANDPIT_DEMO_TBL002', -- target_table_name
    
    'Y', -- with_chunking_yn (enable chunking)
    'DT1', -- chunking_column
    'day', -- chunking_value
    'by_date' -- chunking_data_type
);

  
//extra: monitoring task while it runs:
select status_cd,*
from dmva_tasks
where 1=1
and queue_ts >= '2025-07-25 00:00:00.000'
order by queue_ts desc;

select * from "NPD_D12_DMN_GDWMIG_IBRG_V"."P_V_STG_001_STD_0"."SANDPIT_DEMO_TBL001";
select * from "NPD_D12_DMN_GDWMIG_IBRG_V"."P_V_STG_001_STD_0"."SANDPIT_DEMO_TBL002";

