use role r_dev_npd_d12_gdwmig;
use database npd_d12_dmn_gdwmig;
use schema migration_tracking_v2;
use warehouse wh_usr_npd_d12_gdwmig_001;

//////////////////////////////////////////////// create transient table
-- create or replace TRANSIENT TABLE NPD_D12_DMN_GDWMIG.TMP.PDDSTG_DERV_ACCT_PATY_CHG_20250903 (
--             ACCT_I VARCHAR(16777216),
--             PATY_I VARCHAR(16777216),
--             ASSC_ACCT_I VARCHAR(16777216),
--             PATY_ACCT_REL_C VARCHAR(16777216),
--             PRFR_PATY_F VARCHAR(16777216),
--             SRCE_SYST_C VARCHAR(16777216),
--             EFFT_D DATE,
--             EXPY_D DATE,
--             ROW_SECU_ACCS_C NUMBER(10,0)
-- );
 
 
//////////////////////////////////////////////// step 1: insert mappings for new tables if the table didn't previously exist in dmva_mapping_rules for that same target table
    INSERT INTO MIGRATION_TRACKING_V2.dmva_mapping_rules(
    mapping_rule_id,
    source_system_name,
    source_database_name,
    source_schema_name,
    source_object_name,
    target_system_name,
    target_database_name,
    target_schema_name,
    target_object_name,
    source_object_id,
    target_object_id,
    created_ts,
    updated_ts
    )
    SELECT
    NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING_V2.dmva_mapping_rule_id_seq.nextval mapping_rule_id,
    'teradata_source' source_system_name,
    'NA' source_database_name,
    'K_PDDSTG' source_schema_name,
    'DERV_ACCT_PATY_CHG' source_object_name,
    'snowflake_target' target_system_name,
    'NPD_D12_DMN_GDWMIG' target_database_name,
    'TMP' target_schema_name,
    'PDDSTG_DERV_ACCT_PATY_CHG_20250903' target_object_name,
    NULL source_object_id,
    NULL target_object_id,
    current_timestamp() created_ts,
    current_timestamp() updated_ts;
   
    
//////////////////////////////////////////////// step 2: get metadta to populate dmva objects and dmva column info
-- select * from migration_tracking_v2.dmva_object_info where object_name = 'DERV_ACCT_PATY_CHG' and schema_name = 'K_PDDSTG';
CALL NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING_V2.DMVA_GET_METADATA_TASKS(
    'teradata_source',
    parse_json('{"NA": {"' || 'K_PDDSTG' || '": ["' || 'DERV_ACCT_PATY_CHG' || '"]}}')
);
--wait in a loop until it finishes
--verify that it's done
select * from migration_tracking_v2.dmva_object_info where object_name = 'DERV_ACCT_PATY_CHG' and schema_name = 'K_PDDSTG';
 

//////////////////////////////////////////////// step 3: run dmva_create_sync_tables  to create targets and mappings
CALL MIGRATION_TRACKING_V2.dmva_create_sync_tables(
    true,
    false,
    'teradata_source',
    parse_json('{"NA": {"' || 'K_PDDSTG' || '": ["' || 'DERV_ACCT_PATY_CHG' || '"]}}')
);

--verify that it's done properly - below should return 1 record
select c.*, a.*
from MIGRATION_TRACKING_V2.dmva_source_to_target_mapping a
inner join migration_tracking_v2.dmva_object_info b on
        b.object_name = 'DERV_ACCT_PATY_CHG'
    and b.schema_name = 'K_PDDSTG'
    and b.object_id = a.source_object_id
inner join migration_tracking_v2.dmva_object_info c on
    c.object_id = a.target_object_id;

SELECT * FROM DMVA_OBJECT_INFO WHERE OBJECT_ID IN (3198,3199)
SELECT * FROM DMVA_OBJECT_INFO WHERE OBJECT_ID IN (3209,3199)
 
///step 4: FOR FRESH NEW MIGRATIONS based on a parameter to be passed in
delete from dmva_checksums where checksum_id in(
    SELECT checksum_id--b.database_name, b.schema_name, b.object_name,  a.*
    FROM dmva_checksums a
        inner join dmva_object_info b on
            a.object_id = b.object_id
        and a.object_id in (3213,3214,3198,3200)
    -- ORDER BY CREATED_TS DESC
);
SELECT count(1) FROM NPD_D12_DMN_GDWMIG.TMP.PDDSTG_DERV_ACCT_PATY_CHG_20250903;
DELETE FROM NPD_D12_DMN_GDWMIG.TMP.PDDSTG_DERV_ACCT_PATY_CHG_20250903;
 
 
//////////////////////////////////////////////// step 5: run DMVA_GET_CHECKSUM_TASKS  to migrate data
CALL MIGRATION_TRACKING_V2.DMVA_GET_CHECKSUM_TASKS('teradata_source', parse_json('{"NA": {"' || 'K_PDDSTG' || '": ["' || 'DERV_ACCT_PATY_CHG' || '"]}}'));
