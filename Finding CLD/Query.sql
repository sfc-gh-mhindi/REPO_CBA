
Mazen Hindi 
5:37 PM (0 minutes ago)
to me
show catalog integrations;
show storage integrations;
show external volumes;
 
describe catalog integration CATA_GLUE_CDAO_GDWMIG_TDSF_DEV_RW;
describe external volume EXTV_S3_CDAO_GDWMIG_TDSF_DEV_RW;
 
 
select * from snowflake.information_schema.databases;
select * from NPD_D12_DMN_GDWMIG_IBRG.information_schema.schemata;
select * from NPD_D12_DMN_GDWMIG_IBRG.information_schema.tables where
 
is_iceberg = 'YES'
 
show databases ;
show databases like 'NPD_D12_DMN_GDWMIG_IBRG'; --type
 
-- create or replace ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDBAL001STD0.BV_ES_PDS_TRAN COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 6,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"05/15/2025\",  \"domain\": \"snowflake\" }}'
 
show databases like 'NPD_D12_DMN_GDWMIG_IBRG'; --type
 
-- create or replace ICEBERG TABLE NPD_D12_DMN_GDWMIG_IBRG.PDBAL001STD0.BV_ES_PDS_TRAN COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 6,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"05/15/2025\",  \"domain\": \"snowflake\" }}'
 
-- EXTERNAL_VOLUME = 'EXTV_S3_CDAO_GDWMIG_TDSF_DEV_RW'
-- CATALOG = 'CATA_GLUE_CDAO_GDWMIG_TDSF_DEV_RW'
-- CATALOG_TABLE_NAME = 'BV_ES_PDS_TRAN'
-- CATALOG_NAMESPACE = 'PDBAL001STD0';
;