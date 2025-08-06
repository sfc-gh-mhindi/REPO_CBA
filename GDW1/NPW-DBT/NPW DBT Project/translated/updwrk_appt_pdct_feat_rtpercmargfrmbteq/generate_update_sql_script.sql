{{ config(materialized='view', tags=['UpdWRK_APPT_PDCT_FEAT_RTPERCMARGFrmBTEQ']) }}

WITH --Manual Task - Rowgenerator - Generate_UPDATE_SQL_Script

SELECT * FROM Generate_UPDATE_SQL_Script