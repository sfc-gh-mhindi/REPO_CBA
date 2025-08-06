{{ config(materialized='view', tags=['UpdWRK_APPT_PDCT_FEAT_HL_FEE_DISCFrmBTEQ']) }}

WITH --Manual Task - Rowgenerator - Generate_UPDATE_SQL_Script

SELECT * FROM Generate_UPDATE_SQL_Script