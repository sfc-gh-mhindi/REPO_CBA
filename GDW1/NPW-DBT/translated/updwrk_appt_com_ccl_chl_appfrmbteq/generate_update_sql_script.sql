{{ config(materialized='view', tags=['UpdWRK_APPT_COM_CCL_CHL_APPFrmBTEQ']) }}

WITH --Manual Task - Rowgenerator - Generate_UPDATE_SQL_Script

SELECT * FROM Generate_UPDATE_SQL_Script