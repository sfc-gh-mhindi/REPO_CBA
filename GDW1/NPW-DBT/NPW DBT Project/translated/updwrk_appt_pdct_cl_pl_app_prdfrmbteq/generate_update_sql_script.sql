{{ config(materialized='view', tags=['UpdWRK_APPT_PDCT_CL_PL_APP_PRDFrmBTEQ']) }}

WITH --Manual Task - Rowgenerator - Generate_UPDATE_SQL_Script

SELECT * FROM Generate_UPDATE_SQL_Script