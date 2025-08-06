{{ config(materialized='view', tags=['UpdWRK_APPT_PDCT_FEATFrmBTEQ_IntRateAmtMargin']) }}

WITH --Manual Task - Rowgenerator - Generate_UPDATE_SQL_Script

SELECT * FROM Generate_UPDATE_SQL_Script