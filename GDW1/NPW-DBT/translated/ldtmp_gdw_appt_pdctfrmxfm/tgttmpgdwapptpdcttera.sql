{{ config(materialized='incremental', alias='tmp_gdw_appt_pdct', incremental_strategy='append', tags=['LdTMP_GDW_APPT_PDCTFrmXfm']) }}

SELECT
	APPT_PDCT_I
	RELD_APPT_PDCT_I
	EXPY_FLAG 
FROM {{ ref('SrcApptPdctDS') }}