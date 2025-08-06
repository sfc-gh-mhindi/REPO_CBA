{{ config(materialized='incremental', alias='tmp_hlm_app', incremental_strategy='append', tags=['LdTMP_HLM_APP_FrmXfm']) }}

SELECT
	APPT_I 
FROM {{ ref('TfmSrc') }}