{{ config(materialized='incremental', alias='tmp_unid_paty_gnrc', incremental_strategy='append', tags=['LdTMP_UNID_PATY_GNRCFrmXfm']) }}

SELECT
	UNID_PATY_I
	RUN_STRM
	SRCE_SYST_PATY_I 
FROM {{ ref('Cpy') }}