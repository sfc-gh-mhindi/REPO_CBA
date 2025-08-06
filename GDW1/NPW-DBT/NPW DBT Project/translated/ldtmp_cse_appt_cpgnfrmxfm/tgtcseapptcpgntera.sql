{{ config(materialized='incremental', alias='tmp_cse_appt_cpgn', incremental_strategy='append', tags=['LdTMP_CSE_APPT_CPGNFrmXfm']) }}

SELECT
	APPT_I
	CSE_CPGN_CODE_X
	EFFT_D
	RUN_STRM 
FROM {{ ref('SrcCseApptCpgnDS') }}