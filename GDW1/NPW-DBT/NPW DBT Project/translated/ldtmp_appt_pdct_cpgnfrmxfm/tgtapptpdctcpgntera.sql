{{ config(materialized='incremental', alias='tmp_appt_pdct_cpgn', incremental_strategy='append', tags=['LdTMP_APPT_PDCT_CPGNFrmXfm']) }}

SELECT
	APPT_PDCT_I
	CPGN_TYPE_C
	CPGN_I
	REL_C
	SRCE_SYST_C
	EFFT_D
	EXPY_D
	RUN_STRM 
FROM {{ ref('SrcApptPdctCpgnDS') }}