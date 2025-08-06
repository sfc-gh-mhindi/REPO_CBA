{{ config(materialized='view', tags=['XfmCC_APP_PRODFrmExt1']) }}

WITH OutApptPdctCpgnDs AS (
	SELECT
		APPT_PDCT_I as APPT_PDCT_I,
		CPGN_TYPE_C as CPGN_TYPE_C,
		CPGN_I as CPGN_I,
		REL_C as REL_C,
		SRCE_SYST_C as SRCE_SYST_C,
		EFFT_D as EFFT_D,
		EXPY_D as EXPY_D,
		RUN_STRM as RUN_STRM
	FROM {{ ref('XfmBusinessRules__OutTmpApptPdctCpgnDs1') }}
	UNION ALL
	SELECT
		APPT_PDCT_I,
		CPGN_TYPE_C,
		CPGN_I,
		REL_C,
		SRCE_SYST_C,
		EFFT_D,
		EXPY_D,
		RUN_STRM
	FROM {{ ref('XfmBusinessRules__OutTmpApptPdctCpgnDs2') }}
	UNION ALL
	SELECT
		APPT_PDCT_I,
		CPGN_TYPE_C,
		CPGN_I,
		REL_C,
		SRCE_SYST_C,
		EFFT_D,
		EXPY_D,
		RUN_STRM
	FROM {{ ref('XfmBusinessRules__OutTmpApptPdctCpgnDs3') }}
)

SELECT * FROM OutApptPdctCpgnDs