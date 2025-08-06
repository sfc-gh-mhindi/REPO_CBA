{{ config(materialized='view', tags=['XfmPL_APP_PRODFrmExt']) }}

WITH OutApptPdctChklDS AS (
	SELECT
		APPT_PDCT_I as APPT_PDCT_I,
		CHKL_ITEM_C as CHKL_ITEM_C,
		STUS_D as STUS_D,
		STUS_C as STUS_C,
		SRCE_SYST_C as SRCE_SYST_C,
		CHKL_ITEM_X as CHKL_ITEM_X,
		RUN_STRM as RUN_STRM
	FROM {{ ref('XfmBusinessRules__OutApptPdctChklDS1') }}
	UNION ALL
	SELECT
		APPT_PDCT_I,
		CHKL_ITEM_C,
		STUS_D,
		STUS_C,
		SRCE_SYST_C,
		CHKL_ITEM_X,
		RUN_STRM
	FROM {{ ref('XfmBusinessRules__OutApptPdctChklDS2') }}
)

SELECT * FROM OutApptPdctChklDS