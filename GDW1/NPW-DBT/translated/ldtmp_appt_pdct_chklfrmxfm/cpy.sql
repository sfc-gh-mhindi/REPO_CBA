{{ config(materialized='view', tags=['LdTMP_APPT_PDCT_CHKLFrmXfm']) }}

WITH Cpy AS (
	SELECT
		APPT_PDCT_I,
		CHKL_ITEM_C,
		STUS_D,
		STUS_C,
		SRCE_SYST_C,
		CHKL_ITEM_X,
		RUN_STRM
	FROM {{ ref('TgtTmpApptPdctChklDS') }}
)

SELECT * FROM Cpy