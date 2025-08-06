{{ config(materialized='view', tags=['LdTMP_APPT_ORIGFrmXfm']) }}

WITH CC AS (
	SELECT
		CHNL_CAT_ID,
		APPT_ORIG_C,
		EFFT_D,
		EXPY_D
	FROM {{ ref('ds_MapCseApptOrig') }}
)

SELECT * FROM CC