{{ config(materialized='view', tags=['DltAPPT_PDCT_RELFrmAPPT_PDCT']) }}

WITH FilterStage AS (
	SELECT
		APPT_PDCT_I,
		RELD_APPT_PDCT_I,
		APPT_PDCT_I,
		RELD_APPT_PDCT_I,
		EFFT_D
	FROM {{ ref('SrcApptPdctRelTera') }}
)

SELECT * FROM FilterStage