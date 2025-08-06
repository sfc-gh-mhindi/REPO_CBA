{{ config(materialized='view', tags=['LdTMP_APPT_PRMO_TRAKFrmXfm']) }}

WITH 
____var__dbt__pinprocess_________var__dbt__pfile__name____ AS (
	SELECT
	*
	FROM {{ source("","____var__dbt__pinprocess_________var__dbt__pfile__name____")  }})
sq_ValidatedFile AS (
	SELECT RECD_TYPE,
		MOD_TIMESTAMP,
		CCL_APP_PROD_ID,
		TRACKING_ID
	FROM ____var__dbt__pinprocess_________var__dbt__pfile__name____
)

SELECT * FROM sq_ValidatedFile