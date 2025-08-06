{{ config(materialized='view', tags=['LdApptPrmoTrakUpd']) }}

WITH 
____var__dbt__poutbound_________var__dbt__prun__strm__c__________var__dbt__pctable__name__________var__dbt__prun__strm__pros__d______u AS (
	SELECT
	*
	FROM {{ source("","____var__dbt__poutbound_________var__dbt__prun__strm__c__________var__dbt__pctable__name__________var__dbt__prun__strm__pros__d______u")  }})
sq_ApptPrmoTrakUpd AS (
	SELECT APPT_PDCT_I,
		TRAK_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM ____var__dbt__poutbound_________var__dbt__prun__strm__c__________var__dbt__pctable__name__________var__dbt__prun__strm__pros__d______u
)

SELECT * FROM sq_ApptPrmoTrakUpd