{{ config(materialized='view', tags=['LdApptPrmoTrakIns']) }}

WITH 
____var__dbt__poutbound_________var__dbt__prun__strm__c__________var__dbt__pctable__name__________var__dbt__prun__strm__pros__d______i AS (
	SELECT
	*
	FROM {{ source("","____var__dbt__poutbound_________var__dbt__prun__strm__c__________var__dbt__pctable__name__________var__dbt__prun__strm__pros__d______i")  }})
sq_ApptPrmoTrakIns AS (
	SELECT APPT_PDCT_I,
		TRAK_I,
		TRAK_IDNN_TYPE_C,
		PRVD_D,
		SRCE_SYST_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C
	FROM ____var__dbt__poutbound_________var__dbt__prun__strm__c__________var__dbt__pctable__name__________var__dbt__prun__strm__pros__d______i
)

SELECT * FROM sq_ApptPrmoTrakIns