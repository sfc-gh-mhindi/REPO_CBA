{{ config(materialized='view', tags=['XfmChlBusPrtyAdrs']) }}

WITH dedup_aset_adrs AS (
	SELECT ASET_I, ADRS_I, SRCE_SYST_C, EFFT_D, EXPY_D, EROR_SEQN_I, RUN_STRM, CHL_PRCP_SCUY_FLAG 
	FROM (
		SELECT ASET_I, ADRS_I, SRCE_SYST_C, EFFT_D, EXPY_D, EROR_SEQN_I, RUN_STRM, CHL_PRCP_SCUY_FLAG,
		 ROW_NUMBER() OVER (PARTITION BY ASET_I ORDER BY 1 ASC) AS ROW_NUM 
		FROM {{ ref('XfmBusinessRules__OutAsetAdrsDS') }}
	) AS dedup_aset_adrs_TEMP
	WHERE ROW_NUM = 1
)

SELECT * FROM dedup_aset_adrs