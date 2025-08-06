{{ config(materialized='view', tags=['XfmCC_APP_PRODFrmExt']) }}

WITH Transformer_212 AS (
	SELECT
		CC_APP_PROD_ID,
		REQUESTED_LIMIT_AMT,
		-- *SRC*: Trim(OutCpyRename.CC_INTEREST_OPT_CAT_ID),
		TRIM({{ ref('CgAddMapTypeC') }}.CC_INTEREST_OPT_CAT_ID) AS SRCE_NUMC_1_C,
		CBA_HOMELOAN_NO,
		PRE_APPRV_AMOUNT,
		ORIG_ETL_D,
		-- *SRC*: Trim(OutCpyRename.MAP_TYPE_C),
		TRIM({{ ref('CgAddMapTypeC') }}.MAP_TYPE_C) AS MAP_TYPE_C,
		READ_COSTS_AND_RISKS_FLAG,
		ACCEPTS_COSTS_AND_RISKS_DATE
	FROM {{ ref('CgAddMapTypeC') }}
	WHERE 
)

SELECT * FROM Transformer_212