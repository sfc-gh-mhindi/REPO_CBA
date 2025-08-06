{{ config(materialized='view', tags=['LdREJT_CHL_BUS_FEAT_ATTR']) }}

WITH MappingCols AS (
	SELECT
		HL_FEATURE_ATTR_ID,
		{{ ref('ModConversions') }}.HL_FEATURE_TERM AS FEATURE_TERM,
		{{ ref('ModConversions') }}.HL_FEATURE_AMOUNT AS FEATURE_AMOUNT,
		{{ ref('ModConversions') }}.HL_FEATURE_BALANCE AS FEATURE_BALANCE,
		{{ ref('ModConversions') }}.HL_FEATURE_FEE AS FEATURE_FEE,
		{{ ref('ModConversions') }}.HL_FEATURE_SPEC_REPAY AS FEATURE_SPEC_REPAY,
		{{ ref('ModConversions') }}.HL_FEATURE_EST_INT_AMT AS FEATURE_EST_INT_AMT,
		{{ ref('ModConversions') }}.HL_FEATURE_DATE AS FEATURE_DATE,
		{{ ref('ModConversions') }}.HL_FEATURE_COMMENT AS FEATURE_COMMENT,
		HL_FEATURE_CAT_ID,
		HL_APP_PROD_ID,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM {{ ref('ModConversions') }}
	WHERE 
)

SELECT * FROM MappingCols