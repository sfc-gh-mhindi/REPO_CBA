{{ config(materialized='view', tags=['XfmHL_FEATURE_ATTRFrmExt']) }}

WITH LkpReferences2 AS (
	SELECT
		{{ ref('AddDummyColForLkup2') }}.HL_FEATURE_ATTR_ID,
		{{ ref('AddDummyColForLkup2') }}.HL_FEATURE_TERM,
		{{ ref('AddDummyColForLkup2') }}.HL_FEATURE_AMOUNT,
		{{ ref('AddDummyColForLkup2') }}.HL_FEATURE_BALANCE,
		{{ ref('AddDummyColForLkup2') }}.HL_FEATURE_FEE,
		{{ ref('AddDummyColForLkup2') }}.HL_FEATURE_SPEC_REPAY,
		{{ ref('AddDummyColForLkup2') }}.HL_FEATURE_EST_INT_AMT,
		{{ ref('AddDummyColForLkup2') }}.HL_FEATURE_DATE,
		{{ ref('AddDummyColForLkup2') }}.HL_FEATURE_COMMENT,
		{{ ref('AddDummyColForLkup2') }}.HL_FEATURE_CAT_ID,
		{{ ref('AddDummyColForLkup2') }}.HL_APP_PROD_ID,
		{{ ref('AddDummyColForLkup2') }}.ORIG_ETL_D,
		{{ ref('AddDummyColForLkup2') }}.TARG_CHAR_C_LKP1,
		{{ ref('SrcGRD_GNRC_MAP_Lks2') }}.TARG_CHAR_C AS TARG_CHAR_C_LKP2
	FROM {{ ref('AddDummyColForLkup2') }}
	LEFT JOIN {{ ref('SrcGRD_GNRC_MAP_Lks2') }} ON 
)

SELECT * FROM LkpReferences2