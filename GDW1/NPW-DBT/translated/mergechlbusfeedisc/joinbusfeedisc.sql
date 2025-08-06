{{ config(materialized='view', tags=['MergeChlBusFeeDisc']) }}

WITH JoinBusFeeDisc AS (
	SELECT
		{{ ref('CgAdd_Flag1') }}.BF_RECORD_TYPE,
		{{ ref('CgAdd_Flag1') }}.BF_MOD_TIMESTAMP,
		{{ ref('CgAdd_Flag1') }}.HL_FEE_ID AS leftRec_HL_FEE_ID,
		{{ ref('CgAdd_Flag1') }}.BF_HL_APP_PROD_ID,
		{{ ref('CgAdd_Flag1') }}.BF_XML_CODE,
		{{ ref('CgAdd_Flag1') }}.BF_DISPLAY_NAME,
		{{ ref('CgAdd_Flag1') }}.BF_CATEGORY,
		{{ ref('CgAdd_Flag1') }}.BF_UNIT_AMOUNT,
		{{ ref('CgAdd_Flag1') }}.BF_TOTAL_AMOUNT,
		{{ ref('CgAdd_Flag1') }}.BF_DUMMY,
		{{ ref('CgAdd_Flag1') }}.BF_FOUND_FLAG,
		{{ ref('CgAdd_Flag2') }}.BFD_RECORD_TYPE,
		{{ ref('CgAdd_Flag2') }}.BFD_MOD_TIMESTAMP,
		{{ ref('CgAdd_Flag2') }}.BFD_HL_FEE_DISCOUNT_ID,
		{{ ref('CgAdd_Flag2') }}.HL_FEE_ID AS rightRec_HL_FEE_ID,
		{{ ref('CgAdd_Flag2') }}.BFD_DISCOUNT_REASON,
		{{ ref('CgAdd_Flag2') }}.BFD_DISCOUNT_CODE,
		{{ ref('CgAdd_Flag2') }}.BFD_DISCOUNT_AMT,
		{{ ref('CgAdd_Flag2') }}.BFD_DISCOUNT_TERM,
		{{ ref('CgAdd_Flag2') }}.BFD_HL_FEE_DISCOUNT_CAT_ID,
		{{ ref('CgAdd_Flag2') }}.BFD_DUMMY,
		{{ ref('CgAdd_Flag2') }}.BFD_FOUND_FLAG
	FROM {{ ref('CgAdd_Flag1') }}
	OUTER JOIN {{ ref('CgAdd_Flag2') }} ON {{ ref('CgAdd_Flag1') }}.HL_FEE_ID = {{ ref('CgAdd_Flag2') }}.HL_FEE_ID
)

SELECT * FROM JoinBusFeeDisc