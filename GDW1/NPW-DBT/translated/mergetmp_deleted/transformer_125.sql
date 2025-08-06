{{ config(materialized='view', tags=['MergeTMP_DELETED']) }}

WITH Transformer_125 AS (
	SELECT
		-- *SRC*: \(20)If Ld6.DELETED_TABLE_NAME = 'HL_FEE' Or Ld6.DELETED_TABLE_NAME = 'HL_FEE_DISCOUNT' Then 'HF' Else ( If Ld6.DELETED_TABLE_NAME = 'HL_INT_RATE' Or Ld6.DELETED_TABLE_NAME = 'HL_PROD_INT_MARGIN' Then 'HR' Else ( If Ld6.DELETED_TABLE_NAME = 'PL_INT_RATE' Or Ld6.DELETED_TABLE_NAME = 'PL_MARGIN' Then 'PR' Else ( If Ld6.DELETED_TABLE_NAME = 'PL_FEE' Then 'PF' Else ( If Ld6.DELETED_TABLE_NAME = 'HL_FEATURE_ATTR' Then 'HA' Else ( If Ld6.DELETED_TABLE_NAME = 'CCL_APP_FEE' Then 'CF' Else ''))))),
		IFF(
	    {{ ref('SrcApptPdctFeat') }}.DELETED_TABLE_NAME = 'HL_FEE' OR {{ ref('SrcApptPdctFeat') }}.DELETED_TABLE_NAME = 'HL_FEE_DISCOUNT', 'HF',     
	    IFF(
	        {{ ref('SrcApptPdctFeat') }}.DELETED_TABLE_NAME = 'HL_INT_RATE'
	    or {{ ref('SrcApptPdctFeat') }}.DELETED_TABLE_NAME = 'HL_PROD_INT_MARGIN', 'HR',         
	        IFF(
	            {{ ref('SrcApptPdctFeat') }}.DELETED_TABLE_NAME = 'PL_INT_RATE'
	        or {{ ref('SrcApptPdctFeat') }}.DELETED_TABLE_NAME = 'PL_MARGIN', 'PR', 
	            IFF({{ ref('SrcApptPdctFeat') }}.DELETED_TABLE_NAME = 'PL_FEE', 'PF', IFF({{ ref('SrcApptPdctFeat') }}.DELETED_TABLE_NAME = 'HL_FEATURE_ATTR', 'HA', IFF({{ ref('SrcApptPdctFeat') }}.DELETED_TABLE_NAME = 'CCL_APP_FEE', 'CF', '')))
	        )
	    )
	) AS svFeatStreamType,
		DELETED_TABLE_NAME,
		DELETED_KEY_1,
		-- *SRC*: svFeatStreamType : Ld6.DELETED_KEY_1_VALUE,
		CONCAT(svFeatStreamType, {{ ref('SrcApptPdctFeat') }}.DELETED_KEY_1_VALUE) AS DELETED_KEY_1_VALUE,
		DELETED_KEY_2,
		DELETED_KEY_2_VALUE,
		DELETED_KEY_3,
		DELETED_KEY_3_VALUE,
		DELETED_KEY_4,
		DELETED_KEY_4_VALUE,
		DELETED_KEY_5,
		DELETED_KEY_5_VALUE
	FROM {{ ref('SrcApptPdctFeat') }}
	WHERE 
)

SELECT * FROM Transformer_125