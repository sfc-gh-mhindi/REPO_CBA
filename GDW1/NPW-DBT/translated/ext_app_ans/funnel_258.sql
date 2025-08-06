{{ config(materialized='view', tags=['Ext_APP_ANS']) }}

WITH Funnel_258 AS (
	SELECT
		SRCE_KEY_I as SRCE_KEY_I,
		CONV_M as CONV_M,
		CONV_MAP_RULE_M as CONV_MAP_RULE_M,
		TRSF_TABL_M as TRSF_TABL_M,
		VALU_CHNG_BFOR_X as VALU_CHNG_BFOR_X,
		VALU_CHNG_AFTR_X as VALU_CHNG_AFTR_X,
		TRSF_X as TRSF_X,
		TRSF_COLM_M as TRSF_COLM_M
	FROM {{ ref('Transformer_243__OutErrRespC') }}
	UNION ALL
	SELECT
		SRCE_KEY_I,
		CONV_M,
		CONV_MAP_RULE_M,
		TRSF_TABL_M,
		VALU_CHNG_BFOR_X,
		VALU_CHNG_AFTR_X,
		TRSF_X,
		TRSF_COLM_M
	FROM {{ ref('Transformer_243__OutErrQstnC') }}
)

SELECT * FROM Funnel_258