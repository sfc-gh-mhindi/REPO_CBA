{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_error_cse__chl__bus__int__rt__perc__prod__int__marg__20060101__map__cse__feat__ovrd__reas__hl', incremental_strategy='insert_overwrite', tags=['XfmChlIntRateChlRatePercChlProdIntMargFrmExt']) }}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M 
FROM {{ ref('XfmBusinessRules__OutErrorMAP_CSE_FEAT_OVRD_REAS_HLSeq') }}