{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_error_cse__cpl__bus__fee__margin__20060101__mapcsefeatplfee', incremental_strategy='insert_overwrite', tags=['XfmPlIntRateAmtMarginFrmExt']) }}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M 
FROM {{ ref('XfmBusinessRules__OutErrorMAP_GRD_GNRC_MAP_FEAT_ISeq') }}