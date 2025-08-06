{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_error_cse__ccl__bus__app__prod__20061016__grd__gnrc__map__feat1', incremental_strategy='insert_overwrite', tags=['XfmCclAppProdFrmExt2']) }}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M 
FROM {{ ref('XfmBusinessRules__OutErrorGRD_GNRC_MAP_Feat1Seq') }}