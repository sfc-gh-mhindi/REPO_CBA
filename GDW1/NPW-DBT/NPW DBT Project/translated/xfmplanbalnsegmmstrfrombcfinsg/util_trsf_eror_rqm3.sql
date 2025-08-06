{{ config(materialized='incremental', alias='_cba__app_ccods_uat_inprocess_bcfinsg__util__trsf__eror__rqm3__plan__baln__segm__mstr__20100824', incremental_strategy='insert_overwrite', tags=['XfmPlanBalnSegmMstrFromBCFINSG']) }}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	SRCE_EFFT_D,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M,
	EROR_SEQN_I,
	SRCE_FILE_M,
	PROS_KEY_EFFT_I,
	TRSF_KEY_I 
FROM {{ ref('FUNNEL') }}