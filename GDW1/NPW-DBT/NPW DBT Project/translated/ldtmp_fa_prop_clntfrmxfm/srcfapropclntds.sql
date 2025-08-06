{{ config(materialized='view', tags=['LdTMP_FA_PROP_CLNTFrmXfm']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_tmp__cse__ccc__bus__app__prod__fa__prop__clnt AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_tmp__cse__ccc__bus__app__prod__fa__prop__clnt")  }})
SrcFAPropClntDS AS (
	SELECT FA_PROP_CLNT_ID,
		COIN_ENTY_ID,
		CLNT_CORL_ID,
		COIN_ENTY_NAME,
		FA_ENTY_CAT_ID,
		FA_UTAK_ID,
		FA_PROP_CLNT_CAT_ID,
		ORIG_ETL_D,
		CHNG_CODE,
		INT_GRUP_I,
		SRCE_SYST_PATY_I,
		ORIG_SRCE_SYST_PATY_I,
		UNID_PATY_M,
		PATY_TYPE_C,
		SRCE_SYST_C
	FROM _cba__app_csel4_csel4dev_dataset_tmp__cse__ccc__bus__app__prod__fa__prop__clnt
)

SELECT * FROM SrcFAPropClntDS