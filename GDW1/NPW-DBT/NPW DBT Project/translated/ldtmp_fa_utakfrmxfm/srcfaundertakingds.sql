{{ config(materialized='view', tags=['LdTMP_FA_UTAKFrmXfm']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_tmp__fa__utak AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_tmp__fa__utak")  }})
SrcFAUndertakingDS AS (
	SELECT FA_UTAK_ID,
		PLAN_GRP_NAME,
		COIN_ADVC_GRP_ID,
		ADVC_GRP_CORL_ID,
		CRAT_DATE,
		CRAT_BY_STAF_NUM,
		SM_CASE_ID,
		ORIG_ETL_D,
		INT_GRUP_I,
		INT_GRUP_TYPE_C,
		INT_GRUP_M,
		SRCE_SYST_INT_GRUP_I,
		SRCE_SYST_C,
		ORIG_SRCE_SYST_INT_GRUP_I,
		CRAT_D,
		EMPL_I,
		EMPL_ROLE_C,
		INT_GRUP_EMPL_F
	FROM _cba__app_csel4_csel4dev_dataset_tmp__fa__utak
)

SELECT * FROM SrcFAUndertakingDS