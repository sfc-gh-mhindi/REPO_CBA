{{ config(materialized='view', tags=['LdTMP_APPT_PDCT_CHKLFrmXfm']) }}

WITH 
_cba__app_csel4_dev_dataset_tmp__cse__cpl__bus__app__prod__appt__pdct__chkl AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_tmp__cse__cpl__bus__app__prod__appt__pdct__chkl")  }})
TgtTmpApptPdctChklDS AS (
	SELECT APPT_PDCT_I,
		CHKL_ITEM_C,
		STUS_D,
		STUS_C,
		SRCE_SYST_C,
		CHKL_ITEM_X,
		RUN_STRM
	FROM _cba__app_csel4_dev_dataset_tmp__cse__cpl__bus__app__prod__appt__pdct__chkl
)

SELECT * FROM TgtTmpApptPdctChklDS