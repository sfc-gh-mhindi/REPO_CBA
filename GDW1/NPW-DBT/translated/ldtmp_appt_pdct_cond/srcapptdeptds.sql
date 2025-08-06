{{ config(materialized='view', tags=['LdTMP_APPT_PDCT_COND']) }}

WITH 
_cba__app_hlt_dev_dataset_cse__chl__bus__tu__app__cond__appt__pdct__cond AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_dev_dataset_cse__chl__bus__tu__app__cond__appt__pdct__cond")  }})
SrcApptDeptDS AS (
	SELECT APPT_PDCT_I,
		COND_C,
		APPT_PDCT_COND_MEET_D
	FROM _cba__app_hlt_dev_dataset_cse__chl__bus__tu__app__cond__appt__pdct__cond
)

SELECT * FROM SrcApptDeptDS