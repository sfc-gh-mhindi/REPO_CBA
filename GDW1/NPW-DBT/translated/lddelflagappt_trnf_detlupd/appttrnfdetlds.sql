{{ config(materialized='view', tags=['LdDelFlagAPPT_TRNF_DETLUpd']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cc__app__prod__bal__xfer__appt__trnf__detl AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cc__app__prod__bal__xfer__appt__trnf__detl")  }})
ApptTrnfDetlDS AS (
	SELECT APPT_I,
		APPT_TRNF_I,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_cc__app__prod__bal__xfer__appt__trnf__detl
)

SELECT * FROM ApptTrnfDetlDS