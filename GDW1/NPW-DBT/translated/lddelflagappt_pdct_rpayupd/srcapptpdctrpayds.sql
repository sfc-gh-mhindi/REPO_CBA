{{ config(materialized='view', tags=['LdDelFlagAPPT_PDCT_RPAYUpd']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_hl__pl__app__prod__appt__pdct__rpay AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_hl__pl__app__prod__appt__pdct__rpay")  }})
SrcApptPdctRpayDS AS (
	SELECT APPT_PDCT_I,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_hl__pl__app__prod__appt__pdct__rpay
)

SELECT * FROM SrcApptPdctRpayDS