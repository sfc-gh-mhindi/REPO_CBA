{{ config(materialized='view', tags=['LdDelFlagAPPT_PDCT_UNID_PATYUpd']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_pl__appt__pdct__unid__paty AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_pl__appt__pdct__unid__paty")  }})
SrcApptPdctUnidPatyDS AS (
	SELECT APPT_PDCT_I,
		PATY_ROLE_C,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_pl__appt__pdct__unid__paty
)

SELECT * FROM SrcApptPdctUnidPatyDS