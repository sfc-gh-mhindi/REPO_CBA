{{ config(materialized='view', tags=['LdDelFlagPATY_INT_GRUPUpd']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_fa__client__undertaking__paty__int__grup AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_fa__client__undertaking__paty__int__grup")  }})
PatyIntGrupDS AS (
	SELECT INT_GRUP_I,
		SRCE_SYST_PATY_INT_GRUP_I,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_fa__client__undertaking__paty__int__grup
)

SELECT * FROM PatyIntGrupDS