{{ config(materialized='view', tags=['XfmAppt_Pdct_Dept']) }}

WITH 
grd_oun_map AS (
	SELECT
	*
	FROM {{ source("tdcseld","grd_oun_map")  }}),
GRD_OUN_MAP_NOM AS (SELECT CAST(BSB_BRCH_N AS VARCHAR(255)), DEPT_I FROM GRD_OUN_MAP WHERE EFFT_D <= {{ var("pRUN_STRM_PROS_D") }} QUALIFY ROW_NUMBER() OVER (PARTITION BY BSB_BANK_N, BSB_BRCH_N ORDER BY EFFT_STUS_F ASC, OPEN_F DESC, EFFT_D DESC) = 1)


SELECT * FROM GRD_OUN_MAP_NOM