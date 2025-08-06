{{ config(materialized='view', tags=['MergehlmappEvntDtSmy']) }}

WITH TfmEvntDtSumy AS (
	SELECT
		EVNT_I,
		VALU_T,
		VALU_D
	FROM {{ ref('EVNT_DATE_SUMY') }}
	WHERE 
)

SELECT * FROM TfmEvntDtSumy