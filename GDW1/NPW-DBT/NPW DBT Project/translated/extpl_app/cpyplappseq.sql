{{ config(materialized='view', tags=['ExtPL_APP']) }}

WITH CpyPlAppSeq AS (
	SELECT
		PL_APP_ID,
		NOMINATED_BRANCH_ID,
		PL_PACKAGE_CAT_ID
	FROM {{ ref('SrcPlAppSeq') }}
)

SELECT * FROM CpyPlAppSeq