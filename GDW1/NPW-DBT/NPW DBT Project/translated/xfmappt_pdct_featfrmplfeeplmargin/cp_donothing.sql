{{ config(materialized='view', tags=['XfmAPPT_PDCT_FEATFrmPlFeePlMargin']) }}

WITH CP_DoNothing AS (
	SELECT
		
	FROM {{ ref('SplitRejectTableRecs__IgnoreRejtTableRecs') }}
)

SELECT * FROM CP_DoNothing