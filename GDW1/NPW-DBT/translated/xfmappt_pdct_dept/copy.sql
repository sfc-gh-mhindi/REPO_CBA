{{ config(materialized='view', tags=['XfmAppt_Pdct_Dept']) }}

WITH Copy AS (
	SELECT
		APP_PROD_ID,
		NOMINATED_BSB,
		LODGEMENT_BRANCH_BSB
	FROM {{ ref('CSE_CPO_BUS_APP_PROD') }}
)

SELECT * FROM Copy