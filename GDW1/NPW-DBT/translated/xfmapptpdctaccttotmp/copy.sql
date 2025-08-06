{{ config(materialized='view', tags=['XfmApptPdctAcctToTmp']) }}

WITH Copy AS (
	SELECT
		APP_PROD_ID,
		ACCOUNT_NUMBER,
		REPAYMENT_ACCOUNT_NUMBER
	FROM {{ ref('CSE_CPO_BUS_APP_PROD') }}
)

SELECT * FROM Copy