{{ config(materialized='view', tags=['LdREJT_XS_APP_ANS']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy%mm%dd] (ORIG_ETL_D)
	--MOD_TIMESTAMP= date_from_string[%yyyy%mm%dd] (MOD_TIMESTAMP)
	COM_SUBTYPE_CODE AS APP_ID, COM_APP_ID AS QA_QUESTION_ID, COM_PRODUCT_TYPE_ID AS QA_ANSWER_ID, COM_SM_CASE_ID AS TEXT_ANSWER, COM_FOUND_FLAG AS CIF_CODE, CPL_FOUND_FLAG AS CBA_STAFF_NUMBER, CCL_FOUND_FLAG AS LODGEMENT_BRANCH_ID, SBTY_CODE, ETL_D, ORIG_ETL_D, EROR_C 
	FROM {{ ref('SrcAppCclAppRejectsDS') }}
)

SELECT * FROM ModConversions