{{ config(materialized='view', tags=['XfmApptGnrcEXT']) }}

WITH 
_cba__app01_csel4_dev_inprocess_cse__ccl__cli__date__exp__aud__cse__ccl__cli__date__exp__aud__20100303 AS (
	SELECT
	*
	FROM {{ source("","_cba__app01_csel4_dev_inprocess_cse__ccl__cli__date__exp__aud__cse__ccl__cli__date__exp__aud__20100303")  }})
APPT_GNRC_DATE AS (
	SELECT Record_type,
		promise_type,
		ccl_app_id,
		audit_date,
		change_desc,
		delivery_date,
		user_name,
		first_name,
		last_name,
		mod_user_id,
		change_cat_id
	FROM _cba__app01_csel4_dev_inprocess_cse__ccl__cli__date__exp__aud__cse__ccl__cli__date__exp__aud__20100303
)

SELECT * FROM APPT_GNRC_DATE