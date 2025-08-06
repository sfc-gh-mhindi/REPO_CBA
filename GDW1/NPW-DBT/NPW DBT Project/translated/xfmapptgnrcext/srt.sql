{{ config(materialized='view', tags=['XfmApptGnrcEXT']) }}

WITH Srt AS (
	SELECT
		Record_type,
		promise_type,
		ccl_app_id,
		audit_date,
		delivery_date,
		change_desc,
		user_name,
		first_name,
		last_name,
		mod_user_id,
		change_cat_id
	FROM {{ ref('APPT_GNRC_DATE') }}
	ORDER BY ccl_app_id ASC, audit_date ASC, promise_type ASC
)

SELECT * FROM Srt