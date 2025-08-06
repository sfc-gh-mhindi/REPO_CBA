{{ config(materialized='view', tags=['XfmApptGnrcEXT']) }}

WITH XfmFilter AS (
	SELECT
		-- *SRC*: \(20)IF IsNull(totrans.ccl_app_id) Or Trim(( IF IsNotNull((totrans.ccl_app_id)) THEN (totrans.ccl_app_id) ELSE "")) = '' THEN "N" elSE "Y",
		IFF({{ ref('Srt') }}.ccl_app_id IS NULL OR TRIM(IFF({{ ref('Srt') }}.ccl_app_id IS NOT NULL, {{ ref('Srt') }}.ccl_app_id, '')) = '', 'N', 'Y') AS svlsValidRecord,
		-- *SRC*: \(20)IF IsNull(totrans.promise_type) OR Trim(( IF IsNotNull((totrans.promise_type)) THEN (totrans.promise_type) ELSE "")) = '' THEN "N" ELSE "Y",
		IFF({{ ref('Srt') }}.promise_type IS NULL OR TRIM(IFF({{ ref('Srt') }}.promise_type IS NOT NULL, {{ ref('Srt') }}.promise_type, '')) = '', 'N', 'Y') AS svIsValidRecord3,
		-- *SRC*: \(20)If svIsValidRecord3 = 'Y' and (totrans.promise_type = '1' Or totrans.promise_type = '2') Then 'Y' Else 'N',
		IFF(svIsValidRecord3 = 'Y' AND {{ ref('Srt') }}.promise_type = '1' OR {{ ref('Srt') }}.promise_type = '2', 'Y', 'N') AS svIsValidRecord1,
		-- *SRC*: \(20)If svIsValidRecord1 = 'Y' THEN  IF ((IsNull(totrans.delivery_date) OR Trim(( IF IsNotNull((totrans.delivery_date)) THEN (totrans.delivery_date) ELSE "")) = '') And (IsNull(totrans.change_cat_id) OR Trim(( IF IsNotNull((totrans.change_cat_id)) THEN (totrans.change_cat_id) ELSE "")) = '')) then 'N' ELSE "Y" ELSE "Y",
		IFF(svIsValidRecord1 = 'Y', IFF({{ ref('Srt') }}.delivery_date IS NULL OR TRIM(IFF({{ ref('Srt') }}.delivery_date IS NOT NULL, {{ ref('Srt') }}.delivery_date, '')) = '' AND {{ ref('Srt') }}.change_cat_id IS NULL OR TRIM(IFF({{ ref('Srt') }}.change_cat_id IS NOT NULL, {{ ref('Srt') }}.change_cat_id, '')) = '', 'N', 'Y'), 'Y') AS svIsValidRecord2,
		ccl_app_id,
		promise_type,
		audit_date,
		delivery_date,
		mod_user_id,
		change_cat_id
	FROM {{ ref('Srt') }}
	WHERE svlsValidRecord = 'Y' AND svIsValidRecord2 = 'Y' AND svIsValidRecord3 = 'Y'
)

SELECT * FROM XfmFilter