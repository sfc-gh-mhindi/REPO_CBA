{{ config(materialized='view', tags=['LdTMP_APPT_ASET_SETL_LOCNFrmXfm']) }}

WITH Cpy AS (
	SELECT
		APPT_I,
		ASET_I,
		FRWD_DOCU_C,
		SETL_LOCN_X,
		SETL_CMMT_X,
		RUN_STRM
	FROM {{ ref('TgtTmp_ApptAsetSetlLocnDS') }}
)

SELECT * FROM Cpy