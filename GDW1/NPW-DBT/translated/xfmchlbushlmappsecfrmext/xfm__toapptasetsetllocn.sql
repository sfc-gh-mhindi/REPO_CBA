{{ config(materialized='view', tags=['XfmChlBusHlmAppSecFrmExt']) }}

WITH Xfm__ToApptAsetSetlLocn AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToXfm.FRWD_DOCU_C) Then '9999' Else ToXfm.FRWD_DOCU_C,
		IFF({{ ref('LkpMapApptAsetStloHm') }}.FRWD_DOCU_C IS NULL, '9999', {{ ref('LkpMapApptAsetStloHm') }}.FRWD_DOCU_C) AS svEror,
		-- *SRC*: \(20)If ToXfm.FORWARD_DOC_TO = 9999 Then 'Y' Else 'N',
		IFF({{ ref('LkpMapApptAsetStloHm') }}.FORWARD_DOC_TO = 9999, 'Y', 'N') AS svSetlLocnFltr,
		APPT_I,
		ASET_I,
		'CSE' AS SRCE_SYST_C,
		svEror AS FRWD_DOCU_C,
		SETL_LOCN_X,
		SETL_CMMT_X,
		EFFT_D,
		EXPY_D,
		RUN_STRM
	FROM {{ ref('LkpMapApptAsetStloHm') }}
	WHERE svSetlLocnFltr = 'N'
)

SELECT * FROM Xfm__ToApptAsetSetlLocn