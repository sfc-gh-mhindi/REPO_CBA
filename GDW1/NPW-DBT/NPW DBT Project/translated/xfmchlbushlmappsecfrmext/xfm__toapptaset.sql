{{ config(materialized='view', tags=['XfmChlBusHlmAppSecFrmExt']) }}

WITH Xfm__ToApptAset AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToXfm.FRWD_DOCU_C) Then '9999' Else ToXfm.FRWD_DOCU_C,
		IFF({{ ref('LkpMapApptAsetStloHm') }}.FRWD_DOCU_C IS NULL, '9999', {{ ref('LkpMapApptAsetStloHm') }}.FRWD_DOCU_C) AS svEror,
		-- *SRC*: \(20)If ToXfm.FORWARD_DOC_TO = 9999 Then 'Y' Else 'N',
		IFF({{ ref('LkpMapApptAsetStloHm') }}.FORWARD_DOC_TO = 9999, 'Y', 'N') AS svSetlLocnFltr,
		APPT_I,
		ASET_I,
		PRIM_SECU_F,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		eror_seqn_i,
		ASET_SETL_REQD,
		RUN_STRM
	FROM {{ ref('LkpMapApptAsetStloHm') }}
	WHERE 
)

SELECT * FROM Xfm__ToApptAset