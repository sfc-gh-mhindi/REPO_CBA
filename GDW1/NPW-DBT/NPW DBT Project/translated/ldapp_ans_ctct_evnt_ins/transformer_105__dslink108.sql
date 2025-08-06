{{ config(materialized='view', tags=['LdAPP_ANS_CTCT_EVNT_Ins']) }}

WITH Transformer_105__DSLink108 AS (
	SELECT
		EVNT_I,
		SRCE_SYST_EVNT_I,
		EVNT_ACTL_D,
		SRCE_SYST_C,
		PROS_KEY_EFFT_I,
		EROR_SEQN_I,
		SRCE_SYST_EVNT_TYPE_I,
		CTCT_EVNT_TYPE_C,
		-- *SRC*: StringToTime(OutTgtInsertDS.EVNT_ACTL_T[1, 6], ("%hh:%nn:%ss")),
		STRINGTOTIME(SUBSTRING({{ ref('TgtInsertDS') }}.EVNT_ACTL_T, 1, 6), '%hh:%nn:%ss') AS EVNT_ACTL_T,
		ROW_SECU_ACCS_C
	FROM {{ ref('TgtInsertDS') }}
	WHERE @FALSE
)

SELECT * FROM Transformer_105__DSLink108