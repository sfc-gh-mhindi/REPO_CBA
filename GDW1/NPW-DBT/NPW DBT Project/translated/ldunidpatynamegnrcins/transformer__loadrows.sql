{{ config(materialized='view', tags=['LdUnidPatyNameGnrcIns']) }}

WITH Transformer__LoadRows AS (
	SELECT
		UNID_PATY_I,
		PATY_NAME_TYPE_C,
		EFFT_D,
		SRCE_SYST_C,
		SALU_C,
		PATY_ROLE_C,
		TITL_C,
		FRST_M,
		SCND_M,
		SRNM_M,
		THRD_M,
		FRTH_M,
		SUF_C,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		CO_CTCT_FRST_M,
		CO_CTCT_LAST_M,
		CO_CTCT_PRFR_M
	FROM {{ ref('UNID_PATY_NAME_GNRC_INSERT') }}
	WHERE 
)

SELECT * FROM Transformer__LoadRows