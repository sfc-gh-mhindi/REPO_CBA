{{ config(materialized='view', tags=['DltAPPT_ASETFrmTMP_APPT_ASET']) }}

WITH CpyApptFeat AS (
	SELECT
		NEW_APPT_I,
		NEW_ASET_I,
		{{ ref('SrcTmpApptAsetTera') }}.OLD_APPT_I AS NEW_APPT_I,
		{{ ref('SrcTmpApptAsetTera') }}.OLD_ASET_I AS NEW_ASET_I,
		NEW_PRIM_SECU_F,
		OLD_ASET_I,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptAsetTera') }}
)

SELECT * FROM CpyApptFeat