{{ config(materialized='view', tags=['DltASET_ADRSFrmTMP_ASET_ADRS']) }}

WITH XfmCheckDeltaAction__OutTgtApptFeatUpdateDS AS (
	SELECT
		{{ ref('SrcTmpAsetAdrsTera') }}.NEW_ASET_I AS ASET_I,
		{{ ref('SrcTmpAsetAdrsTera') }}.NEW_ADRS_I AS ADRS_I
	FROM {{ ref('SrcTmpAsetAdrsTera') }}
	WHERE 1 = 2
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptFeatUpdateDS