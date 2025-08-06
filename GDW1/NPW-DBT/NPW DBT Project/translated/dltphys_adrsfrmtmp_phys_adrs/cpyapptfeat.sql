{{ config(materialized='view', tags=['DltPHYS_ADRSFrmTMP_PHYS_ADRS']) }}

WITH CpyApptFeat AS (
	SELECT
		NEW_ADRS_I,
		NEW_PHYS_ADRS_TYPE_C,
		NEW_ADRS_LINE_1_X,
		NEW_ADRS_LINE_2_X,
		NEW_SURB_X,
		NEW_CITY_X,
		NEW_PCOD_C,
		NEW_STAT_C,
		NEW_ISO_CNTY_C,
		{{ ref('SrcTmpPhysAdrsTera') }}.OLD_ADRS_I AS NEW_ADRS_I,
		{{ ref('SrcTmpPhysAdrsTera') }}.OLD_PHYS_ADRS_TYPE_C AS NEW_PHYS_ADRS_TYPE_C,
		{{ ref('SrcTmpPhysAdrsTera') }}.OLD_ADRS_LINE_1_X AS NEW_ADRS_LINE_1_X,
		{{ ref('SrcTmpPhysAdrsTera') }}.OLD_ADRS_LINE_2_X AS NEW_ADRS_LINE_2_X,
		{{ ref('SrcTmpPhysAdrsTera') }}.OLD_SURB_X AS NEW_SURB_X,
		{{ ref('SrcTmpPhysAdrsTera') }}.OLD_CITY_X AS NEW_CITY_X,
		{{ ref('SrcTmpPhysAdrsTera') }}.OLD_PCOD_C AS NEW_PCOD_C,
		{{ ref('SrcTmpPhysAdrsTera') }}.OLD_STAT_C AS NEW_STAT_C,
		{{ ref('SrcTmpPhysAdrsTera') }}.OLD_ISO_CNTY_C AS NEW_ISO_CNTY_C,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpPhysAdrsTera') }}
)

SELECT * FROM CpyApptFeat