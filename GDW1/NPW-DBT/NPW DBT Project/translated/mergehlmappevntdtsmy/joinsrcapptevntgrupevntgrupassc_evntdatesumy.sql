{{ config(materialized='view', tags=['MergehlmappEvntDtSmy']) }}

WITH JoinSrcApptEvntGrupEvntGrupAssc_EvntDateSumy AS (
	SELECT
		{{ ref('TfmEvntDtSumy') }}.EVNT_I,
		{{ ref('TfmEvntDtSumy') }}.VALU_T,
		{{ ref('TfmEvntDtSumy') }}.VALU_D,
		{{ ref('DRVD_EVNT_I') }}.APPT_I
	FROM {{ ref('TfmEvntDtSumy') }}
	INNER JOIN {{ ref('DRVD_EVNT_I') }} ON {{ ref('TfmEvntDtSumy') }}.EVNT_I = {{ ref('DRVD_EVNT_I') }}.EVNT_I
)

SELECT * FROM JoinSrcApptEvntGrupEvntGrupAssc_EvntDateSumy