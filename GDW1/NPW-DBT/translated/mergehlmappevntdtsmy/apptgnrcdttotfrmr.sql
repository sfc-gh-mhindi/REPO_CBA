{{ config(materialized='incremental', alias='_cba__app_csel4_sit_dataset_appt__gnrc__date__20100728', incremental_strategy='insert_overwrite', tags=['MergehlmappEvntDtSmy']) }}

SELECT
	EVNT_I,
	VALU_T,
	VALU_D,
	APPT_I 
FROM {{ ref('JoinSrcApptEvntGrupEvntGrupAssc_EvntDateSumy') }}