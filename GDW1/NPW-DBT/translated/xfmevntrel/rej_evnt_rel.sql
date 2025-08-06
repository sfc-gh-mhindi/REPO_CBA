{{ config(materialized='incremental', alias='_cba__app_csel4_sit_dataset_rej__evnt__rel__i__cse__onln__bus__clnt__rm__rate__20100803', incremental_strategy='insert_overwrite', tags=['XfmEvntRel']) }}

SELECT
	MOD_TIMESTAMP,
	OL_CLIENT_RM_RATING_ID,
	CLIENT_ID,
	CIF_CODE,
	OU_ID,
	CS_USER_ID,
	RATING,
	WIM_PROCESS_ID,
	ETL_DATE,
	ORG_ETL_D,
	EROR_C 
FROM {{ ref('XfmTrans__Reject_Rec') }}