{{ config(materialized='view', tags=['LdTmp_Appt_QstnFrmXfm']) }}

WITH 
_cba__app_pj__itsmoteam_cbmgdw_dev_dataset_cse__com__bus__app__ncpr__answer__cse__com__cpo__bus__ncpr__clnt__20150915 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_pj__itsmoteam_cbmgdw_dev_dataset_cse__com__bus__app__ncpr__answer__cse__com__cpo__bus__ncpr__clnt__20150915")  }})
TgtAppt_Qstn AS (
	SELECT APPT_I,
		QSTN_C,
		RESP_C,
		RESP_CMMT_X,
		PATY_I,
		EFFT_D,
		EXPY_D,
		ROW_SECU_ACCS_C,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_pj__itsmoteam_cbmgdw_dev_dataset_cse__com__bus__app__ncpr__answer__cse__com__cpo__bus__ncpr__clnt__20150915
)

SELECT * FROM TgtAppt_Qstn