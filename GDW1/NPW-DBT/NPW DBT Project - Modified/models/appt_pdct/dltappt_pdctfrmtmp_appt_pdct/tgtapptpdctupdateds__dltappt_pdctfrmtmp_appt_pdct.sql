{{
  config(
    post_hook=[
      "INSERT OVERWRITE INTO " ~ cvar("datasets_schema") ~ "." ~ cvar("base_dir") ~ "__dataset__"~ cvar("tgt_table") ~ "_U_"~ cvar("run_stream") ~ "_"~ cvar("etl_process_dt_tbl") ~ "__DS SELECT * FROM {{ this }}"
    ]
  )
}}


WITH XfmCheckDeltaAction__OutTgtApptPdctUpdateDS AS (
	SELECT
		COALESCE(NEW_APPT_PDCT_I,'')  AS APPT_PDCT_I,
		COALESCE(OLD_APPT_I,'') as APPT_I,
		COALESCE(OLD_EFFT_D,null) AS EFFT_D,
		expirydate AS EXPY_D,
		'{{ cvar("refr_pk") }}' AS PROS_KEY_EXPY_I
	FROM {{ ref('xfmcheckdeltaaction__dltappt_pdctfrmtmp_appt_pdct') }} JoinAll
	WHERE JoinAll.change_code = JoinAll."UPDATE"
)

select 
	APPT_PDCT_I,
	APPT_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM 
	XfmCheckDeltaAction__OutTgtApptPdctUpdateDS