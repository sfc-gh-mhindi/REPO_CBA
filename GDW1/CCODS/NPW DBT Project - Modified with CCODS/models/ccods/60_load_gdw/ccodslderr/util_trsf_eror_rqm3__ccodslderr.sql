{{
  config(
        post_hook=[
      		'MERGE INTO ' ~ cvar("stg_ctl_db") ~ '.' ~ cvar('pods_load_db') ~ '.' ~ cvar('pcerr_table') ~ ' AS TGT 
			USING {{this}} AS SRC 
			ON TGT.SRCE_KEY_I = SRC.SRCE_KEY_I 
			and TGT.CONV_M = SRC.CONV_M
			and TGT.SRCE_EFFT_D = SRC.SRCE_EFFT_D
			WHEN NOT MATCHED 
			THEN INSERT (
			SRCE_KEY_I, 
			CONV_M, 
			CONV_MAP_RULE_M, 
			TRSF_TABL_M, 
			SRCE_EFFT_D,
			VALU_CHNG_BFOR_X,
			VALU_CHNG_AFTR_X,
			TRSF_X,
			TRSF_COLM_M,
			EROR_SEQN_I,
			SRCE_FILE_M,
			PROS_KEY_EFFT_I,
			TRSF_KEY_I
			)
			VALUES (
			SRC.SRCE_KEY_I,
			SRC.CONV_M,
			SRC.CONV_MAP_RULE_M,
			SRC.TRSF_TABL_M,
			SRC.SRCE_EFFT_D,
			SRC.VALU_CHNG_BFOR_X,
			SRC.VALU_CHNG_AFTR_X,
			SRC.TRSF_X,
			SRC.TRSF_COLM_M,
			SRC.EROR_SEQN_I,
			SRC.SRCE_FILE_M,
			SRC.PROS_KEY_EFFT_I,
			SRC.TRSF_KEY_I)
			']
  )
}}



SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	SRCE_EFFT_D,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M,
	EROR_SEQN_I,
	SRCE_FILE_M,
	PROS_KEY_EFFT_I,
	TRSF_KEY_I 
FROM {{ ref('err_file__ccodslderr') }}