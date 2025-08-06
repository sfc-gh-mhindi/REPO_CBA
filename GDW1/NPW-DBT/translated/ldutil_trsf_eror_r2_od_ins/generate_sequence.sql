{{ config(materialized='view', tags=['LdUTIL_TRSF_EROR_R2_OD_Ins']) }}

WITH Generate_Sequence AS (
	SELECT
		SRCE_KEY_I,
		CONV_M,
		CONV_MAP_RULE_M,
		TRSF_TABL_M,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS SRCE_EFFT_D,
		VALU_CHNG_BFOR_X,
		VALU_CHNG_AFTR_X,
		TRSF_X,
		TRSF_COLM_M,
		{{ ref('Seq_Number') }}.ERORSEQ AS EROR_SEQN_I,
		SRCE_FILE_M,
		GDW_PROS_ID AS PROS_KEY_EFFT_I,
		TRSF_KEY_I
	FROM {{ ref('Seq_Number') }}
	WHERE 
)

SELECT * FROM Generate_Sequence