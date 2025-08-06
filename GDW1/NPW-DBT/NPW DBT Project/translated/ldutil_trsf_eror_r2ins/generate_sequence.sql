{{ config(materialized='view', tags=['LdUTIL_TRSF_EROR_R2Ins']) }}

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
		{{ ref('Remove_Duplicates') }}.ERORSEQ AS EROR_SEQN_I
	FROM {{ ref('Remove_Duplicates') }}
	WHERE 
)

SELECT * FROM Generate_Sequence