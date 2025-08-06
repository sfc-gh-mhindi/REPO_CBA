{{ config(materialized='view', tags=['LdUTIL_TRSF_EROR_R2_OD_Ins']) }}

WITH --Manual Task - PxSurrogateKeyGenerator - Seq_Number

SELECT * FROM Seq_Number