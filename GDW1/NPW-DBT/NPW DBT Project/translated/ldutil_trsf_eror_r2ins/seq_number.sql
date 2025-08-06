{{ config(materialized='view', tags=['LdUTIL_TRSF_EROR_R2Ins']) }}

WITH --Manual Task - PxSurrogateKeyGenerator - Seq_Number

SELECT * FROM Seq_Number