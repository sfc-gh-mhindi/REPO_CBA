{{ config(materialized='view', tags=['DltApptGnrcDateCCL']) }}

WITH Column_Generator__PRE__FILTER AS (
	SELECT * FROM {{ ref('Filter') }}
	WHERE MODF_S <> MODF_S_TEMP
),
--Manual Task - ColumnGenerator - Column_Generator

SELECT * FROM Column_Generator