{{ config(materialized='view', tags=['XfmUnidPatyIdnnGnrc']) }}

WITH Lk_BusRules AS (
	SELECT
		{{ ref('Fnl_UtilPatyIdnnGnrc') }}.UNID_PATY_I,
		{{ ref('Fnl_UtilPatyIdnnGnrc') }}.EFFT_D,
		{{ ref('Fnl_UtilPatyIdnnGnrc') }}.PROS_KEY_EFFT_I,
		{{ ref('Fnl_UtilPatyIdnnGnrc') }}.SRCE_SYST_C,
		{{ ref('Fnl_UtilPatyIdnnGnrc') }}.EXPY_D,
		{{ ref('Fnl_UtilPatyIdnnGnrc') }}.PROS_KEY_EXPY_I,
		{{ ref('Fnl_UtilPatyIdnnGnrc') }}.ROW_SECU_ACCS_C,
		{{ ref('Fnl_UtilPatyIdnnGnrc') }}.IDNN_TYPE_C,
		{{ ref('Fnl_UtilPatyIdnnGnrc') }}.IDNN_VALU_X
	FROM {{ ref('Fnl_UtilPatyIdnnGnrc') }}
	LEFT JOIN {{ ref('UnidPatyIdnnGnrc') }} ON {{ ref('Fnl_UtilPatyIdnnGnrc') }}.UNID_PATY_I = {{ ref('UnidPatyIdnnGnrc') }}.UNID_PATY_I
	AND {{ ref('Fnl_UtilPatyIdnnGnrc') }}.IDNN_TYPE_C = {{ ref('UnidPatyIdnnGnrc') }}.IDNN_TYPE_C
	AND {{ ref('Fnl_UtilPatyIdnnGnrc') }}.SRCE_SYST_C = {{ ref('UnidPatyIdnnGnrc') }}.SRCE_SYST_C
)

SELECT * FROM Lk_BusRules