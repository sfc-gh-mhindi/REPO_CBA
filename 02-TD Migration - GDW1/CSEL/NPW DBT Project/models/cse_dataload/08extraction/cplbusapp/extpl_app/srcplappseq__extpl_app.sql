WITH 
before_cte as(
	SELECT
	dummy
	from {{ ref('before__extpl_app') }}
	where 1=2
),
-- 20250813 - added by mhindi to use ingested table instead of the one in the source
{% set default_inprocess =
    cvar('intermediate_db') ~ '.' ~ cvar('files_schema') ~ '.' ~ cvar('base_dir') ~
    '__INPROCESS__CSE_CPL_BUS_APP_' ~ cvar('run_stream') ~ '_' ~ cvar('etl_process_dt') ~ '__DLY'
%}
{% set inprocess_src = var('inprocess_source_override', default_inprocess) %}
-- 20250813 - added by mhindi to use ingested table instead of the one in the source

srcplappseq AS (
	SELECT MOD_TIMESTAMP,
		PL_APP_ID,
		NOMINATED_BRANCH_ID,
		PL_PACKAGE_CAT_ID
	-- 20250813 - added by mhindi to use ingested table instead of the one in the source
	FROM {{ inprocess_src }}
)

SELECT 
	MOD_TIMESTAMP,
	PL_APP_ID,
	NOMINATED_BRANCH_ID,
	PL_PACKAGE_CAT_ID
FROM srcplappseq