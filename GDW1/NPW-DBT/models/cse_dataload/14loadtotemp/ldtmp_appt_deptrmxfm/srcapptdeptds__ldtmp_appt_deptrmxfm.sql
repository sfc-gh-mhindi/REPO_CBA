with cba_app__CSEL4__CSEL4DEV__dataset__Tmp_CSE_COM_BUS_CCL_CHL_COM_APP_APPT_DEPT__DS as(
select 
    APPT_I,
    DEPT_ROLE_C,
    EFFT_D,
    DEPT_I,
    EXPY_D,
    PROS_KEY_EFFT_I,
    PROS_KEY_EXPY_I,
    EROR_SEQN_I,
    RUN_STRM
from {{ cvar("datasets_schema") }}.{{ cvar("base_dir") }}__dataset__Tmp_{{ cvar("run_stream") }}_APPT_DEPT__DS
)

SELECT *
FROM cba_app__CSEL4__CSEL4DEV__dataset__Tmp_CSE_COM_BUS_CCL_CHL_COM_APP_APPT_DEPT__DS
