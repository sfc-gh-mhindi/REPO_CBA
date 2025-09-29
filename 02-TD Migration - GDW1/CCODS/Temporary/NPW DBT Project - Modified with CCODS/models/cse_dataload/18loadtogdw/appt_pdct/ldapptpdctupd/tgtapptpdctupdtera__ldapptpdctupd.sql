{{
  config(
      post_hook=[
            'MERGE INTO ' ~ cvar("mart_db") ~'.'~ cvar("gdw_acct_db") ~ '.' ~ cvar("tgt_table") ~ ' AS target
            USING ' ~ this ~ ' AS source
            ON target.appt_pdct_i = source.APPT_PDCT_I
                AND target.appt_i = source.APPT_I
                AND target.efft_d = source.EFFT_D
            WHEN MATCHED THEN UPDATE SET
                target.expy_d = source.EXPY_D,
                target.pros_key_expy_i = source.PROS_KEY_EXPY_I'
        ]
    )
}}

SELECT 
    APPT_PDCT_I,
    APPT_I,
    EFFT_D,
    EXPY_D,
    PROS_KEY_EXPY_I
FROM {{ ref('tgtapptpdctupdateds__ldapptpdctupd') }} 

