{{ config(
    post_hook=['INSERT OVERWRITE INTO ' ~ cvar("intermediate_db") ~ '.' ~ cvar("files_schema") ~'.PROCESSKEYHASH__HSH SELECT * FROM {{ this}}']
) }}

SELECT
    TRIM(CONV_M) as CONV_M,
    PROS_KEY_I
FROM {{ ref('transformer_6__loadgdwproskeyseq') }}