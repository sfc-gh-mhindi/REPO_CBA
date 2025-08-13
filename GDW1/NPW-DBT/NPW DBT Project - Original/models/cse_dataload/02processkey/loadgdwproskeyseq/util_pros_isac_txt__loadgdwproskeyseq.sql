{{ config(
    post_hook=["""INSERT OVERWRITE INTO """ ~ cvar("intermediate_db") ~ "." ~ cvar("files_schema") ~ "." ~ cvar("base_dir") ~ """__TEMP__UTIL_PROS_ISAC__TXT SELECT * FROM {{ this}}"""]
) }}

SELECT
    CONV_M,
    PROS_KEY_I
FROM {{ ref('transformer_6__loadgdwproskeyseq') }}