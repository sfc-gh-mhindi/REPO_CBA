{{  
  config(
    post_hook=["""INSERT OVERWRITE INTO """ ~ cvar("files_schema") ~ "." ~ cvar("base_dir") ~ """TEMP__UTIL_PROS_ISAC__TXT SELECT * FROM {{ this}}"""]
  ) 
}}

SELECT NUM_LOAD_ERR
FROM {{ ref("transformer_6__utilprosisacprevloadcheck") }}
WHERE AllLoadsComplete = 'A'