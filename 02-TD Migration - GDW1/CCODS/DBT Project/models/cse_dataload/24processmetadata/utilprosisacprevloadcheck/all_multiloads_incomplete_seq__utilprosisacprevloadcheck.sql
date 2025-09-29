{{  
  config(
    post_hook=[
      'INSERT OVERWRITE INTO '~cvar("intermediate_db")~'.' ~ cvar("files_schema") ~ '.' ~ cvar("base_dir") ~ '__TEMP__UTIL_PROS_ISAC__TXT1 SELECT * FROM {{ this }}']
  ) 
}}

SELECT NUM_LOAD_ERR
FROM {{ ref("transformer_6__utilprosisacprevloadcheck") }}
WHERE AllLoadsComplete = 'A'