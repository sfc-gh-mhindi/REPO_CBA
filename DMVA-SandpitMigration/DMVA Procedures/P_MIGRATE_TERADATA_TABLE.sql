CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING.P_MIGRATE_TERADATA_TABLE("SOURCE_DATABASE_NAME" VARCHAR, "SOURCE_TABLE_NAME" VARCHAR, "TARGET_DATABASE_NAME" VARCHAR, "TARGET_SCHEMA_NAME" VARCHAR, "TARGET_TABLE_NAME" VARCHAR, "WITH_CHUNKING_YN" VARCHAR DEFAULT 'N', "CHUNKING_COLUMN" VARCHAR DEFAULT null, "CHUNKING_VALUE" VARCHAR DEFAULT null, "CHUNKING_DATA_TYPE" VARCHAR DEFAULT null)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    result_message STRING DEFAULT '''';
    START_TIME              TIMESTAMP;
    MIGRATION_START_TIME    TIMESTAMP;
    running_count           INTEGER   DEFAULT 1;
    loop_counter            INTEGER DEFAULT 0;
    max_loops INTEGER       DEFAULT 1200;
    object_record_count     INTEGER DEFAULT 0;
    column_record_count     INTEGER DEFAULT 0;
    sync_json               STRING;
    checksum_json           STRING;
    target_table_count      INTEGER DEFAULT 0;
    target_tbl_sql          STRING;
    chunking_result         STRING DEFAULT '''';
BEGIN
 
    result_message := ''Starting Teradata table migration procedure...\n'';
    result_message := result_message || ''Source: '' || source_database_name || ''.'' || source_table_name || ''\n'';
    result_message := result_message || ''Target: '' || target_database_name || ''.'' || target_schema_name || ''.'' || target_table_name || ''\n'';
    result_message := result_message || ''Chunking Enabled: '' || with_chunking_yn || ''\n'';
    
    IF (with_chunking_yn = ''Y'') THEN
        result_message := result_message || ''Chunking Column: '' || COALESCE(chunking_column, ''NULL'') || ''\n'';
        result_message := result_message || ''Chunking Value: '' || COALESCE(chunking_value, ''NULL'') || ''\n'';
        result_message := result_message || ''Chunking Data Type: '' || COALESCE(chunking_data_type, ''NULL'') || ''\n'';
    END IF;
   
 
    result_message := ''Step 0: Checking if table has any existing records...\n'';
    target_tbl_sql := ''SELECT COUNT(1) FROM '' || target_database_name || ''.'' || target_schema_name || ''.'' || target_table_name;
   
    -- EXECUTE IMMEDIATE target_table_sql INTO target_table_count;
   
    -- EXECUTE IMMEDIATE ''SELECT COUNT(1) FROM '' || target_database_name || ''.'' || target_schema_name || ''.'' || target_table_name INTO target_table_count;
    LET count_result RESULTSET := (EXECUTE IMMEDIATE :target_tbl_sql);
    LET count_cursor CURSOR FOR count_result;
    OPEN count_cursor;
    FETCH count_cursor INTO target_table_count;
    CLOSE count_cursor;
   
    result_message := result_message || ''Target table has '' || target_table_count || '' rows...\n'';
   
    IF (target_table_count > 0) THEN
        result_message := result_message || ''Going straight to migrating table\n'';
    end if;
   
    IF (target_table_count = 0) THEN
   
        DELETE FROM DMVA_COLUMN_INFO WHERE COLUMN_ID IN (
            Select A.column_id
            from dmva_COLUMN_info A
                INNER JOIN DMVA_OBJECT_INFO B ON
                    A.OBJECT_ID = B.OBJECT_ID
            WHERE B.OBJECT_NAME = :source_table_name
        );
        DELETE FROM DMVA_OBJECT_INFO WHERE OBJECT_NAME = :source_table_name; --IN (''SANDPIT_DEMO_TBL001'',''SANDPIT_DEMO_TBL002'');
       
        
        
        ///////////////////////////////////////////// Step 1 - Configure Mapping Rules /////////////////////////////////////////////
        result_message := result_message || ''Step 1: Configuring mapping rules...\n'';
   
        DELETE FROM migration_tracking.dmva_mapping_rules WHERE TARGET_OBJECT_NAME = :source_table_name; -- IN (''SANDPIT_DEMO_TBL001'',''SANDPIT_DEMO_TBL002'');
       
        INSERT INTO migration_tracking.dmva_mapping_rules(
        mapping_rule_id,
        source_system_name,
        source_database_name,
        source_schema_name,
        source_object_name,
        target_system_name,
        target_database_name,
        target_schema_name,
        target_object_name,
        source_object_id,
        target_object_id,
        created_ts,
        updated_ts
        )
        SELECT
        NPD_D12_DMN_GDWMIG.migration_tracking.dmva_mapping_rule_id_seq.nextval mapping_rule_id,
        ''teradata_source'' source_system_name,
        ''NA'' source_database_name,
        :source_database_name source_schema_name,
        :source_table_name source_object_name,
        ''snowflake_target'' target_system_name,
        :target_database_name target_database_name,
        :target_schema_name target_schema_name,
        :target_table_name target_object_name,
        NULL source_object_id,
        NULL target_object_id,
        current_timestamp() created_ts,
        current_timestamp() updated_ts;
       
        ///////////////////////////////////////////// Step 2 - Call Metadata Tasks /////////////////////////////////////////////
       
        result_message := result_message || ''Step 2: Starting metadata tasks...\n'';
        CALL NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING.DMVA_GET_METADATA_TASKS_MH(''teradata_source'');
       
        -- Capture start time for monitoring
        START_TIME := DATEADD(''MINUTE'',-1,sysdate());--CURRENT_TIMESTAMP();
       
        ///////////////////////////////////////////// Step 3 - Monitor Metadata Tasks /////////////////////////////////////////////
        result_message := result_message || ''Step 3: Monitoring metadata tasks...\n'';
        -- Loop to monitor metadata tasks until completion
        running_count := 1;
        loop_counter := 0;
        max_loops := 1200; -- Maximum 1200 loops (100 minutes at 5 seconds each)
       
        WHILE (running_count > 0 AND loop_counter < max_loops) DO
       
            -- Check for running tasks
            SELECT COUNT(*)
            INTO running_count
            FROM dmva_tasks
            WHERE (status_cd = ''RUNNING'' or status_cd is null)
            AND queue_ts >= :START_TIME;
           
            -- If still running, wait 2 seconds
            IF (running_count > 0) THEN
                result_message := result_message || ''Metadata Loop '' || loop_counter || '': Found '' || running_count || '' RUNNING tasks, waiting 2 seconds...\n'';
                CALL SYSTEM$WAIT(2, ''SECONDS'');
            END IF;
            loop_counter := loop_counter + 1;
        END WHILE;
       
        -- Final status report
        IF (running_count = 0) THEN
            result_message := result_message || ''All metadata tasks completed successfully!\n'';
            --result_message := result_message || ''loop counter:'' || loop_counter || ''\n'';
            --result_message := result_message || ''running count:'' || running_count || ''\n'';
            --result_message := result_message || ''started at:'' || start_time || ''\n'';
        ELSE
            result_message := result_message || ''Maximum wait time reached for metadata tasks. Some tasks may still be running.\n'';
        END IF;
       
        ///////////////////////////////////////////// Step 4 - Verify Metadata /////////////////////////////////////////////
       
        result_message := result_message || ''Step 4: Verifying metadata...\n'';
       
        -- Verify object info was created
        SELECT COUNT(*) INTO object_record_count
        FROM dmva_object_info
        WHERE OBJECT_NAME = :source_table_name;
       
        result_message := result_message || ''Object metadata count: '' || object_record_count || ''\n'';
       
        -- Stop execution if no object metadata found
        IF (object_record_count = 0) THEN
            result_message := result_message || ''CRITICAL ERROR: No object metadata found for table '' || source_table_name || ''. Migration cannot proceed.\n'';
            result_message := result_message || ''Please check if metadata tasks completed successfully and table exists in source system.\n'';
            RETURN result_message;
        END IF;
        result_message := result_message || ''Object metadata validation: PASS\n'';
       
        -- Verify column info was created
        SELECT COUNT(*) INTO column_record_count
        FROM dmva_COLUMN_info A
        INNER JOIN DMVA_OBJECT_INFO B ON A.OBJECT_ID = B.OBJECT_ID
        WHERE B.OBJECT_NAME = :source_table_name;
       
        result_message := result_message || ''Column metadata count: '' || column_record_count || ''\n'';
       
        -- Stop execution if no column metadata found
        IF (column_record_count = 0) THEN
            result_message := result_message || ''CRITICAL ERROR: No column metadata found for table '' || source_table_name || ''. Migration cannot proceed.\n'';
            result_message := result_message || ''Please check if metadata tasks completed successfully and table structure is accessible.\n'';
            RETURN result_message;
        END IF;
        result_message := result_message || ''Column metadata validation: PASS\n'';
        result_message := result_message || ''Step 4 Validation PASSED: Metadata verification successful, proceeding with migration...\n'';
        ///////////////////////////////////////////// Step 5 - Populate Metadata /////////////////////////////////////////////
        result_message := result_message || ''Step 5: Populating metadata...\n'';
        CALL dmva_create_sync_tables_mh(false,false,''teradata_source'', parse_json(''"{"NA": [[ "'' || :source_database_name || ''", "'' || :source_table_name || ''" ]]}''''));
    END IF;   
    ///////////////////////////////////////////// Step 4.5 - Configure Chunking (Optional) /////////////////////////////////////////////
    
    IF (with_chunking_yn = ''Y'') THEN
        result_message := result_message || ''Step 4.5: Configuring chunking method...\n'';
        
        -- Validate chunking parameters
        IF (chunking_column IS NULL OR chunking_value IS NULL OR chunking_data_type IS NULL) THEN
            result_message := result_message || ''ERROR: Chunking enabled but required parameters are missing:\n'';
            result_message := result_message || ''- chunking_column: '' || COALESCE(chunking_column, ''NULL'') || ''\n'';
            result_message := result_message || ''- chunking_value: '' || COALESCE(chunking_value, ''NULL'') || ''\n'';
            result_message := result_message || ''- chunking_data_type: '' || COALESCE(chunking_data_type, ''NULL'') || ''\n'';
            result_message := result_message || ''Please provide all required chunking parameters.\n'';
            RETURN result_message;
        END IF;
        
        -- Call the chunking procedure
        CALL P_SET_CHUNKING_KEY(
            :source_database_name,
            :source_table_name,
            :chunking_column,
            :chunking_value,
            :chunking_data_type
        ) INTO chunking_result;
        
        result_message := result_message || ''Chunking configuration result:\n'' || chunking_result || ''\n'';
        
        -- Check if chunking configuration was successful
        IF (chunking_result LIKE ''%ERROR:%'') THEN
            result_message := result_message || ''CRITICAL ERROR: Chunking configuration failed. Migration cannot proceed.\n'';
            RETURN result_message;
        END IF;
        
        result_message := result_message || ''Step 4.5 Completed: Chunking configuration successful, proceeding with migration...\n'';
    ELSE
        result_message := result_message || ''Step 4.5: Skipping chunking configuration (with_chunking_yn = N)...\n'';
    END IF;
    ///////////////////////////////////////////// Step 6 - Run Migration /////////////////////////////////////////////
   
    result_message := result_message || ''Step 6: Starting migration tasks...\n'';
    CALL DMVA_GET_CHECKSUM_TASKS_MH(''teradata_source'', false, parse_json(''"{"NA": [[ "'' || :source_database_name || ''", "'' || :source_table_name || ''" ]]}''''));
   
    -- Capture start time for migration monitoring
    MIGRATION_START_TIME := DATEADD(''MINUTE'',-1,sysdate());--CURRENT_TIMESTAMP();
   
    ///////////////////////////////////////////// Step 7 - Monitor Migration Tasks /////////////////////////////////////////////
   
    result_message := result_message || ''Step 7: Monitoring migration tasks...\n'';
    -- Loop to monitor migration tasks until completion
    running_count := 1;
    loop_counter := 0;
    max_loops := 2400; -- Maximum 2400 loops (200 minutes at 5 seconds each) for longer migration tasks
    WHILE (running_count > 0 AND loop_counter < max_loops) DO
   
        -- Check for running tasks specific to our table
        SELECT COUNT(*)
        INTO running_count
        FROM dmva_tasks A
        INNER JOIN DMVA_OBJECT_INFO B ON
        B.OBJECT_ID = A.SOURCE_OBJECT_ID
        AND B.OBJECT_NAME = :source_table_name
        WHERE (A.status_cd = ''RUNNING'' or A.status_cd IS NULL)
        AND A.queue_ts >= :MIGRATION_START_TIME;
       
        -- If still running, wait 2 seconds
        IF (running_count > 0) THEN
            result_message := result_message || ''Migration Loop '' || loop_counter || '': Found '' || running_count || '' RUNNING tasks for target table, waiting 2 seconds...\n'';
            CALL SYSTEM$WAIT(2, ''SECONDS'');
        END IF;
        loop_counter := loop_counter + 1;
    END WHILE;
   
    -- Final migration status report
    IF (running_count = 0) THEN
        result_message := result_message || ''All migration tasks completed successfully!\n'';
        -- result_message := result_message || ''loop counter:'' || loop_counter || ''\n'';
        -- result_message := result_message || ''running count:'' || running_count || ''\n'';
        -- result_message := result_message || ''started at:'' || MIGRATION_START_TIME || ''\n'';
    ELSE
        result_message := result_message || ''Maximum wait time reached for migration tasks. Some tasks may still be running.\n'';
    END IF;
   
    ///////////////////////////////////////////// Step 8 - Final Verification /////////////////////////////////////////////
   
    result_message := result_message || ''Step 8: Migration procedure completed!\n'';
    result_message := result_message || ''Please verify data in table: '' || target_database_name || ''.'' || target_schema_name || ''.'' || target_table_name || ''\n'';
    RETURN result_message;
   
    EXCEPTION
    WHEN OTHER THEN
    result_message := result_message || ''ERROR: '' || SQLERRM || ''\n'';
    RETURN result_message;
END;
';