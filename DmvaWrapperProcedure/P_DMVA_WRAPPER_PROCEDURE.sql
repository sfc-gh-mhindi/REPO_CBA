CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING_V2.P_DMVA_WRAPPER_PROCEDURE(
    "P_SOURCE_DATABASE_NAME" VARCHAR, 
    "P_SOURCE_TABLE_NAME" VARCHAR, 
    "P_TARGET_DATABASE_NAME" VARCHAR, 
    "P_TARGET_SCHEMA_NAME" VARCHAR, 
    "P_TARGET_TABLE_NAME" VARCHAR, 
    "P_WITH_CHUNKING_YN" VARCHAR DEFAULT 'N', 
    "P_CHUNKING_COLUMN" VARCHAR DEFAULT null, 
    "P_CHUNKING_VALUE" VARCHAR DEFAULT null, 
    "P_CHUNKING_DATA_TYPE" VARCHAR DEFAULT null, 
    "P_SKIP_WAITING_FOR_MIGRATION_TASKS" VARCHAR DEFAULT 'N',
    "P_LOAD_TYPE" VARCHAR DEFAULT 'FULL',
    "P_REFRESH_STRUCTURES_YN" VARCHAR DEFAULT 'Y'
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    result_message STRING DEFAULT '''';
    START_TIME TIMESTAMP;
    MIGRATION_START_TIME TIMESTAMP;
    running_count INTEGER DEFAULT 1;
    loop_counter INTEGER DEFAULT 0;
    max_loops INTEGER DEFAULT 1200;
    object_record_count INTEGER DEFAULT 0;
    column_record_count INTEGER DEFAULT 0;
    target_table_count INTEGER DEFAULT 0;
    target_tbl_sql STRING;
    chunking_result STRING DEFAULT '''';
    mapping_record_count INTEGER DEFAULT 0;
    checksum_delete_count INTEGER DEFAULT 0;
    target_delete_count INTEGER DEFAULT 0;
BEGIN

    result_message := ''['' || CURRENT_TIMESTAMP()::STRING || ''] Starting DMVA Wrapper Procedure...\\n'';
    result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Source: '' || P_SOURCE_DATABASE_NAME || ''.'' || P_SOURCE_TABLE_NAME || ''\\n'';
    result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Target: '' || P_TARGET_DATABASE_NAME || ''.'' || P_TARGET_SCHEMA_NAME || ''.'' || P_TARGET_TABLE_NAME || ''\\n'';
    result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Load Type: '' || P_LOAD_TYPE || ''\\n'';
    result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Refresh Structures: '' || P_REFRESH_STRUCTURES_YN || ''\\n'';
    result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Chunking Enabled: '' || P_WITH_CHUNKING_YN || ''\\n'';
    result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Skip Migration Task Waiting: '' || P_SKIP_WAITING_FOR_MIGRATION_TASKS || ''\\n'';

    -- Validate parameters
    IF (UPPER(P_LOAD_TYPE) NOT IN (''FULL'', ''INCREMENTAL'')) THEN
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] ERROR: P_LOAD_TYPE must be either FULL or INCREMENTAL. Provided value: '' || P_LOAD_TYPE || ''\\n'';
        RETURN result_message;
    END IF;

    IF (UPPER(P_REFRESH_STRUCTURES_YN) NOT IN (''Y'', ''N'')) THEN
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] ERROR: P_REFRESH_STRUCTURES_YN must be either Y or N. Provided value: '' || P_REFRESH_STRUCTURES_YN || ''\\n'';
        RETURN result_message;
    END IF;

    IF (UPPER(P_SKIP_WAITING_FOR_MIGRATION_TASKS) NOT IN (''Y'', ''N'')) THEN
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] ERROR: P_SKIP_WAITING_FOR_MIGRATION_TASKS must be either Y or N. Provided value: '' || P_SKIP_WAITING_FOR_MIGRATION_TASKS || ''\\n'';
        RETURN result_message;
    END IF;

    IF (P_WITH_CHUNKING_YN = ''Y'') THEN
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Chunking Column: '' || COALESCE(P_CHUNKING_COLUMN, ''NULL'') || ''\\n'';
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Chunking Value: '' || COALESCE(P_CHUNKING_VALUE, ''NULL'') || ''\\n'';
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Chunking Data Type: '' || COALESCE(P_CHUNKING_DATA_TYPE, ''NULL'') || ''\\n'';
    END IF;

    -- Check if only data migration is needed (P_REFRESH_STRUCTURES_YN = N)
    IF (UPPER(P_REFRESH_STRUCTURES_YN) = ''N'') THEN
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Structure refresh disabled. Skipping to data migration (Step 5 only)...\\n'';
        GOTO STEP_5_DATA_MIGRATION;
    END IF;

    ///////////////////////////////////////////// Step 1 - Configure Mapping Rules /////////////////////////////////////////////
    result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Step 1: Configuring mapping rules...\\n'';

    -- Check if mapping already exists
    SELECT COUNT(*) INTO mapping_record_count
    FROM MIGRATION_TRACKING_V2.dmva_mapping_rules 
    WHERE source_schema_name = :P_SOURCE_DATABASE_NAME 
    AND source_object_name = :P_SOURCE_TABLE_NAME
    AND target_database_name = :P_TARGET_DATABASE_NAME
    AND target_schema_name = :P_TARGET_SCHEMA_NAME
    AND target_object_name = :P_TARGET_TABLE_NAME;

    IF (mapping_record_count = 0) THEN
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Creating new mapping rule...\\n'';
        
        INSERT INTO MIGRATION_TRACKING_V2.dmva_mapping_rules(
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
            NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING_V2.dmva_mapping_rule_id_seq.nextval mapping_rule_id,
            ''teradata_source'' source_system_name,
            ''NA'' source_database_name,
            :P_SOURCE_DATABASE_NAME source_schema_name,
            :P_SOURCE_TABLE_NAME source_object_name,
            ''snowflake_target'' target_system_name,
            :P_TARGET_DATABASE_NAME target_database_name,
            :P_TARGET_SCHEMA_NAME target_schema_name,
            :P_TARGET_TABLE_NAME target_object_name,
            NULL source_object_id,
            NULL target_object_id,
            current_timestamp() created_ts,
            current_timestamp() updated_ts;
        
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Mapping rule created successfully.\\n'';
    ELSE
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Mapping rule already exists ('' || mapping_record_count || '' found).\\n'';
    END IF;

    ///////////////////////////////////////////// Step 2 - Get Metadata /////////////////////////////////////////////
    result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Step 2: Getting metadata to populate dmva objects and dmva column info...\\n'';

    CALL NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING_V2.DMVA_GET_METADATA_TASKS(
        ''teradata_source'',
        parse_json(''{"NA": {"'' || :P_SOURCE_DATABASE_NAME || ''": ["'' || :P_SOURCE_TABLE_NAME || ''"]}}'')
    );

    -- Capture start time for monitoring
    START_TIME := DATEADD(''MINUTE'', -1, sysdate());

    -- Loop to monitor metadata tasks until completion
    result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Monitoring metadata tasks...\\n'';
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
            result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Metadata Loop '' || loop_counter || '': Found '' || running_count || '' RUNNING tasks, waiting 2 seconds...\\n'';
            CALL SYSTEM$WAIT(2, ''SECONDS'');
        END IF;
        loop_counter := loop_counter + 1;
    END WHILE;

    -- Final status report
    IF (running_count = 0) THEN
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] All metadata tasks completed successfully!\\n'';
    ELSE
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Maximum wait time reached for metadata tasks. Some tasks may still be running.\\n'';
    END IF;

    -- Verify metadata was created
    SELECT COUNT(*) INTO object_record_count
    FROM dmva_object_info
    WHERE object_name = :P_SOURCE_TABLE_NAME
    AND schema_name = :P_SOURCE_DATABASE_NAME;

    IF (object_record_count = 0) THEN
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] ERROR: No object metadata found for table '' || P_SOURCE_TABLE_NAME || ''. Cannot proceed.\\n'';
        RETURN result_message;
    END IF;

    result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Step 2 completed: Object metadata verified ('' || object_record_count || '' records found).\\n'';

    ///////////////////////////////////////////// Step 3 - Create Sync Tables /////////////////////////////////////////////
    result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Step 3: Running dmva_create_sync_tables to create targets and mappings...\\n'';

    CALL MIGRATION_TRACKING_V2.dmva_create_sync_tables(
        true,
        false,
        ''teradata_source'',
        parse_json(''{"NA": {"'' || :P_SOURCE_DATABASE_NAME || ''": ["'' || :P_SOURCE_TABLE_NAME || ''"]}}'')
    );

    -- Verify sync tables were created properly
    SELECT COUNT(*) INTO mapping_record_count
    FROM MIGRATION_TRACKING_V2.dmva_source_to_target_mapping a
    INNER JOIN migration_tracking_v2.dmva_object_info b ON
        b.object_name = :P_SOURCE_TABLE_NAME
        AND b.schema_name = :P_SOURCE_DATABASE_NAME
        AND b.object_id = a.source_object_id
    INNER JOIN migration_tracking_v2.dmva_object_info c ON
        c.object_id = a.target_object_id;

    result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Step 3 completed: Sync tables verification ('' || mapping_record_count || '' mappings found).\\n'';

    ///////////////////////////////////////////// Step 4 - Fresh Migration Cleanup (Skip for Incremental) /////////////////////////////////////////////
    IF (UPPER(P_LOAD_TYPE) = ''FULL'') THEN
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Step 4: Performing fresh migration cleanup (FULL load)...\\n'';
        
        -- Delete existing checksums for fresh migration
        DELETE FROM dmva_checksums WHERE checksum_id in(
            SELECT checksum_id
            FROM dmva_checksums a
            INNER JOIN dmva_object_info b ON
                a.object_id = b.object_id
                AND b.object_name = :P_SOURCE_TABLE_NAME
                AND b.schema_name = :P_SOURCE_DATABASE_NAME
        );
        
        GET DIAGNOSTICS checksum_delete_count = ROW_COUNT;
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Deleted '' || checksum_delete_count || '' existing checksum records.\\n'';
        
        -- Check target table count before deletion
        target_tbl_sql := ''SELECT COUNT(1) FROM "'' || P_TARGET_DATABASE_NAME || ''"."'' || P_TARGET_SCHEMA_NAME || ''"."'' || P_TARGET_TABLE_NAME || ''"'';
        LET count_result RESULTSET := (EXECUTE IMMEDIATE :target_tbl_sql);
        LET count_cursor CURSOR FOR count_result;
        OPEN count_cursor;
        FETCH count_cursor INTO target_table_count;
        CLOSE count_cursor;
        
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Target table has '' || target_table_count || '' rows before cleanup.\\n'';
        
        -- Delete existing data from target table for fresh migration
        EXECUTE IMMEDIATE ''DELETE FROM "'' || P_TARGET_DATABASE_NAME || ''"."'' || P_TARGET_SCHEMA_NAME || ''"."'' || P_TARGET_TABLE_NAME || ''"'';
        GET DIAGNOSTICS target_delete_count = ROW_COUNT;
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Deleted '' || target_delete_count || '' rows from target table.\\n'';
        
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Step 4 completed: Fresh migration cleanup completed.\\n'';
    ELSE
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Step 4: Skipping cleanup for INCREMENTAL load.\\n'';
    END IF;

    ///////////////////////////////////////////// Step 4.5 - Configure Chunking (Optional) /////////////////////////////////////////////
    IF (P_WITH_CHUNKING_YN = ''Y'') THEN
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Step 4.5: Configuring chunking method...\\n'';
        
        -- Validate chunking parameters
        IF (P_CHUNKING_COLUMN IS NULL OR P_CHUNKING_VALUE IS NULL OR P_CHUNKING_DATA_TYPE IS NULL) THEN
            result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] ERROR: Chunking enabled but required parameters are missing:\\n'';
            result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] - chunking_column: '' || COALESCE(P_CHUNKING_COLUMN, ''NULL'') || ''\\n'';
            result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] - chunking_value: '' || COALESCE(P_CHUNKING_VALUE, ''NULL'') || ''\\n'';
            result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] - chunking_data_type: '' || COALESCE(P_CHUNKING_DATA_TYPE, ''NULL'') || ''\\n'';
            RETURN result_message;
        END IF;
        
        -- Call the chunking procedure (assuming it exists)
        CALL P_SET_CHUNKING_KEY(:P_SOURCE_DATABASE_NAME, :P_SOURCE_TABLE_NAME, :P_CHUNKING_COLUMN, :P_CHUNKING_VALUE, :P_CHUNKING_DATA_TYPE)
        INTO chunking_result;
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Chunking configuration result:\\n'' || chunking_result || ''\\n'';
        
        -- Check if chunking configuration was successful
        IF (chunking_result LIKE ''%ERROR:%'') THEN
            result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] CRITICAL ERROR: Chunking configuration failed. Migration cannot proceed.\\n'';
            RETURN result_message;
        END IF;
        
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Step 4.5 completed: Chunking configuration successful.\\n'';
    ELSE
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Step 4.5: Skipping chunking configuration (P_WITH_CHUNKING_YN = N).\\n'';
    END IF;

    ///////////////////////////////////////////// Step 5 - Data Migration /////////////////////////////////////////////
    <<STEP_5_DATA_MIGRATION>>
    result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Step 5: Running DMVA_GET_CHECKSUM_TASKS to migrate data...\\n'';

    CALL MIGRATION_TRACKING_V2.DMVA_GET_CHECKSUM_TASKS(
        ''teradata_source'', 
        parse_json(''{"NA": {"'' || :P_SOURCE_DATABASE_NAME || ''": ["'' || :P_SOURCE_TABLE_NAME || ''"]}}'')
    );

    -- Capture start time for migration monitoring
    MIGRATION_START_TIME := DATEADD(''MINUTE'', -1, sysdate());

    ///////////////////////////////////////////// Step 6 - Monitor Migration Tasks /////////////////////////////////////////////
    IF (UPPER(P_SKIP_WAITING_FOR_MIGRATION_TASKS) = ''N'') THEN
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Step 6: Monitoring migration tasks...\\n'';
        
        -- Loop to monitor migration tasks until completion
        running_count := 1;
        loop_counter := 0;
        max_loops := 2400; -- Maximum 2400 loops (200 minutes at 5 seconds each)
        
        WHILE (running_count > 0 AND loop_counter < max_loops) DO
            -- Check for running tasks specific to our table
            SELECT COUNT(*)
            INTO running_count
            FROM dmva_tasks A
            INNER JOIN DMVA_OBJECT_INFO B ON
                B.OBJECT_ID = A.SOURCE_OBJECT_ID
                AND B.OBJECT_NAME = :P_SOURCE_TABLE_NAME
                AND B.SCHEMA_NAME = :P_SOURCE_DATABASE_NAME
            WHERE (A.status_cd = ''RUNNING'' or A.status_cd IS NULL)
            AND A.queue_ts >= :MIGRATION_START_TIME;
            
            -- If still running, wait 2 seconds
            IF (running_count > 0) THEN
                result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Migration Loop '' || loop_counter || '': Found '' || running_count || '' RUNNING tasks, waiting 2 seconds...\\n'';
                CALL SYSTEM$WAIT(2, ''SECONDS'');
            END IF;
            loop_counter := loop_counter + 1;
        END WHILE;
        
        -- Final migration status report
        IF (running_count = 0) THEN
            result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] All migration tasks completed successfully!\\n'';
        ELSE
            result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Maximum wait time reached for migration tasks. Some tasks may still be running.\\n'';
        END IF;
    ELSE
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Step 6: Skipping migration task monitoring (P_SKIP_WAITING_FOR_MIGRATION_TASKS = Y).\\n'';
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Migration tasks have been initiated but monitoring is skipped as requested.\\n'';
    END IF;

    ///////////////////////////////////////////// Final Step - Completion /////////////////////////////////////////////
    result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] DMVA Wrapper Procedure completed!\\n'';
    result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Please verify data in table: '' || P_TARGET_DATABASE_NAME || ''.'' || P_TARGET_SCHEMA_NAME || ''.'' || P_TARGET_TABLE_NAME || ''\\n'';
    
    -- Final verification count
    target_tbl_sql := ''SELECT COUNT(1) FROM "'' || P_TARGET_DATABASE_NAME || ''"."'' || P_TARGET_SCHEMA_NAME || ''"."'' || P_TARGET_TABLE_NAME || ''"'';
    LET final_count_result RESULTSET := (EXECUTE IMMEDIATE :target_tbl_sql);
    LET final_count_cursor CURSOR FOR final_count_result;
    OPEN final_count_cursor;
    FETCH final_count_cursor INTO target_table_count;
    CLOSE final_count_cursor;
    
    result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] Final target table count: '' || target_table_count || '' rows.\\n'';
    
    RETURN result_message;

EXCEPTION
    WHEN OTHER THEN
        result_message := result_message || ''['' || CURRENT_TIMESTAMP()::STRING || ''] ERROR: '' || SQLERRM || ''\\n'';
        RETURN result_message;
END;
'; 