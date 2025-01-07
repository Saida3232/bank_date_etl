
CREATE OR REPLACE PROCEDURE logs.logg_function(IN table_name_param character varying, IN schema_name_param character varying,status_param varchar, IN message_param text)
 LANGUAGE plpgsql
AS $procedure$
DECLARE 
    error_message text;
    start_time TIMESTAMP := now();
BEGIN
    RAISE NOTICE 'the time log : %', start_time;

    INSERT INTO logs.table_logs(log_time, table_name, schema_name,status, message)
    VALUES (start_time, table_name_param, schema_name_param,status_param, message_param);
    
EXCEPTION
    WHEN OTHERS THEN
        error_message := sqlerrm; 
        INSERT INTO logs.table_logs(log_time, table_name, schema_name, error_message)
        VALUES (start_time, table_name_param, schema_name_param, error_message);
        RAISE NOTICE 'error: %!!!', error_message;
END;
$procedure$
;