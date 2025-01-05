-- DROP PROCEDURE logs.logging_function(varchar, varchar, varchar, timestamptz, int4);

CREATE OR REPLACE PROCEDURE logs.logging_function(IN table_name_param character varying, IN schema_name_param character varying, IN action_param character varying, IN start_time_param timestamp with time zone, IN rows_count integer)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    end_time timestamp = clock_timestamp();
    duration_param interval;
BEGIN
    RAISE NOTICE 'the time start %  and time_end % ', start_time_param, end_time;
    duration_param = end_time - start_time_param;
    INSERT INTO logs.logs_table(table_name, start_time, end_time, duration, count_changed_rows, schema_name, action)
    VALUES (table_name_param, start_time_param, end_time, duration_param, rows_count, schema_name_param,action_param);
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'error: %!!!', sqlerrm;
END;
$procedure$
;