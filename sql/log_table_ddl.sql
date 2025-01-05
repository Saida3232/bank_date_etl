CREATE TABLE logs.logs_table (
	table_name varchar NULL,
	start_time timestamp NULL,
	end_time timestamp NULL,
	duration interval NULL,
	count_changed_rows int4 NULL,
	schema_name varchar NULL,
	"action" varchar NULL
);