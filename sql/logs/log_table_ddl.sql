CREATE USER logs_user WITH PASSWORD 'your_password';
CREATE SCHEMA logs AUTHORIZATION logs_user;
GRANT ALL PRIVILEGES ON SCHEMA logs TO logs_user;

CREATE TABLE logs.table_logs (
	id serial PRIMARY KEY,
	log_time timestamp not NULL,
	table_name varchar not NULL,
	schema_name varchar not NULL,
	message text not NULL,
	status varchar not null
);