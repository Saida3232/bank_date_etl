-- ds слой


CREATE USER ds_user WITH PASSWORD 'your_password';
CREATE SCHEMA ds AUTHORIZATION ds_user;
GRANT ALL PRIVILEGES ON SCHEMA DS TO ds_user;

CREATE TABLE ds.ft_balance_f (
	on_date date NOT NULL,
	account_rk int4 NOT NULL,
	currency_rk int4 NULL,
	balance_out float8 NULL,
	CONSTRAINT ft_balance_f_pkey PRIMARY KEY (on_date, account_rk)
);


CREATE TABLE ds.ft_posting_f (
	posting_id serial4 NOT NULL,
	oper_date date NOT NULL,
	credit_account_rk int8 NOT NULL,
	debet_account_rk int8 NOT NULL,
	credit_amount numeric(19, 2) NULL,
	debet_amount numeric(19, 2) NULL
);


CREATE TABLE ds.md_account_d (
	data_actual_date date NOT NULL,
	data_actual_end_date date NOT NULL,
	account_rk int4 NOT NULL,
	account_number varchar(20) NOT NULL,
	char_type varchar(1) NOT NULL,
	currency_rk int4 NOT NULL,
	currency_code varchar(3) NOT NULL,
	CONSTRAINT md_account_d_pkey PRIMARY KEY (data_actual_date, account_rk)
);


CREATE TABLE ds.md_currency_d (
	currency_rk int4 NOT NULL,
	data_actual_date date NOT NULL,
	data_actual_end_date date NULL,
	currency_code varchar(6) NULL,
	code_iso_char varchar(6) NULL,
	CONSTRAINT md_currency_d_pkey PRIMARY KEY (currency_rk, data_actual_date)
);



CREATE TABLE ds.md_exchange_rate_d (
	data_actual_date date NOT NULL,
	data_actual_end_date date NULL,
	currency_rk int4 NOT NULL,
	reduced_cource float8 NULL,
	code_iso_num varchar(3) NULL,
	CONSTRAINT md_exchange_rate_d_pkey PRIMARY KEY (data_actual_date, currency_rk)
);



CREATE TABLE ds.md_ledger_account_s (
	chapter bpchar(1) NULL,
	chapter_name varchar(16) NULL,
	section_number int4 NULL,
	section_name varchar(22) NULL,
	subsection_name varchar(21) NULL,
	ledger1_account int4 NULL,
	ledger1_account_name varchar(47) NULL,
	ledger_account int4 NOT NULL,
	ledger_account_name varchar(153) NULL,
	characteristic bpchar(1) NULL,
	is_resident int4 NULL,
	is_reserve int4 NULL,
	is_reserved int4 NULL,
	is_loan int4 NULL,
	is_reserved_assets int4 NULL,
	is_overdue int4 NULL,
	is_interest int4 NULL,
	pair_account varchar(5) NULL,
	start_date date NOT NULL,
	end_date date NULL,
	is_rub_only int4 NULL,
	min_term varchar(1) NULL,
	min_term_measure varchar(1) NULL,
	max_term varchar(1) NULL,
	max_term_measure varchar(1) NULL,
	ledger_acc_full_name_translit varchar(1) NULL,
	is_revaluation varchar(1) NULL,
	is_correct varchar(1) NULL,
	CONSTRAINT md_ledger_account_s_pkey PRIMARY KEY (ledger_account, start_date)
);
