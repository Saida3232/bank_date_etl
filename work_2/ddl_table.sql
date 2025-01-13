CREATE TABLE dm_account_balance_f (
	account_rk int4 NULL,
	on_date date NULL,
	balance_out numeric(15, 2) NULL,
	balance_out_rub numeric(15, 2) NULL
);

CREATE TABLE dm_account_turnover_f (
	on_date date NULL,
	account_rk numeric NULL,
	credit_amount numeric(23, 8) NULL,
	credit_amount_rub numeric(23, 8) NULL,
	debet_amount numeric(23, 8) NULL,
	debet_amount_rub numeric(23, 8) NULL
);
