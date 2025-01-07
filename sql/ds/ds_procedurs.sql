CREATE OR REPLACE PROCEDURE ds.stage_to_ds_ft_balance_f()
LANGUAGE plpgsql
AS $procedure$
DECLARE
    changed_rows int;
    message text;
BEGIN
    INSERT INTO ds.ft_balance_f (on_date, account_rk, currency_rk, balance_out)
    SELECT
        on_date::date,
        account_rk,
        currency_rk,
        balance_out
    FROM
        stage.ft_balance_f
    ON CONFLICT (on_date, account_rk)
    DO UPDATE SET 
        currency_rk = EXCLUDED.currency_rk,
        balance_out = EXCLUDED.balance_out;

    GET DIAGNOSTICS 
        changed_rows = ROW_COUNT;

    message := FORMAT('INSERT INTO %I FROM %I. Count changed rows = %s', 'ds.ft_balance_f', 'stage.ft_balance_f', changed_rows);
    CALL logs.logg_function('ft_balance_f', 'ds','success', message);
EXCEPTION
    WHEN OTHERS THEN
		CALL logs.logg_function('ft_balance_f', 'ds','error', sqlerrm);
        RAISE notice 'Ошибка: %', SQLERRM;
END; 
$procedure$;


CREATE OR REPLACE PROCEDURE ds.stage_to_ds_ft_posting_f()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    changed_rows int;
    message text;
begin
	insert
	into
	ds.ft_posting_f (oper_date,
	credit_account_rk,
	debet_account_rk,
	credit_amount,
	debet_amount)
	select
		oper_date::DATE,
		credit_account_rk,
		debet_account_rk,
		credit_amount,
		debet_amount
	from
		stage.ft_posting_f;
	get diagnostics changed_rows= ROW_COUNT;
	message := FORMAT('INSERT INTO %I FROM %I. Count changed rows = %s', 'ds.ft_posting_f', 'stage.ft_posting_f', changed_rows);
    CALL logs.logg_function('ft_posting_f', 'ds','success', message);
    exception
		WHEN OTHERS THEN
		message = SQLERRM;
		CALL logs.logg_function('ft_posting_f', 'ds','error', sqlerrm);
        RAISE EXCEPTION 'Ошибка: %', SQLERRM;
end;
 $procedure$
;

CREATE OR REPLACE PROCEDURE ds.stage_to_md_account_d()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    changed_rows int;
    message text;
begin
	insert
	into
	DS.md_account_d (data_actual_date,
	data_actual_end_date,
	account_rk,
	account_number,
	char_type,
	currency_rk,
	currency_code)
	select
		data_actual_date::date,
		data_actual_end_date::date,
		account_rk,
		account_number,
		char_type,
		currency_rk,
		currency_code
	from
		stage.md_account_d
	ON CONFLICT (data_actual_date,account_rk)
	DO UPDATE SET
		data_actual_end_date = EXCLUDED.data_actual_end_date,
		account_number = EXCLUDED.account_number,
		char_type = EXCLUDED.char_type,
		currency_rk = EXCLUDED.currency_rk,
		currency_code = EXCLUDED.currency_code
	;
	get diagnostics changed_rows= ROW_COUNT;
	message := FORMAT('INSERT INTO %I FROM %I. Count changed rows = %s', 'ds.md_account_d', 'stage.md_account_d', changed_rows);
    CALL logs.logg_function('md_account_d', 'ds','success', message);

	exception
		WHEN OTHERS THEN
			message = SQLERRM;
			CALL logs.logg_function('md_account_d', 'ds','error', sqlerrm);
	        RAISE EXCEPTION 'Ошибка: %', SQLERRM;
end;
 $procedure$
;

-- DROP PROCEDURE ds.stage_to_md_currency_d();

CREATE OR REPLACE PROCEDURE ds.stage_to_md_currency_d()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    changed_rows int;
    message text;
begin
	insert
		into
		ds.md_currency_d (currency_rk,
		data_actual_date,
		data_actual_end_date,
		currency_code,
		code_iso_char)
	select
		currency_rk,
		data_actual_date::date,
		data_actual_end_date::date,
		currency_code,
		code_iso_char
	from
		stage.md_currency_d
	on conflict (currency_rk, data_actual_date)
	do update set 
		currency_rk = EXCLUDED.currency_rk,
		data_actual_end_date = EXCLUDED.data_actual_end_date,
		code_iso_char = EXCLUDED.code_iso_char;
	get diagnostics changed_rows= ROW_COUNT;
    message := FORMAT('INSERT INTO %I FROM %I. Count changed rows = %s', 'ds.md_currency_d', 'stage.md_currency_d', changed_rows);
    CALL logs.logg_function('md_currency_d', 'ds','success', message);

	exception
		WHEN OTHERS THEN
			message = SQLERRM;
			CALL logs.logg_function('md_currency_d', 'ds','error', sqlerrm);
	        RAISE EXCEPTION 'Ошибка: %', SQLERRM;
end;
 $procedure$
;
-- DROP PROCEDURE ds.stage_to_md_exchange_rate_d();

CREATE OR REPLACE PROCEDURE ds.stage_to_md_exchange_rate_d()
 LANGUAGE plpgsql
AS $procedure$
	DECLARE
	    changed_rows int;
	    message text;
	begin
		INSERT INTO ds.md_exchange_rate_d (data_actual_date, data_actual_end_date, currency_rk, reduced_cource, code_iso_num)
	SELECT 
	    data_actual_date::date,
	    data_actual_end_date::date,
	    currency_rk,
	    reduced_cource,
	    code_iso_num
	FROM 
	    stage.md_exchange_rate_d
	ON CONFLICT (currency_rk, data_actual_date)
	DO UPDATE SET
	    data_actual_end_date= EXCLUDED.data_actual_end_date,
	    reduced_cource = EXCLUDED.reduced_cource,
	    code_iso_num = EXCLUDED.code_iso_num;
	get diagnostics changed_rows= ROW_COUNT;
    message := FORMAT('INSERT INTO %I FROM %I. Count changed rows = %s', 'ds.md_exchange_rate_d', 'stage.md_exchange_rate_d', changed_rows);
    CALL logs.logg_function('md_exchange_rate_d', 'ds','success', message);
	exception
		WHEN OTHERS THEN
			message = SQLERRM;
			CALL logs.logg_function('md_exchange_rate_d', 'ds','error', sqlerrm);
	        RAISE EXCEPTION 'Ошибка: %', SQLERRM;
end;
 $procedure$
;



CREATE OR REPLACE PROCEDURE ds.stage_to_md_ledger_account_s()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    changed_rows int;
    message text;
begin
	insert
	into
	ds.md_ledger_account_s (chapter,
	chapter_name,
	section_number,
	section_name,
	subsection_name,
	ledger1_account,
	ledger1_account_name,
	ledger_account,
	ledger_account_name,
	characteristic,
	start_date,
	end_date)
	select
	chapter,
	chapter_name,
	section_number,
	section_name,
	subsection_name,
	ledger1_account,
	ledger1_account_name,
	ledger_account,
	ledger_account_name,
	characteristic,
	start_date::date,
		end_date::date
	from
		stage.md_ledger_account_s
	ON CONFLICT (ledger_account, start_date)
	DO UPDATE SET
		chapter = EXCLUDED.chapter,
		chapter_name = EXCLUDED.chapter_name,
		section_number = EXCLUDED.section_number,
		section_name = EXCLUDED.section_name,
		subsection_name = EXCLUDED.subsection_name,
		ledger1_account = EXCLUDED.ledger1_account,
		ledger1_account_name = EXCLUDED.ledger1_account_name,
		ledger_account_name = EXCLUDED.ledger_account_name,
		characteristic = EXCLUDED.characteristic,
		end_date = EXCLUDED.end_date
	;
	get diagnostics changed_rows= ROW_COUNT;
	message := FORMAT('INSERT INTO %I FROM %I. Count changed rows = %s', 'ds.md_ledger_account_s', 'stage.md_ledger_account_s', changed_rows);
    CALL logs.logg_function('md_ledger_account_s', 'ds','success', message);
	
	exception
		WHEN OTHERS THEN
			message = SQLERRM;
			CALL logs.logg_function('md_ledger_account_s', 'ds','error', sqlerrm);
	        RAISE EXCEPTION 'Ошибка: %', SQLERRM;
end;
 $procedure$
;
