


--  Процедуры для перезагрузки данных в витрину
-- Процедура 1: Полная перезагрузка данных
-- Процедура dm.fill_account_balance выполняет полное удаление всех данных из таблицы dm.account_balance_turnover и последующую загрузку актуальных данных:

create or replace procedure dm.fill_account_balance_turnover()
language plpgsql
as $$
	begin
		truncate table dm.account_balance_turnover;
				
		INSERT INTO dm.account_balance_turnover
		(account_rk, currency_name, department_rk, effective_date, account_in_sum, account_out_sum)
		SELECT a.account_rk,
			   COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name,
			   a.department_rk,
			   ab.effective_date,
			   case
			   		when ab.account_in_sum != lag(account_out_sum) over(partition by a.account_rk order by ab.effective_date)
			   		then lag(account_out_sum) over(partition by a.account_rk order by ab.effective_date)
			   		else ab.account_in_sum
			   end as new_account_in,
			   ab.account_out_sum
		FROM rd.account a
		LEFT JOIN rd.account_balance ab ON a.account_rk = ab.account_rk
		LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd;
		
		exception
			when others then raise exception 'error: %',sqlerrm;
	end;
$$;


-- Процедура 2: Обновление некорректных значений
-- Процедура dm.fill_account_balance_2 обновляет некорректные значения в таблице dm.account_balance_turnover, основываясь на данных из таблицы источника rd.account_balance

create or replace procedure dm.fill_account_balance_turnover_2()
language plpgsql
as $$
	begin

		update dm.account_balance_turnover ab
		set account_out_sum = r.account_out_sum
		from rd.account_balance r
		where ab.account_rk = r.account_rk 
				and ab.effective_date = r.effective_date 
				and ab.account_out_sum != r.account_out_sum
		exception
			when others then raise exception 'error: %',sqlerrm;
	end;
$$;

