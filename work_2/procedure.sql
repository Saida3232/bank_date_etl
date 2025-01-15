
CREATE OR REPLACE PROCEDURE dm.fill_account_turnover_f(IN i_ondate date)
 LANGUAGE plpgsql
AS $procedure$
	begin
		delete from dm.DM_ACCOUNT_TURNOVER_F
		where on_date = i_OnDate;

		insert into dm.DM_ACCOUNT_TURNOVER_F
		(on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
		with 
			debet_operation as (
				select oper_date, debet_account_rk ,
						sum(debet_amount) debet_amount, 
						sum(debet_account_rk* coalesce(reduced_cource,1)) debet_amount_rub
				from ds.ft_posting_f fpf 
				left join ds.ft_balance_f fbf on fpf.debet_account_rk= fbf.account_rk and fpf.oper_date= fbf.on_date 
				left join ds.md_exchange_rate_d merd  on fbf.currency_rk=merd.currency_rk and  oper_date between data_actual_date and data_actual_end_date
				where credit_amount is  not null and oper_date = i_OnDate
				group by oper_date, debet_account_rk
),
			credit_operation as (
				select oper_date, credit_account_rk, sum(credit_amount) credit_amount, sum(credit_amount * COALESCE(reduced_cource, 1)) credit_amount_rub
				from ds.ft_posting_f fpf 
				left join ds.ft_balance_f fbf on fpf.credit_account_rk= fbf.account_rk and fpf.oper_date= fbf.on_date 
				left join ds.md_exchange_rate_d merd  on  merd.currency_rk = fbf.currency_rk and oper_date between data_actual_date and data_actual_end_date
				where debet_amount is not null and oper_date = i_OnDate
				group by credit_account_rk,oper_date
		)
		select d_o.oper_date, debet_account_rk as account_rk, debet_amount, debet_amount_rub , credit_amount,credit_amount_rub
		from debet_operation d_o
		join credit_operation c_o on d_o.debet_account_rk = c_o.credit_account_rk;
	exception 
		when others then 
			raise exception 'error:%',sqlerrm;
	end;
$procedure$
;



DO
$$
DECLARE
    i INT;
BEGIN
    FOR i IN 1..30 LOOP
        CALL dm.fill_account_turnover_f(to_date('2018-01-' || i, 'YYYY-MM-DD'));
    END LOOP;
END
$$;


create or replace procedure dm.fill_dm_balance_out_f(On_date_param date)
language plpgsql
as $$
begin

	INSERT INTO dm.dm_account_balance_f
	(account_rk, on_date, balance_out, balance_out_rub)
	with table_a as (select m.account_rk , t.on_date ,
		case 	
			when m.char_type ='А' then t.balance_out - coalesce(d.credit_amount, 0) + coalesce (d.debet_amount,0)
			when m.char_type ='П' then t.balance_out + coalesce(d.credit_amount, 0) - coalesce (d.debet_amount,0)
		end as balance_out 
	from dm.dm_account_balance_f  t
	left join dm.dm_account_turnover_f d on  t.account_rk=d.account_rk and d.on_date = t.on_date
	right join DS.md_account_d m on t.account_rk=m.account_rk
	where t.on_date = On_date_param - interval '1 day'
	)
	select ta.account_rk ,On_date_param , ta.balance_out,ta.balance_out * coalesce(merd.reduced_cource,1) as new_balance_rub
	from table_a ta
	left join ds.ft_balance_f fbf on ta.account_rk= fbf.account_rk and ta.on_date= fbf.on_date 
	left join ds.md_exchange_rate_d merd on  fbf.currency_rk=merd.currency_rk and ta.on_date between merd.data_actual_date and merd.data_actual_end_date; 

	exception
		when others then raise exception 'error: %',sqlerrm;
	end;
$$;

INSERT INTO dm.dm_account_balance_f
(account_rk, on_date, balance_out, balance_out_rub)
select fbf.account_rk, fbf.on_date,balance_out, balance_out * coalesce(reduced_cource,1)
from ds.ft_balance_f fbf
left join ds.md_exchange_rate_d merd on  fbf.currency_rk=merd.currency_rk and fbf.on_date between merd.data_actual_date and merd.data_actual_end_date 
where fbf.on_date = '2017-12-31'


DO
$$
DECLARE
    i INT;
BEGIN
    FOR i IN 1..30 LOOP
        CALL dm.fill_dm_balance_out_f(to_date('2018-01-' || i, 'YYYY-MM-DD'));
    END LOOP;
END
$$;

