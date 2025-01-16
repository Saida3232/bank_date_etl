create or replace procedure dm.fill_f101_round_f(on_date_param date)
language plpgsql
as $$
declare
	need_month date;
	last_day_month date;
	changed_rows int;
	message text;
begin
	need_month = (on_date_param - interval '1 month')::date;
	last_day_month = (on_date_param - interval '1 day')::date;


INSERT INTO dm.dm_f101_round_f
(from_date, to_date, chapter, ledger_account, characteristic, balance_in_rub, balance_in_val, balance_in_total, turn_deb_rub, turn_deb_val, turn_deb_total, turn_cre_rub, turn_cre_val, turn_cre_total, balance_out_rub, balance_out_val, balance_out_total)
select max(need_month) as from_date, 
	   max(last_day_month) as to_date,
	   chapter,
	   left(mad.account_number,5) as ledger_account, 
	   mad.char_type as CHARACTERISTIC ,
	   SUM(CASE 
                WHEN mad.currency_code IN ('810', '643')  and dbf.on_date = need_month - interval '1 day'
                THEN dbf.balance_out_rub
                ELSE 0 
            END) AS BALANCE_IN_RUB,
       SUM(CASE 
                WHEN mad.currency_code not IN ('810', '643') and dbf.on_date = need_month - interval '1 day'
                THEN dbf.balance_out_rub 
                ELSE 0 
            END) AS BALANCE_IN_VAL,
      SUM(dbf.balance_out_rub) filter(where dbf.on_date = need_month - interval '1 day') BALANCE_IN_TOTAL,
      sum(case
	      		when mad.currency_code in ('890','643') and date_trunc('month', dtfbd.on_date)::date = need_month
	      		then dtfbd.debet_amount_rub
	      		else 0
      		end) as TURN_DEB_RUB,
      sum(case
	      		when mad.currency_code not in ('890','643') and date_trunc('month', dtfbd.on_date)::date = need_month
	      		then dtfbd.debet_amount_rub
	      		else 0
      		end) as TURN_DEB_VAL,
      sum(dtfbd.debet_amount_rub) filter(where date_trunc('month', dtfbd.on_date)::date = need_month)as TURN_DEB_TOTAL,
            sum(case
	      		when mad.currency_code in ('890','643') and date_trunc('month', dtfbd.on_date)::date = need_month
	      		then dtfbd.credit_amount_rub 
	      		else 0
      		end) as TURN_CRE_RUB,
      sum(case
	      		when mad.currency_code not in ('890','643') and date_trunc('month', dtfbd.on_date)::date =need_month
	      		then dtfbd.credit_amount_rub
	      		else 0
      		end) as TURN_CRE_VAL,
      sum(dtfbd.credit_amount_rub) filter(where date_trunc('month', dtfbd.on_date)::date = need_month) as TURN_CRE_TOTAL,
      sum(
      	case when mad.currency_code in ('890', '643') and dbf.on_date = last_day_month
      	then dbf.balance_out_rub
      	else 0
      	end) as BALANCE_OUT_RUB,
      sum(
      	case when mad.currency_code not in ('890', '643') and dbf.on_date = last_day_month
      	then dbf.balance_out_rub
      	else 0
      	end) as BALANCE_OUT_VAL,
      sum(dbf.balance_out_rub) as BALANCE_OUT_TOTAL
from ds.MD_ACCOUNT_D mad
left join DM.DM_ACCOUNT_BALANCE_F dbf using(account_rk)
left join ds.md_ledger_account_s  on (left(account_number,5)::int = ledger_account)
left join dm.DM_ACCOUNT_TURNOVER_F dtfbd on mad.account_rk=dtfbd.account_rk
where need_month between data_actual_date and data_actual_end_date 
group by chapter, left(mad.account_number,5), mad.char_type;

get diagnostics changed_rows = ROW_COUNT;
message = format('INSERT INTO %I. Count changed rows = %s', 'dm.dm_f101_round_f', changed_rows);
call logs.logg_function('dm_f101_round_f','dm','succes', message);

exception
	WHEN OTHERS THEN
		message = SQLERRM;
		CALL logs.logg_function('dm_f101_round_f','dm','error', message);
        RAISE EXCEPTION 'Ошибка: %', SQLERRM;
end;
$$;



truncate dm.dm_f101_round_f;
call  dm.fill_f101_round_f('2018-02-01');
select * from dm.dm_f101_round_f;
select * from logs.table_logs;