
-- DROP PROCEDURE dm.get_posting_data_by_date();

CREATE OR REPLACE PROCEDURE dm.get_posting_data_by_date()
 LANGUAGE sql
AS $procedure$
	truncate dm.posting_data_by_date;
	insert into dm.posting_data_by_date(
	operation_date,
	debit_amount,
	credit_amount,
	row_timestamp
	)
SELECT 
    oper_date::DATE,
    SUM(debet_amount) AS debit_amount,
    SUM(credit_amount) AS credit_amount,
    NOW() AS row_timestamp
FROM stage.ft_posting_f stp
GROUP BY 1 ;
$procedure$
;