
SELECT
	DISTINCT(cohort) AS cohort,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 0 THEN 1 ELSE NULL END) AS invoice_1,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 1 THEN 1 ELSE NULL END) AS invoice_2,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 2 THEN 1 ELSE NULL END) AS invoice_3,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 3 THEN 1 ELSE NULL END) AS invoice_4,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 4 THEN 1 ELSE NULL END) AS invoice_5,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 5 THEN 1 ELSE NULL END) AS invoice_6,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 6 THEN 1 ELSE NULL END) AS invoice_7,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 7 THEN 1 ELSE NULL END) AS invoice_8,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 8 THEN 1 ELSE NULL END) AS invoice_9,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 9 THEN 1 ELSE NULL END) AS invoice_10,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 10 THEN 1 ELSE NULL END) AS invoice_11,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 11 THEN 1 ELSE NULL END) AS invoice_12,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 12 THEN 1 ELSE NULL END) AS invoice_13,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 13 THEN 1 ELSE NULL END) AS invoice_14,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 14 THEN 1 ELSE NULL END) AS invoice_15,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 15 THEN 1 ELSE NULL END) AS invoice_16,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 16 THEN 1 ELSE NULL END) AS invoice_17,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 17 THEN 1 ELSE NULL END) AS invoice_18,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 18 THEN 1 ELSE NULL END) AS invoice_19,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 19 THEN 1 ELSE NULL END) AS invoice_20,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 20 THEN 1 ELSE NULL END) AS invoice_21,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 21 THEN 1 ELSE NULL END) AS invoice_22,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 22 THEN 1 ELSE NULL END) AS invoice_23,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 23 THEN 1 ELSE NULL END) AS invoice_24,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 24 THEN 1 ELSE NULL END) AS invoice_25,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 25 THEN 1 ELSE NULL END) AS invoice_26,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 26 THEN 1 ELSE NULL END) AS invoice_27,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 27 THEN 1 ELSE NULL END) AS invoice_28,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 28 THEN 1 ELSE NULL END) AS invoice_29,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 29 THEN 1 ELSE NULL END) AS invoice_30,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 30 THEN 1 ELSE NULL END) AS invoice_31,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 31 THEN 1 ELSE NULL END) AS invoice_32,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 32 THEN 1 ELSE NULL END) AS invoice_33,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 33 THEN 1 ELSE NULL END) AS invoice_34,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 34 THEN 1 ELSE NULL END) AS invoice_35,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 35 THEN 1 ELSE NULL END) AS invoice_36,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 36 THEN 1 ELSE NULL END) AS invoice_37,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 37 THEN 1 ELSE NULL END) AS invoice_38,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 38 THEN 1 ELSE NULL END) AS invoice_39,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 39 THEN 1 ELSE NULL END) AS invoice_40,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 40 THEN 1 ELSE NULL END) AS invoice_41,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 41 THEN 1 ELSE NULL END) AS invoice_42,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 42 THEN 1 ELSE NULL END) AS invoice_43,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 43 THEN 1 ELSE NULL END) AS invoice_44,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 44 THEN 1 ELSE NULL END) AS invoice_45,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 45 THEN 1 ELSE NULL END) AS invoice_46,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 46 THEN 1 ELSE NULL END) AS invoice_47,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 47 THEN 1 ELSE NULL END) AS invoice_48,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 48 THEN 1 ELSE NULL END) AS invoice_49,
	SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 49 THEN 1 ELSE NULL END) AS invoice_50
FROM
(           
SELECT 
	id, 
	account_id, 
	concat(invoice_year_month::text, '-01')::date AS invoice_year_month,						
	date_trunc('month', first_bill_date)::date AS cohort
FROM data_warehouse.account_financial_mrr
WHERE count_toward_mrr = 1 
AND deleted_at IS NULL 
AND invoice_date IS NOT NULL
ORDER BY cohort, invoice_year_month
)
GROUP BY cohort
ORDER BY cohort
