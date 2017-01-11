SELECT SUM(weighted)/SUM(ccs) AS arpu
FROM
(
	SELECT	cohort, COUNT(account_id) AS ccs, SUM(typical_monthly_mrr)/COUNT(account_id) AS arpu,
			COUNT(account_id)*SUM(typical_monthly_mrr)/COUNT(account_id) AS weighted
	FROM
		(SELECT account_id, 
				DATE_TRUNC('months', first_bill_date)::DATE AS cohort,
				typical_monthly_mrr
		FROM 	data_warehouse.salesforce_transfer
		WHERE	cc_date_added IS NOT NULL
		AND		first_bill_date IS NOT NULL
		AND 	ignore_data = 0)
	GROUP BY cohort
	ORDER BY cohort
)