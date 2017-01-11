SELECT
        *
FROM
(
SELECT 
        TO_CHAR(date, 'MM-DD-YYYY') AS date,
        ROUND(
        ((cost +
        lag(cost, 1) over (order by convert_timezone('GMT', 'America/Los_Angeles', date)) +
        lag(cost, 2) over (order by convert_timezone('GMT', 'America/Los_Angeles', date)) +
        lag(cost, 3) over (order by convert_timezone('GMT', 'America/Los_Angeles', date)) +
        lead(cost, 1) over (order by convert_timezone('GMT', 'America/Los_Angeles', date)) +
        lead(cost, 2) over (order by convert_timezone('GMT', 'America/Los_Angeles', date)) +
        lead(cost, 3) over (order by convert_timezone('GMT', 'America/Los_Angeles', date))) 
        /7.0), 0) AS "Adwords Spend"
FROM
(
SELECT 
        date,
        SUM(cost) AS cost
FROM 
        adwords.campaigns
WHERE 
        date >= DATE_ADD('day', -186,   convert_timezone('UTC', 'America/Los_Angeles', GETDATE()))  
AND     date <   (GETDATE() -1 )
GROUP BY 
        date
)
)
WHERE 
        "Adwords Spend" IS NOT NULL
ORDER BY 
        date