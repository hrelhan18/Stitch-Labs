SELECT *
FROM
(
SELECT 
        TO_CHAR(date, 'MM-DD-YYYY') AS date,
        -- CAST(SUBSTRING(TO_CHAR(date, 'MM-DD-YYYY'), 7, 4) AS INT),
        -- DATE_TRUNC('year', date)::date AS year,
        -- DATE_TRUNC('month', date)::date AS month,
        -- COUNT(DISTINCT(domain_userid)) AS "Page Views (non-blog)",
        ROUND((COUNT(DISTINCT(domain_userid)) +
        lag(COUNT(DISTINCT(domain_userid)), 1) over (order by convert_timezone('GMT', 'America/Los_Angeles', date)) +
        lag(COUNT(DISTINCT(domain_userid)), 2) over (order by convert_timezone('GMT', 'America/Los_Angeles', date)) +
        lag(COUNT(DISTINCT(domain_userid)), 3) over (order by convert_timezone('GMT', 'America/Los_Angeles', date)) +
        lead(COUNT(DISTINCT(domain_userid)), 1) over (order by convert_timezone('GMT', 'America/Los_Angeles', date)) +
        lead(COUNT(DISTINCT(domain_userid)), 2) over (order by convert_timezone('GMT', 'America/Los_Angeles', date)) +
        lead(COUNT(DISTINCT(domain_userid)), 3) over (order by convert_timezone('GMT', 'America/Los_Angeles', date)) )
        /7.0, 0) AS "Marketing Site Visitors"
FROM
    (
    SELECT 
            domain_userid,
            convert_timezone('UTC', 'America/Los_Angeles', first_value(collector_tstamp)
                OVER  (
                      PARTITION BY app_id, domain_userid, collector_tstamp::DATE
                      ORDER BY collector_tstamp rows between unbounded preceding 
                      AND unbounded following
                      ))::DATE AS date  
    FROM 
            snowplow_atomic.events
    WHERE 
            event = 'page_view'
            AND app_id = 'marketing'-- 'support', 'stitchlabs',
            AND page_url NOT ILIKE '%http://www.stitchlabs.com/blog%'
    )
WHERE date != DATE_TRUNC('day', GETDATE())   
AND  date > DATE_ADD('day', -186,   GETDATE())   
GROUP BY date
) WHERE "Marketing Site Visitors" IS NOT NULL
ORDER BY 1