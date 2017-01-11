SELECT
        "Week",
        CASE WHEN "Week" = DATE_TRUNC('week', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE()))::DATE 
                      AND DATE_PART('dow', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE())) = 1
                          THEN NULL ELSE "Monday"     END AS "Monday",
        CASE WHEN "Week" = DATE_TRUNC('week', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE()))::DATE 
                      AND 
                      (
                      DATE_PART('dow', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE())) <= 2 
                      AND DATE_PART('dow', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE())) != 0 
                      )
                          THEN NULL ELSE "Tuesday"    END AS "Tuesday",
        CASE WHEN "Week" = DATE_TRUNC('week', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE()))::DATE 
                      AND 
                      (
                      DATE_PART('dow', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE())) <= 3
                      AND DATE_PART('dow', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE())) != 0
                      )
                          THEN NULL ELSE "Wednesday"  END AS "Wednesday",
        CASE WHEN "Week" = DATE_TRUNC('week', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE()))::DATE 
                      AND 
                      (
                      DATE_PART('dow', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE())) <= 4
                      AND DATE_PART('dow', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE())) != 0
                      )
                          THEN NULL ELSE "Thursday"   END AS "Thursday",
        CASE WHEN "Week" = DATE_TRUNC('week', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE()))::DATE 
                      AND 
                      (
                      DATE_PART('dow', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE())) <= 5 
                      AND DATE_PART('dow', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE())) != 0
                      )
                          THEN NULL ELSE "Friday"     END AS "Friday", 
        CASE WHEN "Week" = DATE_TRUNC('week', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE()))::DATE 
                      AND 
                      (
                      DATE_PART('dow', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE())) <= 6 
                      AND DATE_PART('dow', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE())) != 0
                      )
                          THEN NULL ELSE "Saturday"   END AS "Saturday",
        CASE WHEN "Week" = DATE_TRUNC('week', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE()))::DATE 
                --      AND DATE_PART('dow', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE())) NOT IN (1, 2, 3, 4, 5, 6)
                          THEN NULL ELSE "Sunday"     END AS "Sunday"
FROM
    (
    SELECT 
            DATE_TRUNC('week', reg_date)::DATE AS "Week",
            SUM(CASE WHEN DATE_PART('dow', reg_date) = 0 THEN 1 ELSE 0 END) AS "Sunday",
            SUM(CASE WHEN DATE_PART('dow', reg_date) = 1 THEN 1 ELSE 0 END) AS "Monday",
            SUM(CASE WHEN DATE_PART('dow', reg_date) = 2 THEN 1 ELSE 0 END) AS "Tuesday",
            SUM(CASE WHEN DATE_PART('dow', reg_date) = 3 THEN 1 ELSE 0 END) AS "Wednesday",
            SUM(CASE WHEN DATE_PART('dow', reg_date) = 4 THEN 1 ELSE 0 END) AS "Thursday",
            SUM(CASE WHEN DATE_PART('dow', reg_date) = 5 THEN 1 ELSE 0 END) AS "Friday",
            SUM(CASE WHEN DATE_PART('dow', reg_date) = 6 THEN 1 ELSE 0 END) AS "Saturday"
    FROM 
    		data_warehouse.account 
    WHERE   
            ignore_data = 0
            AND reg_date > DATEADD('day', -56, 
                          DATE_TRUNC('week', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE()))) 
            AND reg_date < CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE())
    GROUP BY 1
    )
ORDER BY 1 