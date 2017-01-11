rm(list=ls())
library('DBI')
library('RPostgreSQL')
drv <- dbDriver('PostgreSQL')
con <- dbConnect(drv, 
                 host="################################", 
                 port="########",
                 dbname="#######", 
                 user="#########", 
                 password="################")
query_1 <- "SELECT
	date,
CASE WHEN date IN ('2016-02-26', '2016-02-27') THEN 0 ELSE new_new_plan END as new_new_plan,
CASE WHEN date IN ('2016-02-26', '2016-02-27') THEN 0 ELSE new_new_module END as new_new_module,
CASE WHEN date IN ('2016-02-26', '2016-02-27') THEN 0 ELSE new_new_variable END as new_new_variable,
CASE WHEN date IN ('2016-02-26', '2016-02-27') THEN 0 ELSE new_upgrade_plan END as new_upgrade_plan,
CASE WHEN date IN ('2016-02-26', '2016-02-27') THEN 0 ELSE new_upgrade_module END as new_upgrade_module,
CASE WHEN date IN ('2016-02-26', '2016-02-27') THEN 0 ELSE new_upgrade_variable END as new_upgrade_variable
FROM(
SELECT 
date, 
COALESCE(CASE WHEN new_new_plan = first_new_new_plan THEN new_new_plan 
ELSE new_new_plan - LAG(new_new_plan) OVER (ORDER BY date) END, 0) AS new_new_plan,
COALESCE(CASE WHEN new_new_module = first_new_new_module THEN new_new_module 
ELSE new_new_module - LAG(new_new_module) OVER (ORDER BY date) END , 0) AS new_new_module,
COALESCE(CASE WHEN new_new_variable = first_new_new_variable THEN new_new_variable 
ELSE new_new_variable - LAG(new_new_variable) OVER (ORDER BY date) END , 0) AS new_new_variable,
COALESCE(CASE WHEN new_upgrade_plan = first_new_upgrade_plan THEN new_upgrade_plan
ELSE new_upgrade_plan - LAG(new_upgrade_plan) OVER (ORDER BY date) END , 0) AS new_upgrade_plan,
COALESCE(CASE WHEN new_upgrade_module = first_new_upgrade_module THEN new_upgrade_module 
ELSE new_upgrade_module - LAG(new_upgrade_module) OVER (ORDER BY date) END , 0) AS new_upgrade_module,
COALESCE(CASE WHEN new_upgrade_variable = first_new_upgrade_variable THEN new_upgrade_variable
ELSE new_upgrade_variable - LAG(new_upgrade_variable) OVER (ORDER BY date) END , 0) AS 				  new_upgrade_variable
FROM
(
  SELECT a.date, first_new_new_plan, new_new_plan, first_new_new_module, new_new_module,
  first_new_new_variable, new_new_variable, first_new_upgrade_plan, new_upgrade_plan,
  first_new_upgrade_module, new_upgrade_module, first_new_upgrade_variable, 
  new_upgrade_variable
  FROM 
  (
  SELECT *
  FROM
  (
  SELECT 
  DATE(dateadd(day,730,(getdate()::date - row_number() over (order by true))::date)) as date
  FROM data_warehouse.activity_account 
  )
  WHERE date >= '2015-07-01'
  AND date < CURRENT_DATE
  ORDER BY date
  ) a
  LEFT JOIN
  (
  SELECT  
  log_ymd as date, 
  first_value(new_new_plan) OVER (partition by to_char(log_ymd, 'YYYY-MM')) 
  AS first_new_new_plan,
  new_new_plan,
  first_value(new_new_module) OVER (partition by to_char(log_ymd, 'YYYY-MM')) 
  AS first_new_new_module,
  new_new_module,
  first_value(new_new_variable) OVER (partition by to_char(log_ymd, 'YYYY-MM')) 
  AS first_new_new_variable,
  new_new_variable,
  first_value(new_upgrade_plan) OVER (partition by to_char(log_ymd, 'YYYY-MM')) 
  AS first_new_upgrade_plan,	
  new_upgrade_plan,
  first_value(new_upgrade_module) OVER (partition by to_char(log_ymd, 'YYYY-MM')) 
  AS first_new_upgrade_module,
  new_upgrade_module,
  first_value(new_upgrade_variable) OVER (partition by to_char(log_ymd, 'YYYY-MM')) 
  AS first_new_upgrade_variable,
  new_upgrade_variable
  FROM data_warehouse.account_financial_mrr_log
  WHERE EXTRACT(month FROM cohort_year_month_date) = EXTRACT(month FROM log_ymd)
  AND cohort_year_month_date <= DATE(convert_timezone('GMT','US/Pacific',CURRENT_DATE))
  AND to_char(log_ymd, 'YYYY-MM') = cohort_year_month 
  AND cohort_type = 'all'
  AND deleted_at IS NULL
  ) b
  ON a.date = b.date
)
WHERE date > '2015-11-16'
ORDER BY date
)
ORDER BY date"

res_1 <- dbSendQuery(con, query_1, timeout = 600)
df  <- as.data.frame(dbFetch(res_1)); dbClearResult(res_1)
library(lubridate)
df$months <- paste(year(df$date), '-', month(df$date),sep='')

df$total_new_mrr  <- df$new_new_plan + df$new_new_module + df$new_new_variable    
                   + df$new_upgrade_plan + df$new_upgrade_module +df$new_upgrade_variable

# Add Tuesdays

df$day <- weekdays(as.Date(df$date))
df$Tuesday <- ifelse(df$day == "Tuesday", 1, 0)

# Add order data

query_2 <- "SELECT
DATE(date_added) AS date_added_date, 
COUNT(DISTINCT o.id) AS number_of_orders, 
CASE WHEN DATE(date_added) > '2016-03-08' THEN 1 ELSE 0 END
AS shard_glitch_start
FROM data_warehouse.salesforce_transfer AS salesforce_transfer
LEFT JOIN data_warehouse.order AS o ON (o.account_id) = salesforce_transfer.account_id
WHERE (salesforce_transfer.ignore_data = 0) AND ((((o.date_added) >= ((DATEADD(month,-12, DATE_TRUNC('month', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE()))) ))) AND (o.date_added) < ((DATEADD(month,13, DATEADD(month,-12, DATE_TRUNC('month', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE()))) ) )))))) AND (o.deleted = 0) AND (o.historical_import = 0) AND (o.void = 0)
GROUP BY 1
ORDER BY 1 DESC
"
res_2 <- dbSendQuery(con, query_2, timeout = 600)
order_df  <- as.data.frame(dbFetch(res_2)); dbClearResult(res_2)
order_df$date <- order_df$date_added
order_df$date_added <- NULL

df <- merge(x = df, y = order_df, by.x = "date",
            by.y = "date", all.x = TRUE)

# Add the Adwords data

query_3 <- "SELECT 
date,
SUM(CASE WHEN campaign_id IN (195508785, 195508665, 195509025, 195508905, 121102665, 195508545) 
    THEN cost ELSE 0 END) AS Brand,
SUM(CASE WHEN campaign_id IN (195514905, 138943905, 195514665, 147887865, 195514545, 195515025, 
    195514785, 158181225, 195608745, 179973825, 197051505, 195961665) 
    THEN cost ELSE 0 END) AS Generic,
SUM(CASE WHEN campaign_id IN (179781345) THEN cost ELSE 0 END) AS Remarketing,
SUM(CASE WHEN campaign_id IN (196116465, 196116345) THEN cost ELSE 0 END) AS Competitor
FROM
  adwords._campaign_performance_report
GROUP BY date
ORDER BY date"

#dbDisconnect(con)
res_3 <- dbSendQuery(con, query_3, timeout = 600)
adwords_df  <- as.data.frame(dbFetch(res_3)); dbClearResult(res_3)

df <- merge(x = df, y = adwords_df, by.x = "date", by.y = "date", all.x = TRUE)

# Create Lags
library(dplyr)
working_df  <- 
  df %>%  mutate(order_lag15 = lag(number_of_orders, 15)) 
working_df  <- 
  working_df %>%  mutate(brand_lag29 = lag(brand, 29)) 


# Create a forecast for brand spend. Brad estimates brand spend as varying from
# 100% to 90% of its current rate
library(zoo)
rolling_mean <- (rollapply(zoo(df$brand), 30, mean, fill = NA))
recent_mean <- rolling_mean[complete.cases(rolling_mean),]
brand_spend <- recent_mean[length(recent_mean)] 
brand_spend <- as.data.frame(rep((brand_spend*.95), 365)) 
brand_spend$brand <- brand_spend[,1] 
brand_spend[,1] <- NULL
rm(rolling_mean, recent_mean)

# Create a forecast for the order input

orders <- ts(order_df$number_of_orders, frequency=7)
order_df$December <- ifelse(substr(order_df$date, 6, 7) == '12', 1, 0)

order_model <- Arima(orders, 
                       order=c(4,1,1), 
                       seasonal=c(1,1,1),
                       xreg=cbind(order_df$shard_glitch_star, order_df$December))

# Create a vector of 1's for the shard glitch

ones_vector <- as.data.frame(rep(1, 365)) 
ones_vector$glitch <- rep(1, 365)
ones_vector$'rep(1, 365)' <- NULL
december_vector <- as.data.frame(seq(Sys.Date(), to= Sys.Date()+364, by = "day"))
december_vector$date <- december_vector$'seq(Sys.Date(), to = Sys.Date() + 364, by = \"day\"'
december_vector[,1:1] <-NULL
december_vector$Dec <- ifelse(substr(december_vector$date, 6, 7) == 12, 1, 0)

order_model <- Arima(orders, 
                     order=c(4,1,1), 
                     seasonal=c(1,1,1),
                     xreg=cbind(order_df$shard_glitch_star, order_df$December))

library(forecast)
order_forecast <- forecast(order_model, 
                           xreg = cbind(ones_vector$glitch,
                           december_vector$Dec))

order_forecast <- as.data.frame(order_forecast$mean)
order_forecast$date <- seq(Sys.Date(), to= Sys.Date()+364, by = "day")
order_forecast$number_of_orders <- order_forecast$x
order_forecast$x <- NULL
myvars <- c("date", "number_of_orders")
original_orders <- df[myvars]
order_forecast <- rbind(original_orders, order_forecast)
order_forecast <- 
  order_forecast %>% mutate(order_forecast_lagged = lag(number_of_orders, 15))
new_order_forecast <- order_forecast[ which(order_forecast$date > (Sys.Date()-1)), ]
#new_order_forecast <- new_order_forecast[-1,]
rm(december_vector, ones_vector, order_forecast, original_orders, order_df)

# Create a vector of Tuesdays

new_order_forecast$day <- weekdays(as.Date(new_order_forecast$date))
new_order_forecast$Tuesday <- 
  ifelse(new_order_forecast$day == "Tuesday", 1, 0)

# Apply regressors 
screening_df <- working_df[ which(working_df$date > '2015-12-17'), ]
x <- ts(screening_df$total_new_mrr, frequency=31)

model <- Arima(x, 
                     order=c(0,1,1), 
                     seasonal=c(0,1,1),
                     xreg=cbind(screening_df$brand_lag29,
                                screening_df$order_lag15, 
                                screening_df$Tuesday))
forecast <- as.data.frame(forecast(model, xreg=
                        cbind(brand_spend$brand,
                        new_order_forecast$order_forecast_lagged, 
                        new_order_forecast$Tuesday)
                        ))
forecast$date <- seq(Sys.Date(), to= Sys.Date()+364, by = "day")

original_forecast <- as.data.frame(forecast(model, 
                     xreg=
                     cbind(screening_df$brand_lag29,
                           screening_df$order_lag15, 
                           screening_df$Tuesday)
                     ))
original_forecast$date <- seq(as.Date('2015-12-18'), 
                          to= as.Date('2015-12-18') + 181, by = "day")

myvars1 <- c("date", "total_new_mrr")
recon_part1 <- screening_df[myvars1]
myvars2 <- c("date", "Point Forecast")
recon_part2 <- original_forecast[myvars2]

reconciliation <-  merge(x = recon_part1, 
                         y = recon_part2, 
                         by.x = "date", by.y = "date", 
                         all.x = TRUE)

rm(myvars1, myvars2, recon_part1, recon_part2)


plot(reconciliation$date, reconciliation$total_new_mrr, type='l')
par(new=T)
plot(reconciliation$date, reconciliation$'Point Forecast', type='b')
par(new=F)

reconciliation$diff <- reconciliation$total_new_mrr - 
                       reconciliation$'Point Forecast'
mean_diff <- mean(reconciliation[["diff"]], na.rm=TRUE)

correctedforecast <- forecast
correctedforecast$Adjusted_point <- forecast$'Point Forecast' + mean_diff
correctedforecast$Adjusted_lo_80 <- forecast$'Lo 80' + mean_diff
correctedforecast$Adjusted_hi_80 <- forecast$'Hi 80' + mean_diff
correctedforecast$Adjusted_lo_95 <- forecast$'Lo 95' + mean_diff
correctedforecast$Adjusted_hi_95 <- forecast$'Hi 95' + mean_diff
correctedforecast$"Point Forecast" <- NULL
correctedforecast$"Lo 80" <- NULL          
correctedforecast$"Hi 80" <- NULL          
correctedforecast$"Lo 95" <- NULL          
correctedforecast$"Hi 95" <- NULL         

rm(adwords_df, brand_spend, df, new_order_forecast,
   screening_df, working_df, con, drv, model,
   myvars, order_model, orders, query_1, query_2,
   query_3, res_1, res_2, res_3, x, reconciliation,
   original_forecast, mean_diff)