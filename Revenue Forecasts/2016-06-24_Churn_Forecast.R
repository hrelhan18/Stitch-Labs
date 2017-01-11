rm(list=ls())
library('DBI')
library('RPostgreSQL')
drv <- dbDriver('PostgreSQL')
con <- dbConnect(drv, 
                 host="###############################", 
                 port="#####",
                 dbname="####", 
                 user="####", 
                 password="###################")
query_1 <- "SELECT 
	date,
first_value(arr) OVER (partition by arr_partition) AS arr,
COALESCE(churn, 0) AS churn,
first_value(account_holders) OVER (partition by account_holders_partition) AS account_holders,
first_value(arpu) OVER (partition by arpu_partition) AS arpu,
first_value(renewal_account_customer) OVER (partition by renewal_account_partition) AS renewal_account_customers,
(first_value(renewal_account_customer) OVER (partition by renewal_account_partition)*(1.0))/
first_value(account_holders) OVER (partition by account_holders_partition) AS percentage_onboarded
FROM
(               
SELECT 
a.date,
arr,
churn,
account_holders,
arpu,
renewal_account_customer,
SUM(CASE WHEN arr IS NULL THEN 0 ELSE 1 END) 
OVER (ORDER BY a.date ROWS BETWEEN unbounded preceding AND 0 following) AS arr_partition,
SUM(CASE WHEN account_holders IS NULL THEN 0 ELSE 1 END) 
OVER (ORDER BY a.date ROWS BETWEEN unbounded preceding AND 0 following) AS account_holders_partition,
SUM(CASE WHEN arpu IS NULL THEN 0 ELSE 1 END) 
OVER (ORDER BY a.date ROWS BETWEEN unbounded preceding AND 0 following) AS arpu_partition,
SUM(CASE WHEN renewal_account_customer IS NULL THEN 0 ELSE 1 END) 
OVER (ORDER BY a.date ROWS BETWEEN unbounded preceding AND 0 following) AS renewal_account_partition
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
        date, 
        arr,
        CASE WHEN churn = first_churn THEN churn ELSE churn - LAG(churn) OVER (ORDER BY date) END AS churn,
        account_holders,
        arpu,
        renewal_account_customer
        FROM
        (
        SELECT  
        log_ymd as date, 
        arr,
        churn_account * (-1) AS churn,
        (first_value(churn_account) OVER (partition by to_char(log_ymd, 'YYYY-MM'))) * (-1) AS first_churn,
        new_account_including_future  
        + renewal_account_customer + new_account_onboarding + new_account_customer AS account_holders,
        (arr/12)/(new_account_including_future 
        + renewal_account_customer + new_account_onboarding + new_account_customer) AS arpu, 
        renewal_account_customer          
        FROM data_warehouse.account_financial_mrr_log
        WHERE EXTRACT(month FROM cohort_year_month_date) = EXTRACT(month FROM log_ymd)
        AND cohort_year_month_date <= DATE(convert_timezone('GMT','US/Pacific',CURRENT_DATE))
        AND to_char(log_ymd, 'YYYY-MM') = cohort_year_month 
        AND cohort_type = 'all'
        AND deleted_at IS NULL
        )
) b
        ON a.date = b.date
)
ORDER BY date"

res_1 <- dbSendQuery(con, query_1, timeout = 600)
df  <- as.data.frame(dbFetch(res_1)); dbClearResult(res_1)
df$dod_churn_rate <- (df$churn)/(df$arr)
library(lubridate)
df$months <- paste(year(df$date), '-', month(df$date),sep='')

# Add booleans for day of week

df$day <- weekdays(as.Date(df$date))
df$Monday <- ifelse(df$day == "Monday", 1, 0)
df$Tuesday <- ifelse(df$day == "Tuesday", 1, 0)
df$Wednesday <- ifelse(df$day == "Wednesday", 1, 0)
df$Thursday <- ifelse(df$day == "Thursday", 1, 0)
df$Friday <- ifelse(df$day == "Friday", 1, 0)
df$Saturday <- ifelse(df$day == "Saturday", 1, 0)
df$Sunday <- ifelse(df$day == "Sunday", 1, 0)
df$Weekend <- ifelse(df$day == "Sunday" | df$day == "Saturday", 1, 0)

# Add booleans for month

df$January <- ifelse(df$month == '1', 1, 0)
df$February <- ifelse(df$month == '2', 1, 0)
df$March <- ifelse(df$month == '3', 1, 0)
df$April <- ifelse(df$month == '4', 1, 0)
df$May <- ifelse(df$month == '5', 1, 0)
df$June <- ifelse(df$month == '6', 1, 0)
df$July <- ifelse(df$month == '7', 1, 0)
df$August <- ifelse(df$month == '8', 1, 0)
df$September <- ifelse(df$month == '9', 1, 0)
df$October <- ifelse(df$month == '10', 1, 0)
df$November <- ifelse(df$month == '11', 1, 0)
df$December <- ifelse(df$month == '12', 1, 0)

# Add pricing data
setwd("/Users/bsknight/Documents/June 2016 Forecasting Project")
# .csv generated from https://stitchlabs.looker.com/x/8pW9pqs
pricing_plans <- read.csv("Pricing_Plans.csv")
library(kimisc)
pricing_plans$over_300 <- (
                          coalesce.na(pricing_plans$plan.3.60.State2.Count.Accounts, 0)  
                        + coalesce.na(pricing_plans$plan.5.60.State2.Count.Accounts, 0)
                        + coalesce.na(pricing_plans$plan.6.40.State2.Count.Accounts, 0)
                        + coalesce.na(pricing_plans$plan.6.90.State2.Count.Accounts, 0)
                        + coalesce.na(pricing_plans$plan.6.41.State2.Count.Accounts, 0)
                        + coalesce.na(pricing_plans$plan.6.91.State2.Count.Accounts, 0)
                        + coalesce.na(pricing_plans$plan.7.30.State2.Count.Accounts, 0)
                        + coalesce.na(pricing_plans$plan.7.31.State2.Count.Accounts, 0)
                        + coalesce.na(pricing_plans$plan.7.32.State2.Count.Accounts, 0)
                          )/pricing_plans$State2.Count.Accounts
pricing_plans$date <- as.Date(pricing_plans$Account.Date.Account.Date.Date)
pricing_plans <- pricing_plans[, 38:39]
df <- merge(x = df, y = pricing_plans, by.x = "date",  by.y = "date", all.x =TRUE)

df <- df[ which(df$renewal_account_customers!=0), ]
working_df <- df

working_df$logged_over_300 <- log(df$over_300)
working_df$logged_account_holders <- log(df$account_holders)
working_df$logged_renewal_account_customers <- log(df$renewal_account_customers)
working_df$first_of_month <- ifelse(substr(working_df$date, 9, 10) == '01', 1, 0)
#working_df <- working_df[complete.cases(working_df),]

library(forecast)
model <- Arima((working_df$dod_churn_rate), 
      order=c(0,0,1), 
      seasonal=c(0,0,0),
      xreg=
        cbind(working_df$Wednesday, 
        working_df$over_300,
        working_df$logged_renewal_account_customers,
        working_df$logged_account_holders, 
        working_df$first_of_month))

# Create a vector of Wednesday values

new_values <- as.data.frame(seq(from = 1, to = 365))

new_values$Wednesday <- seq(Sys.Date(), to= Sys.Date()+364, by = "day")
new_values$Wednesday <- weekdays(as.Date(new_values$Wednesday))
new_values$Wednesday <- ifelse(new_values$Wednesday == "Wednesday", 1, 0)

new_values$'seq(from = 1, to = 365)' <- NULL

# Create a vector of over_300 values
myvars <- c("date", "over_300", "logged_renewal_account_customers",
            "logged_account_holders")
over_300_df <- working_df[myvars]

# P6 has been in existance since August 1, 2014
# P7a has been in existance since December 28th, 2015
# P7b has been in existance since February 29th, 2016
# P7c has been in existance since April 1st, 2016

over_300_df$p6 <- ifelse(over_300_df$date <= '2015-12-28', 1, 0)
over_300_df$p7a <- ifelse(over_300_df$date > '2015-12-28' & over_300_df$date <= '2016-02-29', 1, 0)
over_300_df$p7b <- ifelse(over_300_df$date > '2016-02-29' & over_300_df$date <= '2016-04-01', 1, 0)
over_300_df$p7c <- ifelse(over_300_df$date > '2016-04-01', 1, 0)

dv1 <- ts(over_300_df$over_300, frequency=31)

over_300_forecast <- auto.arima(dv1)
x <- forecast(over_300_forecast, 365)
new_values$over_300 <- x$mean

# Create a vector of logged_renewal_account_customers values

dv2 <- ts(over_300_df$logged_renewal_account_customers, frequency=31)

renewal_model <- Arima(dv2, 
      order=c(1,1,1), 
      seasonal=c(1,0,0),
      xreg=
        cbind(over_300_df$p7b, 
              over_300_df$p7c))

new_values$p7b <- rep(0, 365) 
new_values$p7c <- rep(1, 365) 

logged_renewal_account_customers <- forecast(renewal_model, 365, xreg=
           cbind(new_values$p7b, new_values$p7c))
new_values$logged_renewal_account_customers <- logged_renewal_account_customers$mean

# Create a vector of logged_account_holders values

dv3 <- ts(over_300_df$logged_account_holders, frequency=31)

account_model <- Arima(dv3, 
                       order=c(1,1,1), 
                       seasonal=c(1,0,0))

logged_account_customers <- forecast(account_model, 365)
new_values$logged_account_holders <- logged_account_customers$mean

# Create a vector for first of the month

new_values$date <- seq(Sys.Date(), to= Sys.Date()+364, by = "day")
new_values$first_of_month <- ifelse(substr(new_values$date, 9, 10) == '01', 1, 0)

results <- as.data.frame(forecast(model, xreg=
           cbind(new_values$Wednesday, 
                 as.numeric(new_values$over_300),
                 new_values$logged_renewal_account_customers,
                 new_values$logged_account_holders, 
                 new_values$first_of_month))
                        )
results$date <- seq(Sys.Date(), to= Sys.Date()+364, by = "day")

working_df$Wednesday <- ifelse(working_df$day == "Wednesday", 1, 0)

original_forecast <- as.data.frame(forecast(model, 
                     xreg=
                     cbind(working_df$Wednesday,
                     working_df$over_300,
                     working_df$logged_renewal_account_customers,
                     working_df$logged_account_holders,
                     working_df$first_of_month)
                     ))

original_forecast$date <- working_df$date

myvars1 <- c("date", "dod_churn_rate")
recon_part1 <- working_df[myvars1]
myvars2 <- c("date", "Point Forecast")
recon_part2 <- original_forecast[myvars2]

reconciliation <-  merge(x = recon_part1, 
                         y = recon_part2, 
                         by.x = "date", by.y = "date", 
                         all.x = TRUE)

reconciliation$diff <- reconciliation$dod_churn_rate - 
  reconciliation$'Point Forecast'
mean_diff <- mean(reconciliation[["diff"]], na.rm=TRUE)

correctedforecast <- results
correctedforecast$Adjusted_point <- results$'Point Forecast' + mean_diff
correctedforecast$Adjusted_lo_80 <- results$'Lo 80' + mean_diff
correctedforecast$Adjusted_hi_80 <- results$'Hi 80' + mean_diff
correctedforecast$Adjusted_lo_95 <- results$'Lo 95' + mean_diff
correctedforecast$Adjusted_hi_95 <- results$'Hi 95' + mean_diff
correctedforecast$"Point Forecast" <- NULL
correctedforecast$"Lo 80" <- NULL          
correctedforecast$"Hi 80" <- NULL          
correctedforecast$"Lo 95" <- NULL          
correctedforecast$"Hi 95" <- NULL   

rm(df, new_values, original_forecast, working_df,
   over_300_df, pricing_plans, recon_part2,
   recon_part1, account_model, con, drv, dv1,
   dv2, dv3, logged_account_customers, 
   logged_renewal_account_customers, mean_diff,
   model, myvars, myvars1, myvars2, 
   over_300_forecast, query_1, renewal_model,
   res_1, x, reconciliation)