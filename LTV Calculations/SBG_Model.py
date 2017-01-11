# http://danielweitzenfeld.github.io/passtheroc/blog/2015/01/19/s-b-g/

# In this post, I'm going to show how to use MCMC (via pymc) to estimate one
# of the models they've developed. Using MCMC makes it easy to quantify the
# uncertainty of the model parameters, and because LTV is a function of the
# model parameters, to pass that uncertainty through into the estimates of
# LTV itself.

import psycopg2
import numpy as np
import pymc as pm
import pandas as pd
from scipy.special import hyp2f1
import seaborn as sns # Seaborn: statistical data visualization
import matplotlib.pyplot as plt

conn = psycopg2.connect(


cur = conn.cursor()
cohort_1 =  "SELECT " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 0 " \
"THEN 1 ELSE NULL END) AS invoice_1, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 1 " \
"THEN 1 ELSE NULL END) AS invoice_2, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 2 " \
"THEN 1 ELSE NULL END) AS invoice_3, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 3 " \
"THEN 1 ELSE NULL END) AS invoice_4, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 4 " \
"THEN 1 ELSE NULL END) AS invoice_5, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 5 " \
"THEN 1 ELSE NULL END) AS invoice_6, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 6 " \
"THEN 1 ELSE NULL END) AS invoice_7, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 7 " \
"THEN 1 ELSE NULL END) AS invoice_8, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 8 " \
"THEN 1 ELSE NULL END) AS invoice_9, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 9 " \
"THEN 1 ELSE NULL END) AS invoice_10, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 10 " \
"THEN 1 ELSE NULL END) AS invoice_11, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 11 " \
"THEN 1 ELSE NULL END) AS invoice_12, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 12 " \
"THEN 1 ELSE NULL END) AS invoice_13, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 13 " \
"THEN 1 ELSE NULL END) AS invoice_14, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 14 " \
"THEN 1 ELSE NULL END) AS invoice_15, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 15 " \
"THEN 1 ELSE NULL END) AS invoice_16, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 16 " \
"THEN 1 ELSE NULL END) AS invoice_17, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 17 " \
"THEN 1 ELSE NULL END) AS invoice_18, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 18 " \
"THEN 1 ELSE NULL END) AS invoice_19, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 19 " \
"THEN 1 ELSE NULL END) AS invoice_20, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 20 " \
"THEN 1 ELSE NULL END) AS invoice_21, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 21 " \
"THEN 1 ELSE NULL END) AS invoice_22, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 22 " \
"THEN 1 ELSE NULL END) AS invoice_23, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 23 " \
"THEN 1 ELSE NULL END) AS invoice_24, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 24 " \
"THEN 1 ELSE NULL END) AS invoice_25, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 25 " \
"THEN 1 ELSE NULL END) AS invoice_26, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 26 " \
"THEN 1 ELSE NULL END) AS invoice_27, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 27 " \
"THEN 1 ELSE NULL END) AS invoice_28, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 28 " \
"THEN 1 ELSE NULL END) AS invoice_29, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 29 " \
"THEN 1 ELSE NULL END) AS invoice_30, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 30 " \
"THEN 1 ELSE NULL END) AS invoice_31, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 31 " \
"THEN 1 ELSE NULL END) AS invoice_32, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 32 " \
"THEN 1 ELSE NULL END) AS invoice_33, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 33 " \
"THEN 1 ELSE NULL END) AS invoice_34, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 34 " \
"THEN 1 ELSE NULL END) AS invoice_35, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 35 " \
"THEN 1 ELSE NULL END) AS invoice_36, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 36 " \
"THEN 1 ELSE NULL END) AS invoice_37, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 37 " \
"THEN 1 ELSE NULL END) AS invoice_38, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 38 " \
"THEN 1 ELSE NULL END) AS invoice_39, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 39 " \
"THEN 1 ELSE NULL END) AS invoice_40, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 40 " \
"THEN 1 ELSE NULL END) AS invoice_41, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 41 " \
"THEN 1 ELSE NULL END) AS invoice_42, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 42 " \
"THEN 1 ELSE NULL END) AS invoice_43, " \
"SUM(CASE WHEN date_diff('month', cohort, invoice_year_month) = 43 " \
"THEN 1 ELSE NULL END) AS invoice_44 " \
"FROM " \
    "( " \
    "SELECT " \
    "id, " \
    "account_id, " \
    "concat(invoice_year_month::text, '-01')::date AS invoice_year_month, " \
    "date_trunc('month', first_bill_date)::date AS cohort " \
    "FROM data_warehouse.account_financial_mrr " \
    "WHERE count_toward_mrr = 1 " \
    "AND deleted_at IS NULL " \
    "AND invoice_date IS NOT NULL " \
    "ORDER BY cohort, invoice_year_month " \
    ") " \
    "GROUP BY cohort " \
    "ORDER BY cohort;"

cur.execute(cohort_1)
cohort = cur.fetchall()

arpu_query = "SELECT SUM(weighted)/SUM(ccs) AS arpu " \
"FROM " \
"( " \
"	SELECT	cohort, COUNT(account_id) AS ccs, " \
"           SUM(typical_monthly_mrr)/COUNT(account_id) AS arpu, " \
"			COUNT(account_id)*SUM(typical_monthly_mrr)/COUNT(account_id) " \
"           AS weighted " \
"	FROM " \
"		(SELECT account_id, " \
"               DATE_TRUNC('months', first_bill_date)::DATE AS cohort, " \
"				typical_monthly_mrr " \
"		FROM 	data_warehouse.salesforce_transfer " \
"		WHERE	cc_date_added IS NOT NULL " \
"		AND		first_bill_date IS NOT NULL " \
"		AND 	ignore_data = 0) " \
"	GROUP BY cohort " \
"	ORDER BY cohort " \
");"

cur.execute(arpu_query)
arpu = cur.fetchone()
cur.close()
conn.close()

cohort = list(cohort[0])
arpu = arpu[0]

print(cohort)
print(arpu)

def n_lost(data):
    lost = [None]
    for i in range(1, len(data)):
        lost.append(data[i - 1] - data[i])
    return lost

example_data_n_lost = n_lost(cohort)

data = (cohort, example_data_n_lost)

# define the SBG parameters
alpha = pm.Uniform('alpha', 0.00001, 1000, value=1)
beta = pm.Uniform('beta', 0.00001, 1000, value=1)
print(data)

num_periods = len(cohort)

# "pm.deterministic" tells pymc that the output is completely
# (derministically) determined by the inputs, as is the case here.
@pm.deterministic
def P_T_is_t(alpha=alpha, beta=beta, num_periods=num_periods):
    p = [None, alpha / (alpha + beta)]
    for t in range(2, num_periods):
        pt = (beta + t - 2) / (alpha + beta + t - 1) * p[t-1]
        p.append(pt)
    return p

@pm.deterministic
def survival_function(P_T_is_t=P_T_is_t, num_periods=num_periods):
    s = [None, 1 - P_T_is_t[1]]
    for t in range(2, num_periods):
        s.append(s[t-1] - P_T_is_t[t])
    return s


@pm.observed
def retention_rates(P_T_is_t=P_T_is_t, survival_function=survival_function,
                    value=data):
    def logp(value, P_T_is_t, survival_function):
        active, lost = value

        # Those who've churned along the way...
        died = np.log(P_T_is_t[1:]) * lost[1:]

        # and those still active in last period
        still_active = np.log(survival_function[-1]) * active[-1]
        return sum(died) + still_active


mcmc = pm.MCMC([alpha, beta, P_T_is_t, survival_function, retention_rates])

mcmc.sample(20000, 5000, 20)

# You can plot the distributions if desired
#sns.set(style="darkgrid")
#pm.Matplot.plot(alpha)
#pm.Matplot.plot(beta)
#plt.show()

df_trace = pd.DataFrame({'alpha': alpha.trace(), 'beta': beta.trace()})

# Discounted Expected Residual Lifetime
# The motivation for the DERL is that once you've fit an sBG model to a
# customer base, an obvious follow up question is, "how much money can I
# expect to take in from this customer base in the future?" The DERL for a
# customer who pays $xx per period and who is at the end of their nn
# period is the number such that DERL∗x is the expected present
# value of future payments from that customer. The DERL is a function of α,
# β, a discount rate d, and the current period nn of the subscriber.

# The DERL can also give us the expected discounted CLV of a new customer,
# if we set n=1 and add an undiscounted initial payment. And because we
# have posterior distributions for α and β, we can easily leverage our
# uncertainty in αα and ββ to understand our uncertainty in the statistic
# we really care about, the CLV.



def derl(alpha, beta, d, n):
    """
    Discounted Expected Residual Lifetime, as derived in Fader
    and Hardie (2010).  See equation (6).
    :param alpha: sBG alpha param
    :param beta: sBG beta param
    :param d: discount rate
    :param n: customer's contract period (customer has made n-1 renewals)
    :return: float
    """
    return (beta + n - 1) / (alpha + beta + n - 1) * hyp2f1(1, beta + n,
                             alpha + beta + n, 1 / (1 + d))

df_trace['derl'] = df_trace.apply(lambda x: 1 + derl(x['alpha'],
                                  x['beta'], .1, 1), axis=1)
df_trace['lifetime_value'] = float(arpu) * df_trace.derl + float(arpu)

median_clv = df_trace.lifetime_value.median()
cred_interval = df_trace.lifetime_value.quantile(.025), \
                df_trace.lifetime_value.quantile(.975)
ax = df_trace['lifetime_value'].hist()
ax.set_title('Customer Lifetime Value (Discount Rate: .1)')
ax.set_xlabel('Discounted Expected Customer Lifetime')
ax.plot([median_clv, median_clv], ax.get_ylim())
plt.annotate('Median: %.1f' % median_clv,
             xy=(median_clv + .02,
             ax.get_ylim()[1]-10))
ax.plot([cred_interval[0],
         cred_interval[0]],
         ax.get_ylim(),
         c=sns.color_palette()[2],
         lw=1)
_ = ax.plot([cred_interval[1],
             cred_interval[1]],
             ax.get_ylim(),
             c=sns.color_palette()[2],
             lw=1)
plt.show()






