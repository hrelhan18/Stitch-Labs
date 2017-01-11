
# coding: utf-8

# In[15]:

import pandas as pd
import numpy  as np
from stitch.query import Redshift as rs


# **Master of Stock is Turned on for all Channels**

# In[16]:

conn = rs.RedshiftConnection(user='####',password='#########') 
master_of_stock_query = """
SELECT 
c.account_id,
COUNT(CASE WHEN c.map_default_update_setting  = 1 THEN id END) AS mos_enabled_channels,
COUNT(CASE WHEN c.map_default_update_setting != 1 THEN id END) AS mos_disabled_channels
FROM data_warehouse.connector AS c 
WHERE c.account_id IN
                    (
                    SELECT c.account_id
                    FROM
                    (
                    SELECT
                            a.account_id,
                            a.reg_date,
                            COALESCE(cc_date_added, first_invoice) 
                                 AS cc_date_added,
                            COALESCE(afm_churn_date, close_date) 
                                AS churn_date,
                            ignore_data,
                            DATE(
                            DATEADD(day, 1,
                                    CASE 	WHEN 
                                            cc_date_added IS NOT NULL
                                            THEN 	(
                                                    CASE  WHEN first_bill_date 
                                                          IS NOT NULL
                                                    THEN first_bill_date 
                                                    ELSE 
                                                    GREATEST(bill_start_date, 
                                                    cc_date_added)
                                                    END
                                                    )
                                            ELSE bill_start_date
                                            END
                                        )
                                    ) AS adj_bill_start_date	
                    FROM
                            (
                            SELECT 	
                                    a.account_id,
                                    a.reg_date,
                                    a.close_date,
                                    a.ignore_data,
                                    ab.cc_date_added,
                                    ab.bill_start_date	
                            FROM 
                                    data_warehouse.account a,
                                    data_warehouse.account_billing ab
                            WHERE
                                    ab.account_id = a.account_id	
                            ) a
                    LEFT JOIN			
                            (	
                            SELECT
                                    DISTINCT(account_id),
                                    first_bill_date,
                                    FIRST_VALUE(afm.invoice_date) OVER
                                        (	
                                        PARTITION BY afm.account_id
                                        ORDER BY invoice_date ASC
                                        ROWS BETWEEN UNBOUNDED PRECEDING 
                                        AND UNBOUNDED FOLLOWING
                                        ) AS first_invoice,
                                    FIRST_VALUE(churn_date) OVER
                                        (	
                                        PARTITION BY afm.account_id
                                        ORDER BY created_at DESC
                                        ROWS BETWEEN UNBOUNDED PRECEDING 
                                        AND UNBOUNDED FOLLOWING
                                        ) AS afm_churn_date
                            FROM
                                    data_warehouse.account_financial_mrr afm
                            ) b
                    ON a.account_id = b.account_id
                    ) c
                    LEFT JOIN
                    (
                    SELECT
                            account_id,
                            fee_version,
                            fee_setting
                    FROM 
                            data_warehouse.account_billing
                    ) d
                    ON c.account_id = d.account_id
                    WHERE  
                            cc_date_added IS NOT NULL
                            AND    churn_date IS NULL
                            AND    ignore_data = 0
                            AND( 
                               (fee_version = 3 AND fee_setting = 60)
                            OR (fee_version = 5 AND fee_setting = 60)
                            OR (fee_version = 6 AND fee_setting = 40)
                            OR (fee_version = 6 AND fee_setting = 41)
                            OR (fee_version = 6 AND fee_setting = 90)
                            OR (fee_version = 6 AND fee_setting = 91)
                            OR (fee_version = 7 AND fee_setting = 30)
                            OR (fee_version = 7 AND fee_setting = 31)
                            OR (fee_version = 7 AND fee_setting = 32)
                            OR (fee_version = 7 AND fee_setting = 40)
                            OR (fee_version = 7 AND fee_setting = 90)
                               )
                    ) 
AND c.date_removed IS NULL
AND c.active = 1 
GROUP BY c.account_id
ORDER BY c.account_id
"""
df = conn.query(master_of_stock_query)


# In[17]:

df["Sum"] = df["mos_enabled_channels"] + df["mos_disabled_channels"]
df["Master_Of_Stock"] = df["mos_enabled_channels"] / df["Sum"]
df.drop(["mos_enabled_channels", "mos_disabled_channels", "Sum"], axis=1, inplace=True)
df = df.set_index('account_id')


# In[18]:

from bigquery import get_client
import pandas as pd

project_id = "###################"
service_account = '####################################################'
key = '##########################################################################'
client = get_client(project_id, 
                    service_account=service_account,
                    private_key_file=key, 
                    readonly=True)


# **Linked Listings**

# In[19]:

unlinked_query = """
SELECT 
        cp.account_id account_id,
        SUM(CASE    WHEN mp.id IS NOT NULL AND (p.deleted != 1 OR p.deleted IS NULL) 
                    THEN 1 ELSE 0 END) /
                    ((
                    SUM(CASE WHEN mp.id IS NULL     
                             AND (p.deleted != 1 OR p.deleted IS NULL)    
                             THEN 1 ELSE 0 END) +
                    SUM(CASE WHEN mp.id IS NOT NULL 
                             AND (p.deleted != 1 OR p.deleted IS NULL)   
                             THEN 1 ELSE 0 END) 
                    )*(1.00)) Percent_Linked_Listings
FROM 
        (SELECT id, account_id, active, archived, connector_id 
         FROM stitchapp.connector_product) cp
        LEFT JOIN 
        (SELECT * 
         FROM stitchapp.map_product WHERE active=1) mp
        ON (mp.connector_sku_id = cp.id AND mp.account_id=cp.account_id)
        LEFT JOIN 
        (SELECT * 
         FROM stitchapp.connector) c
        ON (c.id=cp.connector_id AND c.account_id=cp.account_id)
        LEFT JOIN 
        (SELECT * 
         FROM stitchapp.product_sku) ps
        ON (ps.id=mp.sku_id AND ps.account_id=cp.account_id)
        LEFT JOIN 
        (SELECT id, account_id, deleted FROM stitchapp.product) p
        ON (ps.product_id = p.id AND cp.account_id = p.account_id)
WHERE 
        cp.account_id IN (SELECT account_id from stitchapp.csm_accounts)
        AND cp.active=1
        AND cp.archived=0
        AND c.active=1   
GROUP BY 1
ORDER BY 1
"""


# In[20]:

try:
    job_id, results = client.query(unlinked_query, timeout=60)
except BigQueryTimeoutException:
    print "Timeout"

unlinked_results = pd.DataFrame(client.get_query_rows(job_id))
unlinked_results = unlinked_results.set_index('account_id')
df = pd.concat([df, unlinked_results], axis=1)


# **Linked Orders**

# In[21]:

linked_orders_query = """
SELECT 
        o.account_id account_id,
        SUM(CASE WHEN o.limbo==0 THEN 1 ELSE 0 END)/
        (SUM(CASE WHEN o.limbo==1 THEN 1 ELSE 0 END) + 
        SUM(CASE WHEN o.limbo==0 THEN 1 ELSE 0 END)) percent_linked_orders
FROM 
        stitchapp.order o
JOIN        
        stitchapp.csm_accounts csm
ON
        o.account_id = csm.account_id
WHERE 
        o.deleted = 0 
        AND o.void = 0 
        AND o.draft = 0 
        AND 1 = CASE WHEN o.complete=1 THEN o.limbo ELSE 1 END 
GROUP BY 
        1
ORDER BY 
        1
"""


# In[22]:

try:
    job_id, results = client.query(linked_orders_query, timeout=60)
except BigQueryTimeoutException:
    print "Timeout"

linked_orders = pd.DataFrame(client.get_query_rows(job_id))
linked_orders = linked_orders.set_index('account_id')
df = pd.concat([df, linked_orders], axis=1)


# **Duplicate SKUs**

# In[23]:

dupe_query = """
SELECT
   account_id,
   ((COALESCE(INTEGER(duplicate), INTEGER(0))*(1.0)) / 
   (COALESCE(INTEGER(duplicate), INTEGER(0)) + COALESCE(INTEGER(unique_sku), INTEGER(0)) + 
   COALESCE(INTEGER(no_sku), INTEGER(0)))) 
   AS Percent_Duplicate_SKUs
FROM
(
SELECT 
    ps.account_id,
    COUNT(DISTINCT(CASE WHEN ps.sku  IS NULL THEN ps.id END)) AS no_sku,
    COUNT(DISTINCT(CASE WHEN pstwo.ps_sku IS NULL AND ps.sku IS NOT NULL THEN ps.id END)) 
    AS unique_sku,
    COUNT(DISTINCT(CASE WHEN pstwo.ps_sku IS NOT NULL THEN ps.id END)) AS duplicate
FROM 
        stitchapp.product_sku ps 
JOIN 
        stitchapp.product p 
ON 
        ps.product_id = p.id 
AND 
        ps.account_id=p.account_id 
LEFT JOIN (
            SELECT     
                    ps.sku ps_sku, 
                    ps.account_id ps_aid, 
                    COUNT(*) count 
            FROM 
                    stitchapp.product_sku ps 
            JOIN 
                    stitchapp.product p 
            ON 
                    ps.product_id = p.id 
            AND 
                    ps.account_id=p.account_id 
            WHERE 
                    ps.account_id IN (SELECT account_id FROM stitchapp.csm_accounts)
            AND 
                    ps.active = 1 
            AND     p.archived = 0 
            AND     p.deleted = 0 
            AND     ps.sku IS NOT NULL 
            GROUP BY 
                    ps_aid, 
                    ps_sku
            HAVING     COUNT(*) > 1
            ) pstwo
ON 
        pstwo.ps_sku = ps.sku 
AND 
        pstwo.ps_aid = ps.account_id 
WHERE 
        ps.account_id IN (SELECT account_id FROM stitchapp.csm_accounts)
AND     ps.active = 1 
AND     p.archived = 0 
AND     p.deleted = 0
GROUP BY 1
)
"""


# In[24]:

try:
    job_id, results = client.query(dupe_query, timeout=60)
except BigQueryTimeoutException:
    print "Timeout"

dupe_df = pd.DataFrame(client.get_query_rows(job_id))
dupe_df = dupe_df.set_index('account_id')
df = pd.concat([df, dupe_df], axis=1)


# **Average Unit Cost**

# In[25]:

AUC_query = """
SELECT 
            ps.account_id account_id,
            ((COUNT(CASE WHEN average_cost > 0 THEN ps.id END))*(1.0)) /
            (
            (COUNT(CASE WHEN average_cost > 0  THEN ps.id END)) +
            (COUNT(CASE WHEN NOT(average_cost > 0) 	THEN ps.id END)) 
            ) AS Percent_with_AUC
FROM        stitchapp.product_sku ps 
JOIN        stitchapp.product p 
ON          ps.product_id  =  p.id 
AND         ps.account_id  =  p.account_id 
WHERE       ps.account_id   IN (SELECT account_id FROM stitchapp.csm_accounts)
AND         ps.active       =  1 
AND         p.archived      =  0 
AND         p.deleted       =  0 
AND         ps.stock       !=  0 
GROUP BY    1;
"""


# In[26]:

try:
    job_id, results = client.query(AUC_query, timeout=60)
except BigQueryTimeoutException:
    print "Timeout"

auc_df = pd.DataFrame(client.get_query_rows(job_id))
auc_df = auc_df.set_index('account_id')
df = pd.concat([df, auc_df], axis=1)


# **Orders Closing Efficiently**

# In[27]:

ready_to_close_query = """
SELECT 
      o.account_id account_id,
      SUM(CASE WHEN o.complete != 1 THEN 1 ELSE 0 END) ready_to_close,
FROM 
      stitchapp.order o 
WHERE 
      o.deleted = 0 
      AND o.void = 0 
      AND o.draft = 0 
      AND o.limbo = 0 
      AND o.status_invoice >= 1 
      AND o.status_payment >= 1 
      AND o.status_packing_slip >= 1 
      AND o.status_package >= 1 
GROUP BY 
      1
ORDER BY
      1
"""


# In[28]:

try:
    job_id, results = client.query(ready_to_close_query, timeout=60)
except BigQueryTimeoutException:
    print "Timeout"

ready_to_close = pd.DataFrame(client.get_query_rows(job_id))
ready_to_close = ready_to_close.set_index('account_id')
df = pd.concat([df, ready_to_close], axis=1)


# **Re-Code Attributes**

# In[29]:

df["MOS"] = np.where(df["Master_Of_Stock"] == 1, 100, 
                    np.where(
                            (df["Master_Of_Stock"] < 1) & 
                            (df["Master_Of_Stock"] > 0), 50, 
                             np.where(df["Master_Of_Stock"] == 0, 0, 
                                      df["Master_Of_Stock"]))
                            )
##############################################################################################
df["PLL"] = np.where(df["Percent_Linked_Listings"] >= .9, 100, 
                    np.where(
                            (df["Percent_Linked_Listings"] < .9) & 
                            (df["Percent_Linked_Listings"] >= 0.5), 50, 
                            np.where(df["Percent_Linked_Listings"] <0.5, 0, 
                                     df["Percent_Linked_Listings"])) 
                            )
##############################################################################################
df["PLO"] = np.where(df["percent_linked_orders"] >= .9, 100, 
                    np.where(
                            (df["percent_linked_orders"] < .9) & 
                            (df["percent_linked_orders"] >= 0.5), 50, 
                            np.where(df["percent_linked_orders"] <0.5, 0, 
                                     df["percent_linked_orders"])) 
                            )
##############################################################################################
df["PDS"] = np.where(df["Percent_Duplicate_SKUs"] == 0, 100, 
                    np.where(
                            (df["Percent_Duplicate_SKUs"] < 0.05), 50, 
                            np.where(df["Percent_Duplicate_SKUs"] >= 0.05, 0, 
                                     df["Percent_Duplicate_SKUs"])) 
                            )
##############################################################################################
df["PWA"] = np.where(df["Percent_with_AUC"] >= .9, 100, 
                    np.where(
                            (df["Percent_with_AUC"] < .9) & 
                            (df["Percent_with_AUC"] >= 0.5), 50, 
                            np.where(df["Percent_with_AUC"] < 0.5, 0, 
                                     df["Percent_with_AUC"])) 
                            )
##############################################################################################
df["RTC"] = np.where(df["ready_to_close"]== 0, 100, 
                     np.where(df["ready_to_close"] > 0, 50, 
                                     df["ready_to_close"])
                     )


# **Weigh Attributes** 

# In[30]:

df.drop(["Master_Of_Stock", "Percent_Linked_Listings", "percent_linked_orders", 
         "Percent_Duplicate_SKUs", "Percent_with_AUC", "ready_to_close"], 
        axis=1, inplace=True)
df =  df.fillna(0)
df["results"] = (df["MOS"]*0.3) + (df["PLL"]*0.2) + (df["PLO"]*0.2) +                 (df["PDS"]*0.1) + (df["PWA"]*0.1) + (df["RTC"]*0.1)
# df["results"] = df["results"] / df["divisor"]
df.drop(["MOS", "PLL", "PLO", "PDS", "PWA", "RTC"], axis=1, inplace=True)
results = df.rename(columns = {"results":"gainsight_audit_score"})


# **Add Additional Data for SFDC Export**

# In[31]:

conn = rs.RedshiftConnection(user='###',password='###################') 
sfdc_prep_query = """
SELECT  ac.account_id, 
        ac.profile_id, 
        sft.company_name, 
        sft.owner_email AS email
FROM    data_warehouse.salesforce_transfer sft, 
        data_warehouse.access_control ac
WHERE   ac."owner"      =   1
AND     ac.account_id   =   sft.account_id
AND     sft.account_id IN 
                    (
                    SELECT c.account_id
                    FROM
                    (
                    SELECT
                            a.account_id,
                            a.reg_date,
                            COALESCE(cc_date_added, first_invoice) 
                                 AS cc_date_added,
                            COALESCE(afm_churn_date, close_date) 
                                AS churn_date,
                            ignore_data,
                            DATE(
                            DATEADD(day, 1,
                                    CASE 	WHEN 
                                            cc_date_added IS NOT NULL
                                            THEN 	(
                                                    CASE  WHEN first_bill_date 
                                                          IS NOT NULL
                                                    THEN first_bill_date 
                                                    ELSE 
                                                    GREATEST(bill_start_date, 
                                                    cc_date_added)
                                                    END
                                                    )
                                            ELSE bill_start_date
                                            END
                                        )
                                    ) AS adj_bill_start_date	
                    FROM
                            (
                            SELECT 	
                                    a.account_id,
                                    a.reg_date,
                                    a.close_date,
                                    a.ignore_data,
                                    ab.cc_date_added,
                                    ab.bill_start_date	
                            FROM 
                                    data_warehouse.account a,
                                    data_warehouse.account_billing ab
                            WHERE
                                    ab.account_id = a.account_id	
                            ) a
                    LEFT JOIN			
                            (	
                            SELECT
                                    DISTINCT(account_id),
                                    first_bill_date,
                                    FIRST_VALUE(afm.invoice_date) OVER
                                        (	
                                        PARTITION BY afm.account_id
                                        ORDER BY invoice_date ASC
                                        ROWS BETWEEN UNBOUNDED PRECEDING 
                                        AND UNBOUNDED FOLLOWING
                                        ) AS first_invoice,
                                    FIRST_VALUE(churn_date) OVER
                                        (	
                                        PARTITION BY afm.account_id
                                        ORDER BY created_at DESC
                                        ROWS BETWEEN UNBOUNDED PRECEDING 
                                        AND UNBOUNDED FOLLOWING
                                        ) AS afm_churn_date
                            FROM
                                    data_warehouse.account_financial_mrr afm
                            ) b
                    ON a.account_id = b.account_id
                    ) c
                    LEFT JOIN
                    (
                    SELECT
                            account_id,
                            fee_version,
                            fee_setting
                    FROM 
                            data_warehouse.account_billing
                    ) d
                    ON c.account_id = d.account_id
                    WHERE  
                            cc_date_added IS NOT NULL
                            AND    churn_date IS NULL
                            AND    ignore_data = 0
                            AND( 
                               (fee_version = 3 AND fee_setting = 60)
                            OR (fee_version = 5 AND fee_setting = 60)
                            OR (fee_version = 6 AND fee_setting = 40)
                            OR (fee_version = 6 AND fee_setting = 41)
                            OR (fee_version = 6 AND fee_setting = 90)
                            OR (fee_version = 6 AND fee_setting = 91)
                            OR (fee_version = 7 AND fee_setting = 30)
                            OR (fee_version = 7 AND fee_setting = 31)
                            OR (fee_version = 7 AND fee_setting = 32)
                            OR (fee_version = 7 AND fee_setting = 40)
                            OR (fee_version = 7 AND fee_setting = 90)
                               )
                    ) 
"""
sfdc_prep = conn.query(sfdc_prep_query)


# In[32]:

sfdc_prep = sfdc_prep.set_index('account_id')
df = pd.concat([sfdc_prep, results], axis=1)
df.reset_index(level=0, inplace=True)
df = df[np.isfinite(df['account_id'])]
df = df.where((pd.notnull(df)), None) 
df = df[['account_id', 'profile_id', 'company_name', 'email', 'gainsight_audit_score']]
df = df[pd.notnull(df['gainsight_audit_score'])]
df.profile_id = df.profile_id.astype(np.float64)
df['gainsight_audit_score'] = (df['gainsight_audit_score'].astype(np.double)).round()
df = df[np.isfinite(df['profile_id'])]


# **SFDC Export**

# In[33]:

import analytics
from time import sleep
analytics.write_key = 'ZQvjyaoZivJTSzVYbAdEY0gKnWEbqifE'

def send_gainsight_audit_score(account_id, profile_id, company_name, email, gainsight_audit_score):
    analytics.group(profile_id, account_id,
        {
                'name'                  : company_name
            ,   'Gainsight_Audit_Score' : gainsight_audit_score
        },
        integrations={
        'all'        : False,
        'Salesforce' : True
        }    
    ) 
    sleep(0.5)


# In[34]:

df[['account_id','profile_id', 'company_name', 'email','gainsight_audit_score']].apply(
        lambda row: send_gainsight_audit_score(row[0], row[1], row[2], row[3], row[4])
        , axis=1
    )


# In[35]:

df

