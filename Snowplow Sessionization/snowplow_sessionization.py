import psycopg2
from contextlib import closing

db = psycopg2.connect(  host        = ( "#######################################"
                                        "######################"),                                    
                        port        =   "####",
                        dbname      =   "###",
                        user        =   "###",
                        password    =   "###############")
                                         
########################################################################################
# Define the session_prep_chunker
########################################################################################

def session_prep_chucker():
    for i in range(1, 13):
        cur.execute(  
                    """
                    DROP TABLE IF EXISTS    snowplow_sessionization.snowplow_session_prep_%s;
                    CREATE  TABLE           snowplow_sessionization.snowplow_session_prep_%s 
                    diststyle key
                    distkey(session_pkey)
                    sortkey(session_pkey)
                    AS
                    SELECT 
                        DISTINCT resession.domain_userid || resession.domain_sessionidx || 
                                                     '_' || resessionidx as session_pkey
                        , resession.domain_userid
                        , resession.domain_sessionidx
                        , resessionidx
                    -- geo fields
                        , first_value(user_ipaddress ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS user_ipaddress 
                        , first_value(geo_country ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS geo_country 
                        , first_value(geo_region ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS geo_region
                        , first_value(geo_city ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS geo_city
                    -- landing page fields
                        , first_value(  CASE WHEN page_urlhost ='www.stitchlabs.com' 
                                        THEN 'marketing' 
                                        ELSE app_id 
                                        END ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS landing_page_app_id
                        , first_value(page_urlhost ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS landing_page_urlhost
                        , first_value(page_urlpath ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS landing_page_urlpath
                        , first_value(page_urlquery) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS landing_page_urlquery
                    -- exit page fields
                        , last_value(page_urlhost ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS exit_page_urlhost
                        , last_value(page_urlpath ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS exit_page_urlpath
                    -- browser fields
                        , first_value(br_name ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS br_name
                        , first_value(br_family ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS br_family
                        , first_value(br_version ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS br_version
                        , first_value(br_type ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS br_type
                        , first_value(br_renderengine ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS br_renderengine
                        , first_value(br_lang ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS br_lang
                        , first_value(br_features_director ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS br_features_director
                        , first_value(br_features_flash ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS br_features_flash
                        , first_value(br_features_gears ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS br_features_gears
                        , first_value(br_features_java ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS br_features_java
                        , first_value(br_features_pdf ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS br_features_pdf
                        , first_value(br_features_quicktime ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS br_features_quicktime
                        , first_value(br_features_realplayer ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS br_features_realplayer
                        , first_value(br_features_silverlight ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS br_features_silverlight
                        , first_value(br_features_windowsmedia ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS br_features_windowsmedia
                        , first_value(br_cookies ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS br_cookies
                    -- os fields
                        , first_value(os_name ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS os_name
                        , first_value(os_family ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS os_family
                        , first_value(os_manufacturer ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS os_manufacturer
                        , first_value(os_timezone ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS os_timezone
                    -- device fields
                        , first_value(dvce_type ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS dvce_type
                        , first_value(dvce_ismobile ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS dvce_ismobile
                        , first_value(dvce_screenwidth ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS dvce_screenwidth
                        , first_value(dvce_screenheight ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS dvce_screenheight
                    -- marketing fields
                        , first_value(( CASE WHEN mkt_source = '' 
                                        OR refr_medium = 'internal' 
                                        THEN NULL ELSE mkt_source END) ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS mkt_source
                        , first_value(( CASE WHEN mkt_medium = '' 
                                        OR refr_medium = 'internal' 
                                        THEN NULL ELSE mkt_medium END) ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS mkt_medium
                        , first_value(( CASE WHEN mkt_campaign = '' 
                                        OR refr_medium = 'internal' 
                                        THEN NULL ELSE mkt_campaign END) ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS mkt_campaign
                        , first_value(( CASE WHEN mkt_term = '' 
                                        OR refr_medium = 'internal' 
                                        THEN NULL ELSE mkt_term END) ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS mkt_term
                        , first_value(( CASE WHEN mkt_content = '' 
                                        OR refr_medium = 'internal' 
                                        THEN NULL ELSE mkt_content END) ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS mkt_content
                        , first_value(( CASE WHEN mkt_clickid = '' 
                                        OR refr_medium = 'internal' 
                                        THEN NULL ELSE mkt_clickid END)) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS mkt_clickid
                    -- referrer fields - dont ignore nulls
                        , first_value(( CASE WHEN refr_source = '' 
                                        OR refr_medium = 'internal' 
                                        THEN NULL ELSE refr_source END)) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS refr_source
                        , first_value(( CASE WHEN refr_medium = '' 
                                        OR refr_medium = 'internal' 
                                        THEN NULL ELSE refr_medium END)) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS refr_medium
                        , first_value(( CASE WHEN refr_term = '' 
                                        OR refr_medium = 'internal' 
                                        THEN NULL ELSE refr_term END)) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS refr_term
                        , first_value(( CASE WHEN refr_urlhost = '' 
                                        OR refr_medium = 'internal' 
                                        THEN NULL ELSE refr_urlhost END)) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS refr_urlhost
                        , first_value(( CASE WHEN refr_urlpath = '' 
                                        OR refr_medium = 'internal' 
                                        THEN NULL ELSE refr_urlpath END)) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS refr_urlpath 
                    -- demo fields
                        , first_value(demo.email ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS form_email
                        , first_value(demo.phone ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS form_phone
                        , first_value(demo.full_name ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS form_full_name
                        , first_value(demo.company ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS form_company
                        , first_value(demo.demo_qual_order_volume ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS form_qual_order_volume
                        , first_value(demo.form_name ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS form_form
                        , first_value(demo.root_tstamp ignore nulls) 
                            OVER (partition by  resession.domain_userid, 
                                                resession.domain_sessionidx, 
                                                resession.resessionidx 
                            ORDER BY dvce_created_tstamp rows 
                                between unbounded preceding and unbounded following) 
                            AS form_request_submitted_at
                        , GETDATE() AS date_created
                    FROM   
                        snowplow_sessionization.snowplow_resession as resession,
                        snowplow_atomic.events
                        LEFT JOIN snowplow_sessionization.snowplow_form_submission_summary 
                            AS demo
                            ON events.event_id = demo.root_id
                    WHERE 
                        events.event_id = resession.event_id
                        AND date_part(month, resession.session_start) = %s
                        AND resession.domain_userid IS NOT NULL
                        AND resession.domain_sessionidx IS NOT NULL
                        AND resession.domain_userid != ''
                        AND user_ipaddress NOT IN (SELECT ip_address 
                            FROM ben_scratch.ignored_ip_addresses)
                        AND dvce_created_tstamp IS NOT NULL
                        AND collector_tstamp > DATEADD(WEEK, -23, GETDATE())
                        AND dvce_created_tstamp > '2000-01-01'
                        AND dvce_created_tstamp < '2030-01-01'
                    """ % (i, i, i)
                    )
        db.commit()
    for i in range(2, 13):
        cur.execute(
                    """
    	    		INSERT INTO snowplow_sessionization.snowplow_session_prep_1
    	    		(SELECT * FROM snowplow_sessionization.snowplow_session_prep_%s);
    	    		DROP TABLE snowplow_sessionization.snowplow_session_prep_%s;
    	    		""" % (i, i)
    	    		)
        db.commit()
    cur.execute(	
            	"""
            	DROP TABLE IF EXISTS  snowplow_sessionization.snowplow_session_prep;
            	"""
                )
    db.commit()
    cur.execute(
                """
            	SET SEARCH_PATH TO    snowplow_sessionization;
            	ALTER TABLE           snowplow_session_prep_1
                RENAME TO             snowplow_session_prep;
            	"""
                )
    db.commit()
    cur.execute(    
                """
                DROP TABLE IF EXISTS  snowplow_sessionization.snowplow_session_prep_1;
                """
                )
    db.commit()
    cur.execute(
	            """
	            GRANT       select 
	            ON table    snowplow_sessionization.snowplow_session_prep
	            TO          mode, GROUP data_team;
	            """
	            )
    db.commit()
    	
    
##################################################################################################
# Define the session_chucker
##################################################################################################

def session_chucker():
    for i in range(1, 13):
        cur.execute(   
                    """
                    DROP    TABLE IF EXISTS snowplow_sessionization.snowplow_session_%s;
                    CREATE  TABLE           snowplow_sessionization.snowplow_session_%s 
                    diststyle key
                    distkey(domain_userid)
                    compound sortkey(domain_userid, domain_sessionidx, resessionidx, start_at)
                    AS
                    (
                    WITH
                    sessions_pre as (
                        SELECT  snowplow_resession.domain_userid || 
                                snowplow_resession.domain_sessionidx || '_' || 
                                snowplow_resession.resessionidx as session_pkey
                          , min(collector_tstamp) as start_at
                          , max(collector_tstamp) as last_event_at
                          , min(dvce_created_tstamp) AS dvce_min_tstamp
                          , max(dvce_created_tstamp) AS dvce_max_tstamp
                          , count(1) as number_of_events
                          , count(CASE WHEN events.event = 'page_view' THEN 1 ELSE null END) 
                                AS number_of_pageviews
                          , count(distinct(floor(extract(epoch 
                            FROM dvce_created_tstamp)/30)))/2::float 
                            AS time_engaged_with_minutes
                        FROM 
                          snowplow_atomic.events as events,
                          snowplow_sessionization.snowplow_resession as snowplow_resession          
                        WHERE
                          events.event_id = snowplow_resession.event_id
                          AND events.event_id = snowplow_resession.event_id 
                          AND date_part(month, snowplow_resession.session_start) = %s
                          AND snowplow_resession.domain_userid is not null
                          AND snowplow_resession.domain_sessionidx is not null
                          AND snowplow_resession.domain_userid != ''
                          AND dvce_created_tstamp IS NOT NULL
                          AND dvce_created_tstamp > '2000-01-01' -- Prevent SQL errors
                          AND dvce_created_tstamp < '2030-01-01' -- Prevent SQL errors
                    GROUP BY session_pkey
                    )
                  
                    SELECT
                    CONVERT_TIMEZONE('GMT','US/Pacific',start_at) as start_at
                    , CONVERT_TIMEZONE('GMT','US/Pacific',least(last_event_at + 
                        interval '1 minute', lead(start_at) over (partition by b.domain_userid 
                        ORDER BY b.domain_sessionidx, b.resessionidx))) as end_at
                    , number_of_events
                    , number_of_pageviews
                    , time_engaged_with_minutes
                    , b.*
                    FROM sessions_pre
                    inner join snowplow_sessionization.snowplow_session_prep as b
                      on sessions_pre.session_pkey = b.session_pkey
                    );
                    """ % (i, i, i)
                    )
        db.commit()
    
    for i in range(2, 13):
        cur.execute(
                    """
                    INSERT INTO snowplow_sessionization.snowplow_session_1
                    (SELECT * FROM snowplow_sessionization.snowplow_session_%s);
                    DROP TABLE snowplow_sessionization.snowplow_session_%s;
                    """ % (i, i)
                    )
        db.commit()
    cur.execute(    
                """
                DROP TABLE IF EXISTS  snowplow_session;
                """
                )
    db.commit()
    cur.execute(
                """
                SET SEARCH_PATH TO    snowplow_sessionization;
                ALTER TABLE           snowplow_session_1
                RENAME TO             snowplow_session;
                """
                )
    db.commit()
    cur.execute(    
                """
                DROP TABLE IF EXISTS  snowplow_sessionization.snowplow_session_1;
                """
                )
    db.commit()
    cur.execute(
                """
                GRANT       select 
                ON table    snowplow_sessionization.snowplow_session
                TO          mode, GROUP data_team;
                """
                )
    db.commit()

######################################################################################
# Execute the functions
######################################################################################

with closing(db.cursor()) as cur:

# create the snowplow_form_submission table
    cur.execute(
                """
                DROP TABLE IF EXISTS snowplow_sessionization.snowplow_form_submission;
                CREATE TABLE snowplow_sessionization.snowplow_form_submission AS
                SELECT
                    SUBSTRING(form_id,8,36) as hubspot_guid,
                    hs_form.name AS form_name,
                    hs_form.submit_text AS form_submit_text,
                    root_id,
                    json_extract_path_text( json_extract_array_element_text(
                                            elements,li.index),'name') as field_name,
                    json_extract_path_text( json_extract_array_element_text(
                                            elements,li.index),'value') as field_value,
                    json_extract_path_text( json_extract_array_element_text(
                                            elements,li.index),'type') as field_type,
                    li.index as "index",
                    root_tstamp,
                    GETDATE() AS date_created
                FROM
                    hubspot._form AS hs_form,
                    snowplow_atomic.com_snowplowanalytics_snowplow_submit_form_1 form,
                    data_team.list_index li
                WHERE SUBSTRING(form.form_id,8,36) = hs_form.guid
                AND li.index < JSON_ARRAY_LENGTH(form.elements)
                AND len(form.elements) < 4000;
                """
                )
    db.commit()

    cur.execute(
                """
                GRANT       select 
                ON table    snowplow_sessionization.snowplow_form_submission 
                TO          mode, GROUP data_team;
                """
                )

# create the snowplow_form_submission_summary table
    cur.execute(
                """
                DROP TABLE IF EXISTS snowplow_sessionization.snowplow_form_submission_summary;
                CREATE TABLE snowplow_sessionization.snowplow_form_submission_summary AS
                SELECT
                    form.form_name,
                    form.form_submit_text,
                    root_id,
                    MAX( CASE WHEN field_name like '%email%' THEN field_value
                        ELSE NULL END ) as email,
                    MAX( CASE WHEN field_name like '%full_name%' THEN field_value
                        ELSE NULL END ) as full_name,
                    MAX( CASE WHEN field_name like '%phone%' THEN field_value
                        ELSE NULL END ) as phone,
                    MAX( CASE WHEN field_name like '%company%' THEN field_value
                        ELSE NULL END ) as company,
                    MAX( CASE WHEN field_name like '%order_volume%' THEN field_value
                        ELSE NULL END ) as demo_qual_order_volume,
                    MAX(root_tstamp) as root_tstamp,
                    GETDATE() AS date_created
                FROM 
                    snowplow_sessionization.snowplow_form_submission as form
                GROUP BY 
                    form.form_name
                    , form.form_submit_text
                    , form.root_id;
                """
                )
    db.commit()

    cur.execute(
                """
                GRANT       select 
                ON table    snowplow_sessionization.snowplow_form_submission_summary 
                TO          mode, GROUP data_team;
                """
                )


# create snowplow_resession table
    cur.execute(
                """
                DROP TABLE IF EXISTS snowplow_sessionization.snowplow_resession;
                CREATE TABLE snowplow_sessionization.snowplow_resession 
                diststyle key
                distkey(event_id)
                compound sortkey(event_id,resessionidx)
                AS
                SELECT
                    event_id,
                    events.domain_userid,
                    events.domain_sessionidx,
                    RANK() OVER (PARTITION BY domain_userid,
                        domain_sessionidx ORDER BY session_start NULLS FIRST)
                        AS resessionidx,
                    ROW_NUMBER() OVER
                        (PARTITION BY domain_userid, domain_sessionidx,
                        session_start ORDER BY dvce_created_tstamp ASC NULLS FIRST)
                        AS event_num,
                    min(collector_tstamp) over
                        (   partition by    events.domain_userid, domain_sessionidx 
                                order by    collector_tstamp 
                                rows        unbounded preceding) AS session_start,
                    GETDATE() AS date_created
                FROM
                (
                SELECT
                    event_id,
                    domain_userid,
                    domain_sessionidx,
                    LAST_VALUE(
                        CASE WHEN (refr_urlhost NOT LIKE '%stitchlabs.com'
                        OR refr_urlhost IS NULL) AND event = 'page_view'
                        THEN dvce_created_tstamp
                        ELSE NULL
                        END)
                    IGNORE NULLS OVER (
                        PARTITION BY domain_userid, domain_sessionidx
                        ORDER BY dvce_created_tstamp ASC ROWS UNBOUNDED PRECEDING)
                        AS session_start,
                    dvce_created_tstamp,
                    collector_tstamp
                FROM snowplow_atomic.events
                WHERE user_ipaddress not in
                (SELECT ip_address FROM ben_scratch.ignored_ip_addresses)
                AND collector_tstamp >= '2016-03-01'
                ) events;
                """
                )
    db.commit()

    cur.execute(
                """
                GRANT       select 
                ON table    snowplow_sessionization.snowplow_resession 
                TO          mode, GROUP data_team;
                """
                )

    session_prep_chucker()
    session_chucker() 

    cur.execute(
                """
                DROP    TABLE IF EXISTS snowplow_sessionization.snowplow_map_domain_userid;
                CREATE  TABLE           snowplow_sessionization.snowplow_map_domain_userid AS
                (
                WITH tracked as 
                (
                SELECT 
                        account_id, profile_id, domain_userid
                        , min(first_event) as first_event
                        , max(last_event) as last_event
                FROM 
                        (
                        SELECT 
                                * 
                        FROM
                            (
                            SELECT
                                    STRTOL(regexp_substr(
                                            split_part(se_property,'-',1),'([0-9]+)$'),10) 
                                            as account_id,
                                    STRTOL(regexp_substr(
                                            split_part(se_property,'-',2),'([0-9]+)$'),10) 
                                            as profile_id,
                                    domain_userid, 
                                    min(dvce_created_tstamp) as first_event, 
                                    max(dvce_created_tstamp) as last_event
                            FROM 
                                    snowplow_atomic.events
                            WHERE 
                                    event = 'struct'
                                    AND user_ipaddress not in 
                                        (
                                        SELECT ip_address
                                        FROM  ben_scratch.ignored_ip_addresses
                                        )
                                    AND se_property not like '%customerService'
                                    AND (STRTOL(regexp_substr(
                                            split_part(
                                                se_property,'-',1),'([0-9]+)$'),10) > 99)
                                    AND ((STRTOL(regexp_substr(
                                            split_part(
                                                se_property,'-',2),'([0-9]+)$'),10) != 99))
                            GROUP BY 
                                    domain_userid, account_id, profile_id 
                            ) AS snowplow
                        UNION ALL
                            (
                            SELECT 
                                    profile.account_id,
                                    profile.id as profile_id,
                                    substring(context_snowplow_domain_userid from 0 for 17) 
                                        AS domain_userid,
                                    min(original_timestamp) as first_event,
                                    max(original_timestamp) as last_event
                            FROM
                                    marketing_site.identifies,
                                    data_warehouse.profile
                            WHERE 
                                    context_snowplow_domain_userid is not null
                                    and profile.email = identifies.email 
                                    AND context_ip NOT IN  
                                        (
                                        SELECT ip_address
                                        FROM  ben_scratch.ignored_ip_addresses
                                        )
                            GROUP BY 1,2,3
                            )
                )
                GROUP BY 1,2,3
                )
                SELECT
                        sf.account_id,
                        tracked.profile_id,
                        domain_userid,
                        first_event,
                        last_event
                FROM 
                        data_warehouse.salesforce_transfer as sf LEFT JOIN tracked
                ON 
                        tracked.account_id = sf.account_id
                WHERE 
                        sf.ignore_data = 0
                );
                """
                )
    db.commit()

    cur.execute("""
                GRANT       select 
                ON table    snowplow_sessionization.snowplow_map_domain_userid 
                TO          mode, GROUP data_team;
                """)

# create the snowplow_advanced_session table
    cur.execute(
                """
                DROP TABLE IF EXISTS snowplow_sessionization.snowplow_advanced_session;
                CREATE TABLE snowplow_sessionization.snowplow_advanced_session
                diststyle key
                distkey(domain_userid)
                compound sortkey(domain_userid, domain_sessionidx, resessionidx, start_at)
                AS
                (
                WITH
                sessions_pre as 
                    (
                    select  snowplow_resession.domain_userid || 
                            snowplow_resession.domain_sessionidx || '_' || 
                            snowplow_resession.resessionidx as session_pkey
                            , min(collector_tstamp) as start_at
                            , max(collector_tstamp) as last_event_at
                            , min(dvce_created_tstamp) AS dvce_min_tstamp
                            , max(dvce_created_tstamp) AS dvce_max_tstamp
                            , count(1) as number_of_events
                            , count(CASE WHEN events.event = 'page_view' 
                                    THEN 1 ELSE null END) as number_of_pageviews
                            , count(distinct(floor(extract(epoch 
                                FROM dvce_created_tstamp)/30)))/2::float 
                                AS time_engaged_with_minutes
                    FROM 
                            snowplow_atomic.events as events,
                            snowplow_sessionization.snowplow_resession as snowplow_resession
                    WHERE
                            events.event_id = snowplow_resession.event_id
                            and snowplow_resession.domain_userid is not null
                            and snowplow_resession.domain_sessionidx is not null
                            and snowplow_resession.domain_userid != ''
                            and dvce_created_tstamp IS NOT NULL
                            and dvce_created_tstamp > '2000-01-01' -- Prevent SQL errors
                            and dvce_created_tstamp < '2030-01-01' -- Prevent SQL errors
                    GROUP BY 
                            session_pkey
                    )
                SELECT
                    CONVERT_TIMEZONE('GMT','US/Pacific',start_at) as start_at
                    , CONVERT_TIMEZONE('GMT','US/Pacific',least(last_event_at + 
                        interval '1 minute', lead(start_at) 
                        OVER (partition by b.domain_userid 
                        ORDER BY b.domain_sessionidx, b.resessionidx))) 
                        AS end_at
                    , number_of_events
                    , number_of_pageviews
                    , time_engaged_with_minutes
                    , b.*
                , CASE 
                    WHEN mkt_medium is not null THEN CASE 
                    WHEN mkt_medium = 'ppc' THEN 'cpc'
                        ELSE mkt_medium
                        END
                    WHEN refr_medium = 'search' THEN 
                        CASE WHEN landing_page_urlpath like '/lp/%' 
                        AND NOT landing_page_urlpath like '%laptop' THEN 'cpc'
                        ELSE 'organic'
                        END
                    WHEN refr_medium ='social' THEN 'social'
                    WHEN refr_medium = 'email' THEN 'email'
                    WHEN refr_medium = 'unknown' THEN
                CASE
                    WHEN refr_urlhost like '%.stitchlabs.com' THEN 'internal'
                    ELSE 'other' END
                    WHEN landing_page_app_id = 'stitchlabs' 
                    THEN CASE 
                    WHEN landing_page_urlhost = 'app.stitchlabs.com' 
                      THEN CASE 
                        WHEN regexp_substr(landing_page_urlquery,'rc=[^&]*') is not null
                          THEN 'partner-integration'
                        ELSE 'direct: in app'
                        END
                    ELSE 'direct: subdomain'
                    END
                  WHEN refr_urlhost is null THEN 'direct: marketing or support'
                  ELSE 'unclassified' END 
                  as acquisition_type_detail
                ,CASE 
                WHEN mkt_source is not null THEN mkt_source
                WHEN refr_urlhost is not null THEN refr_urlhost
                WHEN landing_page_urlhost = 'app.stitchlabs.com' 
                    AND regexp_substr(landing_page_urlquery,'rc=[^&]*') is not null
                THEN regexp_replace(regexp_substr(landing_page_urlquery,'rc=([^&]*)'),'rc=','')
                ELSE 'direct'
                END as acquisition_source
                FROM sessions_pre
                inner join snowplow_sessionization.snowplow_session_prep as b
                on sessions_pre.session_pkey = b.session_pkey);
                """
                )
    db.commit()

    cur.execute(
                """
                GRANT       select 
                ON table    snowplow_sessionization.snowplow_advanced_session 
                TO          mode, GROUP data_team;
                """
                )

    cur.execute(
                """
                DROP TABLE IF EXISTS snowplow_sessionization.snowplow_impression;
                CREATE TABLE snowplow_sessionization.snowplow_impression
                diststyle key
                distkey(impression_account_id)
                compound sortkey(impression_account_id, start_at)
                AS
                (
                SELECT 
                    domain.account_id as impression_account_id,
                    session.*
                    ,CASE 
                        WHEN mkt_medium is not null 
                        THEN CASE 
                            WHEN mkt_medium = 'ppc'
                            THEN 'cpc'
                        ELSE mkt_medium
                        END
                        WHEN refr_medium = 'search' 
                        THEN CASE 
                            WHEN landing_page_urlpath like '/lp/%' 
                            AND NOT landing_page_urlpath like '%laptop' THEN 'cpc'
                            ELSE 'organic'
                            END
                        WHEN refr_medium ='social' THEN 'social'
                        WHEN refr_medium = 'email' THEN 'email'
                        WHEN refr_medium = 'unknown' THEN
                            CASE
                            WHEN refr_urlhost like '%.stitchlabs.com' THEN 'internal'
                            ELSE 'other' END
                        WHEN domain.domain_userid is null THEN 'untracked'
                        WHEN session.landing_page_app_id = 'stitchlabs' 
                            THEN CASE 
                            WHEN session.landing_page_urlhost = 'app.stitchlabs.com' 
                                THEN CASE 
                            WHEN regexp_substr(session.landing_page_urlquery,'rc=[^&]*') 
                                IS NOT NULL
                                THEN 'partner-integration'
                            ELSE 'direct: in app'
                            END
                        WHEN session.landing_page_urlhost like (domain.subdomain||'.%')
                            THEN 'direct: on subdomain'
                        ELSE 'direct: other subdomain'
                        END
                        WHEN refr_urlhost is null 
                            THEN 'direct: marketing or support'
                        ELSE 'unclassified' END as acquisition_type_detail,
                    CASE 
                        WHEN mkt_source is not null THEN mkt_source
                        WHEN refr_urlhost is not null THEN refr_urlhost
                        WHEN session.landing_page_urlhost = 'app.stitchlabs.com' 
                        AND regexp_substr(session.landing_page_urlquery,'rc=[^&]*') 
                            IS NOT NULL
                            THEN    regexp_replace(regexp_substr(
                                    session.landing_page_urlquery,'rc=([^&]*)'),'rc=','')
                    ELSE NULL
                    END as acquisition_source,
                    CASE WHEN 
                      domain.domain_userid is not null THEN 1
                    ELSE 0 END as tracked  
                FROM
                (
                  SELECT sf.account_id, map.domain_userid, sf.subdomain
                  FROM
                    data_warehouse.salesforce_transfer sf,
                    snowplow_sessionization.snowplow_map_domain_userid as map
                  WHERE
                    sf.account_id = map.account_id
                  GROUP BY sf.account_id, map.domain_userid, sf.subdomain
                ) as domain
                LEFT JOIN
                snowplow_sessionization.snowplow_session as session
                ON
                domain.domain_userid = session.domain_userid
                ) ;
                """
                )
    db.commit()

    cur.execute(
                """
                GRANT       select 
                ON table    snowplow_sessionization.snowplow_impression
                TO          mode, GROUP data_team;
                """
                )

# create the derived_conversion table
    cur.execute(
                """
                DROP TABLE IF EXISTS snowplow_sessionization.derived_conversion;
                CREATE TABLE snowplow_sessionization.derived_conversion
                diststyle key
                distkey(account_id)
                compound sortkey(conversion_type, account_id, conversion_date)
                AS
                (
                select 
                  account_id, 
                  conversion_type, 
                  DATEADD(MINUTE,5,conversion_date) as conversion_date
                  FROM
                  (
                    (
                      select account_id, 'reg' as conversion_type, 
                      --CONVERT_TIMEZONE('US/Pacific','GMT',
                      reg_date
                      --)
                      as conversion_date
                      FROM data_warehouse.salesforce_transfer
                    )
                    UNION ALL
                    (
                      SELECT account_id, 'cc' as conversion_type, 
                      --CONVERT_TIMEZONE('US/Pacific','GMT',
                      cc_date_added
                      --)
                      as conversion_date
                      FROM data_warehouse.salesforce_transfer
                      WHERE cc_date_added is not null
                    )
                    UNION ALL
                    (
					SELECT  
					        p.account_id, 
					        'mql' as conversion_type, 
					        la.activity_date AS conversion_date
					FROM 		
					        marketo.activity_change_data_value cdv, 
						  	marketo.lead_activity la,
						  	marketo.lead l,
						  	data_warehouse.profile p,
						  	data_warehouse.account a
					WHERE 	
					        p.email = l.email
					        AND a.account_id = p.account_id
					        AND a.ignore_data = 0    
					        AND cdv.id = la.id
					        AND	la.lead_id = l.id
					        AND new_value =  'MQL'
					        AND primary_attribute_value = 'Lifecycle Stage'
					        AND l.email NOT ILIKE '%stitchlabs.com'
                    )
                    UNION ALL
                    (
					SELECT
					    		a.account_id,
					    		'churn' as conversion_type, 
					    		COALESCE(afm_churn_date, close_date) 
					    		AS conversion_date
					FROM
					    		(
					    		SELECT 	
					    				a.account_id,
					    				a.reg_date,
					    				a.close_date,
					    				a.ignore_data,
					    		 		ab.cc_date_added	
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
					WHERE COALESCE(afm_churn_date, close_date) IS NOT NULL
                    )
                    UNION ALL
                    (
                      SELECT account_id, LOWER(acset.value) as conversion_type, min(
                      --CONVERT_TIMEZONE('US/Pacific','GMT',
                      acse.date_added
                      --)
                      ) as conversion_date
                      FROM 
                        data_warehouse.account_cust_serv_event acse,
                        data_warehouse.account_cust_serv_event_type acset
                      WHERE 
                        acse.active = 1
                        and acset.id = acse.type
                        and acset.id in (7,11,55,56,57)
                      GROUP BY account_id, acset.value
                    )
                    UNION ALL
                    (
                      SELECT account_id, 'core' as conversion_type, 
                      --CONVERT_TIMEZONE('US/Pacific','GMT',
                      core_v01
                      --) 
                      as conversion_date
                      FROM 
                        data_warehouse.salesforce_transfer
                      WHERE 
                        core_v01 is not null
                        and ignore_data =0
                    )
                    UNION ALL
                    (
                      SELECT    account_id, 'all' as conversion_type,  
                                CONVERT_TIMEZONE('GMT','US/Pacific', getdate()) 
                                as conversion_date
                      FROM 
                        data_warehouse.salesforce_transfer
                      WHERE 
                        ignore_data = 0
                    )
                  )
                ) ;
                """
                )
    db.commit()

    cur.execute(
                """
                GRANT       select 
                ON table    snowplow_sessionization.derived_conversion
                TO          mode, GROUP data_team;
                """
                )
# create the fmql table
    cur.execute(
                """
                DROP TABLE IF EXISTS ben_scratch.fmqls;
                CREATE TABLE ben_scratch.fmqls AS
                (
                SELECT 
                COALESCE(
                        convert_timezone('GMT', 'America/Los_Angeles', reg_date),
                        convert_timezone('GMT', 'America/Los_Angeles', created_date)
                        ) AS mql_date,
                id AS sfdc_lead_id,
                stitch__id___c AS sfdc_stitch_id 
                FROM
                (
                  SELECT 
                      id, 
                      stitch__id___c,
                      country__code___c,
                      lead_source,
                      phone,
                      email,
                      created_date,
                      COALESCE( monthly_sales_orders_c, 
                                lead_qual_order_volume_c
                                ) 
                                AS sf_lead_order_data,
                      CASE WHEN id IN   
                      (
                      SELECT sf_lead_id
                      FROM
                      (
                      SELECT 
                          sf_lead_id, 
                          stitch_id, 
                          lead_source,
                          phone,
                          email,
                          country__code___c,
                          cc_date_added,
                          COALESCE( response, 
                                lead_qual_order_volume_c, 
                                monthly_sales_orders_c) AS order_volume
                                FROM
                                (
                                SELECT  id AS sf_lead_id, 
                                    stitch__id___c AS stitch_id,
                                    lead_source, 
                                    phone,
                                    email,
                                    country__code___c,
                                    monthly_sales_orders_c,
                                    lead_qual_order_volume_c
                                FROM salesforce_fivetran._lead
                                WHERE is_deleted IS NOT TRUE
                                AND (   country__code___c ILIKE 'US'
                                    OR  country__code___c ILIKE 'CA'
                                    OR  country__code___c ILIKE 'GB'
                                    OR  country__code___c ILIKE 'AU'
                                    OR  country__code___c ILIKE 'NZ'
                                    OR  country__code___c ILIKE 'SG'
                                    OR  country__code___c IS NULL)
                                ) a   
                                LEFT JOIN
                                  (
                                  SELECT account_id, response
                                  FROM data_warehouse.lead_qual_response
                                  WHERE QUESTION = 'num_monthly_orders'
                                  ) b
                                ON a.stitch_id = b.account_id
                                LEFT JOIN
                                  (
                                  SELECT account_id, cc_date_added
                                  FROM data_warehouse.salesforce_transfer
                                  ) c
                                ON a.stitch_id = c.account_id
                          WHERE       
                            (
                            COALESCE( response, 
                                  lead_qual_order_volume_c, 
                                  monthly_sales_orders_c) IS NOT NULL
                            AND phone IS NOT NULL
                            AND email IS NOT NULL
                            AND phone NOT IN 
                                (SELECT phone FROM ben_scratch.bad_phone_numbers)
                            )
                          OR  
                            (
                            lead_source IN ('Demo Request', 'Marketo')
                            AND phone IS NOT NULL
                            AND email IS NOT NULL
                            AND phone NOT IN 
                                (SELECT phone FROM ben_scratch.bad_phone_numbers)
                            )
                          OR  
                            (
                            lead_source IN ('Demo Request', 'Marketo')
                            AND cc_date_added IS NOT NULL
                            AND email IS NOT NULL
                            AND phone NOT IN 
                                (SELECT phone FROM ben_scratch.bad_phone_numbers)
                            )
                      )
                    )
                    THEN 1 ELSE 0 END AS mql
                    FROM salesforce_fivetran._lead
                    ) a
                    LEFT JOIN
                    (
                    SELECT account_id, reg_date
                    FROM data_warehouse.salesforce_transfer
                    ) b
                ON 
                        a.stitch__id___c = b.account_id
                WHERE 
                        mql = 1
                );
                """
                )
    db.commit()

    cur.execute(
                """
                GRANT       select 
                ON table    ben_scratch.fmqls
                TO          mode, GROUP data_team;
                """
                )

# create the derived_revenue table
    cur.execute(
                """
                DROP TABLE IF EXISTS snowplow_sessionization.derived_revenue;
                CREATE TABLE snowplow_sessionization.derived_revenue
                diststyle key
                distkey(account_id)
                compound sortkey(account_id, revenue_type)
                AS
                (
                SELECT 
                      account_id, 
                      revenue_type, 
                      revenue
                    FROM
                    (
                      (
                        SELECT 
                          account_id,
                          'typical_monthly_recurring' as revenue_type, 
                          CASE 
                            WHEN typical_monthly_mrr > 0 
                              THEN typical_monthly_mrr
                            WHEN most_recent_monthly_mrr > 0 
                              THEN most_recent_monthly_mrr
                          ELSE NULL
                          END as revenue
                        FROM
                          data_warehouse.salesforce_transfer
                        WHERE ignore_data = 0
                        --WHERE cc_date_added is not null
                        --AND SFDC_ACCOUNT_STATUS like '%paying%'
                      )
                      UNION
                      (
                        SELECT 
                          account_id,
                          'most_recent_monthly_recurring' as revenue_type, 
                          most_recent_monthly_mrr as revenue
                        FROM
                          data_warehouse.salesforce_transfer
                         WHERE ignore_data = 0
                        --WHERE cc_date_added is not null
                        --AND SFDC_ACCOUNT_STATUS like '%paying%'
                      )
                      UNION
                      (
                        SELECT 
                          account_id, 
                          'first_monthly_recurring' as revenue_type,
                          revenue 
                        FROM
                        (
                          SELECT
                            account_id, 
                            count_toward_mrr * invoice_total as revenue,
                            row_number() OVER (PARTITION BY account_id 
                                ORDER BY invoice_date ASC)
                            as row_num
                          FROM
                            data_warehouse.account_financial_mrr
                        ) as mrr
                        WHERE row_num= 1
                      )
                      UNION
                      (
                        SELECT account_id, 
                            'total' as revenue_type,
                            SUM(count_toward_mrr * invoice_total) as revenue
                        FROM
                            data_warehouse.account_financial_mrr
                        WHERE invoice_date < current_date
                        GROUP BY account_id
                        
                      )
                    )
                ) ;
                """
                )
    db.commit()

    cur.execute(
                """
                GRANT       select 
                ON table    snowplow_sessionization.derived_revenue
                TO          mode, GROUP data_team;
                """
                )

# create the snowplow_session visitor table
    cur.execute(
                """
                DROP TABLE IF EXISTS snowplow_sessionization.snowplow_visitor;
                CREATE TABLE snowplow_sessionization.snowplow_visitor
                diststyle key
                distkey(domain_userid)
                compound sortkey(domain_userid, type)
                AS
                (
                  SELECT 
                    domain_userid
                    ,visitor_id
                    ,start_at
                    ,type
                    ,form_name
                  FROM
                  (
                    (
                      select 
                        domain_userid
                        , domain_userid as visitor_id
                        , min(start_at) as start_at
                        , 'new visitor' as type
                        , null as form_name
                      FROM
                        snowplow_sessionization.snowplow_advanced_session 
                      WHERE landing_page_urlhost = 'www.stitchlabs.com'
                      group by 1,2
                    )
                    UNION ALL
                    (
                      SELECT 
                        domain_userid
                        , sft.account_id::text as visitor_id
                        , reg_date as start_at
                        ,'sc1' as type
                        , null as form_name
                      FROM
                      (
                        SELECT 
                          domain_userid, account_id
                        FROM
                          snowplow_sessionization.snowplow_map_domain_userid
                        group by 1,2
                      ) as map
                      RIGHT JOIN
                      data_warehouse.salesforce_transfer sft
                      ON sft.account_id = map.account_id
                      WHERE 
                        ignore_data = 0
                    )
                    UNION ALL
                    (
                      SELECT 
                        domain_userid
                        , sft.account_id::text as visitor_id 
                        , cc_date_added as start_at
                        , 'cc' as type
                        , null as form_name
                      FROM
                      (
                        SELECT 
                          domain_userid, account_id
                        FROM
                          snowplow_sessionization.snowplow_map_domain_userid
                        GROUP BY 1,2
                      ) as map
                      RIGHT JOIN
			                      	data_warehouse.salesforce_transfer sft
			                      	ON sft.account_id = map.account_id
                      WHERE 
		                        	ignore_data = 0 
		                        	AND sft.cc_date_added is not null
                    )
                    UNION ALL
                    (
                      SELECT 
                        events.domain_userid as domain_userid
                        , demo.email as visitor_id
                        , CONVERT_TIMEZONE('GMT','US/Pacific',demo.root_tstamp) as start_at
                        , 'request a demo' as type
                        , null as form_name
                      FROM 
                        snowplow_sessionization.snowplow_form_submission_summary as demo,
                        snowplow_atomic.events as events
                      WHERE
                        (
                          form_submit_text like '%DEMO%'
                          or form_name like '%Demo%'
                        )
                        and
                        events.event_id = demo.root_id
                        and events.user_ipaddress 
                        NOT IN (SELECT ip_address FROM ben_scratch.ignored_ip_addresses)
                    )
                    UNION ALL
                    (
                      SELECT 
                        events.domain_userid as domain_userid
                        , demo.email as visitor_id
                        , CONVERT_TIMEZONE('GMT','US/Pacific',demo.root_tstamp) as start_at
                        , 'other form' as type
                        , demo.form_name as form_name
                      FROM 
                        snowplow_sessionization.snowplow_form_submission_summary as demo,
                        snowplow_atomic.events as events
                      WHERE 
                        (
                          form_submit_text not like '%DEMO%'
                          AND form_name not like '%Demo%'
                        )
                        and
                        events.event_id = demo.root_id
                        and events.user_ipaddress NOT IN 
                            (SELECT ip_address FROM ben_scratch.ignored_ip_addresses)
                    ) 
                    UNION ALL 
                    (
                    SELECT 
                    SUBSTRING(context_snowplow_domain_userid FROM 0 FOR 17)     AS domain_userid
                    ,email                                                      AS visitor_id
                    ,CONVERT_TIMEZONE('GMT','US/Pacific', original_timestamp)   AS start_at
                    ,'other form'                                               AS type
                    ,form_title                                                 AS form_name
                    FROM 
                            marketing_site.create_lead
                    WHERE
                            context_ip NOT IN 
                            (SELECT ip_address FROM ben_scratch.ignored_ip_addresses)
                    )           
                    UNION ALL
                    (
                    SELECT 
                    SUBSTRING(context_snowplow_domain_userid FROM 0 FOR 17)     AS domain_userid
                    ,email                                                      AS visitor_id
                    ,CONVERT_TIMEZONE('GMT','US/Pacific', original_timestamp)   AS start_at
                    ,'other form'                                               AS type
                    ,event                                                      AS form_name
                    FROM 
                            marketing_site.sign_up
                    WHERE
                            context_ip NOT IN (SELECT ip_address 
                            FROM ben_scratch.ignored_ip_addresses)
                    )
                    UNION ALL 
                    (
                    SELECT 
                            ud.foreign_id AS domain_userid
                            ,l.id || ud.id AS visitor_id
                            ,CASE   WHEN cc_date_added IS NOT NULL 
                                    THEN cc_date_added
                                    ELSE DATEADD(month, 1, GETDATE())
                                    END AS start_at
                            ,'FMQL' AS type
                            ,NULL AS form_name
                    FROM    
                            ben_scratch.fmqls f  
                            ,foreign_contact.foreign_contact AS l
                            ,foreign_contact.foreign_contact AS ud
                            ,data_warehouse.salesforce_transfer sft
                    WHERE
                            f.sfdc_lead_id = l.foreign_id
                            AND sft.account_id = f.sfdc_stitch_id
                            AND l.business_id = ud.business_id
                            AND l.source IN ('salesforce.lead')
                            AND ud.source IN ('snowplow_atomic.events.domain_userid')
                    )
                  )
                );
                """
                )
    db.commit()
    
    cur.execute(
                """
                GRANT       select 
                ON table    snowplow_sessionization.snowplow_visitor
                TO          mode, GROUP data_team;
                """
                )

# create the snowplow_session snowplow_visitor_epoch table
    cur.execute(
                """
                DROP TABLE IF EXISTS snowplow_sessionization.snowplow_visitor_epoch;
                CREATE TABLE snowplow_sessionization.snowplow_visitor_epoch
                diststyle key
                distkey(domain_userid)
                compound sortkey(type, visitor_event_at)
                AS
                (
                SELECT 
                        * 
                FROM
                        (           
                        SELECT
                                row_number() over (order by visitor.start_at asc) AS epoch_id,
                                SUM(1)  OVER (PARTITION BY visitor.type, visitor_id 
                                ORDER BY session.start_at DESC ROWS UNBOUNDED PRECEDING) 
                                AS position,
                                SUM(1)  OVER (PARTITION BY visitor.type, visitor_id
                                ORDER BY session.start_at DESC 
                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
                                AS num_impressions,
                                visitor.visitor_id,
                                visitor.type,
                                visitor.form_name,
                                visitor.start_at as visitor_event_at,
                                session.*
                        FROM
                                snowplow_sessionization.snowplow_visitor as visitor
                        LEFT JOIN
                                snowplow_sessionization.snowplow_advanced_session AS session
                        ON 
                                session.start_at <= DATEADD(MINUTE,5,visitor.start_at)
                                AND session.domain_userid = visitor.domain_userid
                  )
                );
                GRANT       select 
                ON table    snowplow_sessionization.snowplow_visitor_epoch
                TO          mode, GROUP data_team;
                """
                )
    db.commit()
# create the sf_confirmation_status table
    cur.execute(
                """
                DROP TABLE IF EXISTS snowplow_sessionization.sf_confirmation_status;
                CREATE TABLE snowplow_sessionization.sf_confirmation_status
                AS
                (
                SELECT
                    a.lead_id, 
                    a.account_id, 
                    a.stitch_account_id, 
                    a.lead_source, 
                    a.first_lead_id, 
                    a.first_lead_created_date, 
                    a.first_lead_source,
                    a.confirmation_status, 
                    CASE 
                      WHEN a.has_phone = 0 AND a.confirmation_status = 'Unknown' 
                      THEN 'No Phone'
                      ELSE a.confirmation_status
                    END as ext_conf_status,
                    a.last_confirmed_date, 
                    a.last_confirmed_by, 
                    s.first_name + ' ' + s.last_name AS last_confirmed_by_name, 
                    a.has_phone,
                    a.first_sales_ready_date,
                    a.first_sales_not_ready_date, 
                    a.first_disqual_date, 
                    LEAST(a.first_sales_ready_date, a.first_sales_not_ready_date, 
                          a.first_disqual_date) as first_qualified_date,
                    a.mid_market, 
                    a.classification,
                    st.account_classification AS st_classification, 
                    CASE
                      WHEN st.account_classification = a.classification THEN TRUE
                      WHEN st.account_classification IS NULL 
                      AND (a.classification = 'Unclassified' OR a.classification = 'Unknown') 
                      THEN TRUE
                      WHEN st.account_classification IS NULL 
                      AND a.classification IS NULL THEN TRUE
                      ELSE FALSE
                    END AS class_match, 
                    COALESCE(a.account_id, a.lead_id) AS who_id
                  FROM
                  (
                    SELECT
                      l.id AS lead_id,
                      a.id AS account_id, 
                      CASE 
                        WHEN a.phone IS NOT NULL THEN a.phone
                        WHEN a.phone IS NULL AND l.phone IS NOT NULL THEN l.phone
                      END as phone, 
                      CASE 
                        WHEN a.phone IS NOT NULL AND length(a.phone) > 0 
                        AND a.phone NOT LIKE '%[not provided]%' THEN 1
                        WHEN a.phone IS NULL AND l.phone IS NOT NULL 
                        AND length(l.phone) > 0 and l.phone 
                        NOT LIKE '%[not provided]%' THEN 1        
                        ELSE 0
                      END as has_phone,
                      CASE
                        WHEN a.stitch__id___c IS NOT NULL THEN a.stitch__id___c
                        WHEN a.stitch__id___c IS NULL AND l.stitch__id___c IS NOT NULL 
                        THEN l.stitch__id___c
                        ELSE NULL
                      END AS stitch_account_id,
                      l.lead_source,
                      CASE 
                        WHEN fls.first_lead_id IS NOT NULL THEN fls.first_lead_id
                        ELSE l.id
                      END as first_lead_id,
                      CASE 
                        WHEN fls.first_lead_created_date IS NOT NULL 
                        THEN fls.first_lead_created_date
                        ELSE l.created_date
                      END as first_lead_created_date, 
                      CASE 
                        WHEN fls.first_lead_source IS NOT NULL 
                        THEN fls.first_lead_source
                        ELSE l.lead_source
                      END as first_lead_source, 
                      CASE
                        WHEN a.confirmation__status___c IS NOT NULL 
                        THEN a.confirmation__status___c
                        WHEN a.confirmation__status___c IS NULL 
                        AND l.confirmation__status___c IS NOT NULL 
                        THEN l.confirmation__status___c
                        ELSE 'Unclassified'
                      END AS confirmation_status, 
                      CASE
                        WHEN a.last__confirmed__date___c IS NOT NULL 
                        THEN a.last__confirmed__date___c
                        ELSE l.last__confirmed__date___c
                      END AS last_confirmed_date, 
                      CASE
                        WHEN a.last__confirmed__by___c IS NOT NULL 
                        THEN a.last__confirmed__by___c
                        ELSE l.last__confirmed__by___c
                      END AS last_confirmed_by, 
                      CASE
                        WHEN a.date__confirmed__sales__ready___c IS NOT NULL 
                        THEN a.date__confirmed__sales__ready___c
                        ELSE l.date__confirmed__sales__ready___c
                      END AS first_sales_ready_date,
                      CASE
                        WHEN a.date__confirmed__not__sales__ready___c IS NOT NULL 
                        THEN a.date__confirmed__not__sales__ready___c
                        ELSE l.date__confirmed__not__sales__ready___c
                      END AS first_sales_not_ready_date,
                      CASE
                        WHEN a.date__confirmed__disqualified___c IS NOT NULL 
                        THEN a.date__confirmed__disqualified___c
                        ELSE l.date__confirmed__disqualified___c
                      END AS first_disqual_date,
                      CASE
                        WHEN a.mid__market___c IS NOT NULL THEN a.mid__market___c
                        ELSE l.mid__market___c
                      END AS mid_market, 
                      CASE 
                        WHEN a.classification_c IS NOT NULL THEN a.classification_c
                        WHEN a.classification_c IS NULL AND l.classification_c IS NOT NULL 
                        THEN l.classification_c
                        ELSE 'Unknown'
                      END AS classification
                      
                    FROM salesforce_fivetran._lead l
                    LEFT JOIN salesforce_fivetran._account a 
                    ON l.converted_account_id = a.id 
                    AND a.is_deleted IS NOT TRUE
                    AND a.ignore__data___c IS NOT TRUE
                    
                    LEFT JOIN (
                      SELECT
                        account_id,  
                        lead_id as first_lead_id, 
                        created_date as first_lead_created_date, 
                        lead_source as first_lead_source
                      FROM (
                        SELECT 
                          a.id AS account_id,   
                          l.id as lead_id,                    
                          l.lead_source, 
                          l.created_date, 
                          l.date__confirmed__sales__ready___c, 
                          l.last__confirmed__date___c,
                          row_number() OVER (PARTITION BY a.id 
                            ORDER BY l.created_date ASC) AS row_number 
                        FROM salesforce_fivetran._account a
                        JOIN salesforce_fivetran._lead l on a.id = l.converted_account_id
                        WHERE a.is_deleted IS NOT TRUE
                        AND l.is_deleted IS NOT TRUE
                        AND a.ignore__data___c IS NOT TRUE
                        AND l.ignore__data___c IS NOT TRUE
                      )
                      WHERE row_number = 1
                    ) fls ON  a.id = fls.account_id 
                    
                    WHERE l.is_deleted IS NOT TRUE
                    AND l.ignore__data___c IS NOT TRUE
                  ) a
                  LEFT JOIN data_warehouse.internal_stitch_team s 
                  ON a.last_confirmed_by = s.id
                  LEFT JOIN data_warehouse.salesforce_transfer st 
                  ON a.stitch_account_id = st.account_id
                );
                GRANT       select 
                ON table    snowplow_sessionization.sf_confirmation_status
                TO          mode, GROUP data_team;
                """
                )
    db.commit()

# create the sf_demo_events table
    cur.execute(
                """
                DROP TABLE IF EXISTS snowplow_sessionization.sf_demo_events;
                CREATE TABLE snowplow_sessionization.sf_demo_events
                AS
                (
                SELECT
                  a.id AS account_id, 
                  a.stitch__id___c AS stitch_account_id, 
                  CASE
                    WHEN d.num_demo_sched IS NOT NULL THEN 1
                    ELSE 0
                  END AS had_demo_scheduled, 
                  CASE 
                    WHEN d.num_demo_sched IS NOT NULL THEN d.num_demo_sched
                    ELSE 0
                  END num_demo_sched,
                  e.first_demo_sched_date,
                  CASE
                    WHEN b.num_demo_complete IS NOT NULL THEN 1
                    ELSE 0
                  END AS had_demo,
                  CASE
                    WHEN b.num_demo_complete IS NOT NULL THEN b.num_demo_complete
                    ELSE 0
                  END AS num_demo_complete, 
                  c.first_demo_date, 
                  a.mid__market___c, 
                  a.classification_c
                  
                  FROM salesforce_fivetran._account a
                  
                  LEFT JOIN (
                    SELECT 
                    a.id AS account_id,  
                    count(DISTINCT e.id) AS num_demo_complete
                    FROM salesforce_fivetran._account a
                    JOIN salesforce_fivetran._event e ON a.id = e.account_id
                    WHERE e.type = 'Demo Complete'
                    AND a.is_deleted IS NOT TRUE
                    AND e.is_deleted IS NOT TRUE
                    AND a.ignore__data___c IS NOT TRUE
                    GROUP BY a.id
                  ) b ON a.id = b.account_id
                  
                  LEFT JOIN (
                    SELECT
                    account_id,  
                    first_demo_date
                    FROM
                    (
                      SELECT 
                      a.id AS account_id, 
                      e.start_date_time AS first_demo_date,
                      row_number() OVER (PARTITION BY a.id ORDER BY e.start_date_time ASC) 
                            AS row_number 
                      FROM salesforce_fivetran._account a
                      JOIN salesforce_fivetran._event e on a.id = e.account_id
                      WHERE e.type = 'Demo Complete'
                      AND a.is_deleted IS NOT TRUE
                      AND e.is_deleted IS NOT TRUE
                      AND a.ignore__data___c IS NOT TRUE
                    )
                    WHERE row_number = 1
                  ) c ON a.id = c.account_id
                  
                  LEFT JOIN (
                    SELECT 
                    a.id AS account_id,  
                    count(DISTINCT e.id) num_demo_sched
                    FROM salesforce_fivetran._account a
                    JOIN salesforce_fivetran._event e ON a.id = e.account_id
                    WHERE e.type LIKE 'Demo%'
                    AND a.is_deleted IS NOT TRUE
                    AND e.is_deleted IS NOT TRUE
                    AND a.ignore__data___c IS NOT TRUE
                    GROUP BY a.id
                  ) d ON a.id = d.account_id
                  
                  LEFT JOIN (
                    SELECT
                    account_id,  
                    first_demo_sched_date
                    FROM
                    (
                      SELECT 
                      a.id AS account_id, 
                      e.created_date AS first_demo_sched_date,
                      row_number() OVER (PARTITION BY a.id ORDER BY e.created_date ASC) 
                        AS row_number 
                      FROM salesforce_fivetran._account a
                      JOIN salesforce_fivetran._event e on a.id = e.account_id
                      WHERE e.type LIKE 'Demo%'
                      AND a.is_deleted IS NOT TRUE
                      AND e.is_deleted IS NOT TRUE
                      AND a.ignore__data___c IS NOT TRUE
                    )
                    WHERE row_number = 1
                  ) e ON a.id = e.account_id

                  WHERE a.is_deleted IS NOT TRUE
                  AND a.ignore__data___c IS NOT TRUE
                );
                GRANT       select 
                ON table    snowplow_sessionization.sf_demo_events
                TO          mode, GROUP data_team;
                """
                )
    db.commit()

# create the derived_lead_conversion table
    cur.execute(
                """
                DROP TABLE IF EXISTS snowplow_sessionization.derived_lead_conversion;
                CREATE TABLE snowplow_sessionization.derived_lead_conversion
                diststyle key
                distkey(lead_id)
                compound sortkey(conversion_type,lead_id,conversion_date)
                AS
                (
                select 
                      lead_id, 
                      conversion_type, 
                      DATEADD(MINUTE,5,conversion_date) as conversion_date
                      FROM
                      (
                        (
                          select lead_id, 'reg' as conversion_type, 
                          --CONVERT_TIMEZONE('America/Los_Angeles','GMT',
                          sft.reg_date
                          --) 
                          as conversion_date
                          FROM data_warehouse.salesforce_transfer sft,
                          snowplow_sessionization.sf_confirmation_status as lead
                          WHERE lead.stitch_account_id = sft.account_id
                          and sft.ignore_data = 0
                        )
                        UNION ALL
                        (
                          select lead_id, 'cc' as conversion_type, 
                          --CONVERT_TIMEZONE('America/Los_Angeles','GMT',
                          sft.cc_date_added
                          --) 
                          as conversion_date
                          FROM data_warehouse.salesforce_transfer sft,
                          snowplow_sessionization.sf_confirmation_status as lead
                          WHERE lead.stitch_account_id = sft.account_id
                          and sft.ignore_data = 0
                          and sft.cc_date_added is not null
                        )
                        UNION ALL
                        (
                          select 
                            lead_id as lead_id,
                            'lead_creation' as conversion_type,
                             CONVERT_TIMEZONE('GMT','America/Los_Angeles', 
                             lead.first_lead_created_date) as conversion_date
                          FROM
                            snowplow_sessionization.sf_confirmation_status as lead
                        )
                        UNION ALL
                        (
                          select 
                            id as lead_id,
                            'all' as conversion_type,
                             CONVERT_TIMEZONE('GMT','America/Los_Angeles', getdate()) as conversion_date
                          FROM
                            salesforce_fivetran._lead as lead
                        )
                      )    
                );
                """
                )
    db.commit()

    cur.execute(
                """
                GRANT       select 
                ON table    snowplow_sessionization.derived_lead_conversion
                TO          mode, GROUP data_team;
                """
                )
#######################################################################################################
    db.close()
