-----------------------------------------------------------------------------
-- This file contains Verizon Business CONFIDENTIAL code.
--
-- (C) COPYRIGHT 2014 Verizon Business Inc.
-- All rights Reserved Worldwide
-- US Government Users Restricted Rights - Use, duplication or
-- disclosure restricted by GSA ADP Schedule Contract with Verizon
-- Business Inc.
--
-- DDL         : "stp_load_outage_mdn.hql"
--
-- Author      : Akhilesh S
--
-- Description : This DDL is used to create OC mdn Events into IVR
--
-- Usage:
--    /usr/bin/hive -hiveconf "DATABASE_NAME"="$HIVEDB" -hiveconf "SUCCESS_THRLD"="$SUCCESS_THRESHOLD" \
--    -hiveconf "NSRPARTITION"="$last_partition" \
--    -hiveconf "OUTAGEID"="$outage" \
--    -hiveconf "PROCESS_LOADTIME"="$running_timestamp" \
--    -hiveconf "CURRENT_LOADTIME"="$current_time" \
--    -f $LOAD_STP_OUTAGE_MEDIATION_HIVE_FILE >>$LOGFILE 2>&1
--
-- ----------------------------------------------------------------------------------


-------------------------------------------------------------------------------
--1. All MDNs visiting the enodeb affected by outage for the hour of the day
-------------------------------------------------------------------------------

INSERT INTO ${hiveconf:DATABASE_NAME}.STP_OUTAGE_ENODEB_MDN_DAY_HOUR PARTITION(OUTAGE_DATE)
--(outage_id, outage_date_hour, outage_day, outage_hour, outage_start_time, enodebid, emin, mdn, cumulative_success_connect_count, mediation_week_count, load_time, city, state, next_15min_outage_time)
SELECT MAX(OUTAGE_ID) OUTAGE_ID,
       MAX(NSR_DATE) OUTAGE_DATE_HOUR,
       NSR_DAY OUTAGE_DAY,
       SUBSTR(MAX(NSR_DATE),9) OUTAGE_HOUR,
       MAX(START_TIME) OUTAGE_START_TIME,
       '' ENODEBID,
       EMIN,
       MAX(MDN) MDN,
       SUM(CONNECT_ATTEMPTS) CUMM_SUCCESS_CONNECTS,
       COUNT(1) NUM_WEEKS,
       '${hiveconf:CURRENT_LOADTIME}' LOAD_TIME,
       MAX(CITY) CITY,
       MAX(STATE) STATE,
       MAX(NEXT_15MIN_OUTAGE_TIME) NEXT_15MIN_OUTAGE_TIME,
       MAX(NSR_DATE) OUTAGE_DATE
 FROM (SELECT IOM.MDN,NSR.*,IO.* 
         FROM (SELECT DISTINCT OUTAGE_ID, MDN FROM ${hiveconf:DATABASE_NAME}.STP_IVR_OUTAGE_MDNS) IOM
       INNER JOIN
              (SELECT OUTAGE_ID,MAX(START_TIME) START_TIME,CAST(MAX(OUTAGE_START_DATE_HOUR) AS BIGINT) NSR_START_HOUR,MAX(CITY) CITY,MAX(STATE) STATE,MAX(NEXT_15MIN_OUTAGE_TIME) NEXT_15MIN_OUTAGE_TIME   
                FROM ${hiveconf:DATABASE_NAME}.STP_OUTAGE_MEDIATION_LOG
               WHERE OUTAGE_ID=${hiveconf:OUTAGEID} GROUP BY OUTAGE_ID
              ) IO ON IO.OUTAGE_ID=IOM.OUTAGE_ID
       INNER JOIN
              ${hiveconf:DATABASE_NAME}.NSR_GEOSPATIAL_HOURLY_AGG NSR
          ON  NSR.NSR_DATE=NSR_START_HOUR
         AND  IOM.MDN=NSR.MTN
       UNION ALL
              SELECT IOM.MDN,NSR.*,IO.* FROM (SELECT DISTINCT OUTAGE_ID, MDN FROM ${hiveconf:DATABASE_NAME}.STP_IVR_OUTAGE_MDNS) IOM
       INNER JOIN
              (
              SELECT OUTAGE_ID,MAX(START_TIME) START_TIME,CAST(MAX(OUTAGE_START_DATE_HOUR_WEEK1) AS BIGINT) NSR_DATE_WEEK1,MAX(CITY) CITY,MAX(STATE) STATE,MAX(NEXT_15MIN_OUTAGE_TIME) NEXT_15MIN_OUTAGE_TIME FROM ${hiveconf:DATABASE_NAME}.STP_OUTAGE_MEDIATION_LOG WHERE OUTAGE_ID=${hiveconf:OUTAGEID} GROUP BY OUTAGE_ID
              ) IO ON IO.OUTAGE_ID=IOM.OUTAGE_ID
       INNER JOIN
              ${hiveconf:DATABASE_NAME}.NSR_GEOSPATIAL_HOURLY_AGG NSR
         ON   NSR.NSR_DATE=NSR_DATE_WEEK1
         AND  IOM.MDN=SUBSTR(NSR.CELLTOWER,2)
       UNION ALL
             SELECT IOM.MDN,NSR.*,IO.* FROM (SELECT DISTINCT OUTAGE_ID, MDN FROM ${hiveconf:DATABASE_NAME}.STP_IVR_OUTAGE_MDNS) IOM
       INNER JOIN
              (
              SELECT OUTAGE_ID,MAX(START_TIME) START_TIME,CAST(MAX(OUTAGE_START_DATE_HOUR_WEEK2) AS BIGINT) NSR_DATE_WEEK2,MAX(CITY) CITY ,MAX(STATE) STATE,MAX(NEXT_15MIN_OUTAGE_TIME) NEXT_15MIN_OUTAGE_TIME  FROM ${hiveconf:DATABASE_NAME}.STP_OUTAGE_MEDIATION_LOG WHERE OUTAGE_ID=${hiveconf:OUTAGEID} GROUP BY OUTAGE_ID
              ) IO ON IO.OUTAGE_ID=IOM.OUTAGE_ID
       INNER JOIN
             ${hiveconf:DATABASE_NAME}.NSR_GEOSPATIAL_HOURLY_AGG NSR
         ON  NSR.NSR_DATE=NSR_DATE_WEEK2
        AND  IOM.MDN=SUBSTR(NSR.CELLTOWER,2)
       UNION ALL
             SELECT IOM.MDN,NSR.*,IO.* FROM (SELECT DISTINCT OUTAGE_ID, MDN FROM ${hiveconf:DATABASE_NAME}.STP_IVR_OUTAGE_MDNS) IOM
       INNER JOIN
             (
             SELECT OUTAGE_ID,MAX(START_TIME) START_TIME,CAST(MAX(OUTAGE_START_DATE_HOUR_WEEK3) AS BIGINT) NSR_DATE_WEEK3,MAX(CITY) CITY,MAX(STATE) STATE,MAX(NEXT_15MIN_OUTAGE_TIME) NEXT_15MIN_OUTAGE_TIME FROM ${hiveconf:DATABASE_NAME}.STP_OUTAGE_MEDIATION_LOG WHERE OUTAGE_ID=${hiveconf:OUTAGEID} GROUP BY OUTAGE_ID
             ) IO ON IO.OUTAGE_ID=IOM.OUTAGE_ID
       INNER JOIN
             ${hiveconf:DATABASE_NAME}.NSR_GEOSPATIAL_HOURLY_AGG NSR
         ON  NSR.NSR_DATE=NSR_DATE_WEEK3
        AND  IOM.MDN=SUBSTR(NSR.CELLTOWER,2)
       UNION ALL
             SELECT IOM.MDN,NSR.*,IO.* FROM (SELECT DISTINCT OUTAGE_ID, MDN FROM ${hiveconf:DATABASE_NAME}.STP_IVR_OUTAGE_MDNS) IOM
       INNER JOIN
             (
             SELECT OUTAGE_ID,MAX(START_TIME) START_TIME,CAST(MAX(OUTAGE_START_DATE_HOUR_WEEK4) AS BIGINT) NSR_DATE_WEEK4,MAX(CITY) CITY,MAX(STATE) STATE ,MAX(NEXT_15MIN_OUTAGE_TIME) NEXT_15MIN_OUTAGE_TIME FROM ${hiveconf:DATABASE_NAME}.STP_OUTAGE_MEDIATION_LOG WHERE OUTAGE_ID=${hiveconf:OUTAGEID} GROUP BY OUTAGE_ID
             ) IO ON IO.OUTAGE_ID=IOM.OUTAGE_ID
       INNER JOIN
             ${hiveconf:DATABASE_NAME}.NSR_GEOSPATIAL_HOURLY_AGG NSR
         ON  NSR.NSR_DATE=NSR_DATE_WEEK4
        AND  IOM.MDN=SUBSTR(NSR.CELLTOWER,2)
      ) A 
 GROUP BY MDN,EMIN,NSR_DAY;

-------------------------------------------------------------------------------
--3. Creating event for each MDN
-------------------------------------------------------------------------------

INSERT INTO ${hiveconf:DATABASE_NAME}.STP_CUSTOMER_EVENT_HISTORY_IVR PARTITION(EVENT_DAY) 
--(event_date_gmt, mdn, emin, imei, imsi, record_type, event_id, event_type, event_name, event_location, event_details, service_impacted, service_type, start_date, resolved_date, resolved_category, severity, create_date, data_source, customer_impacted_time_intervals, outage_customer_status, outage_customer_location, event_duration, load_time)
SELECT 
Outage.EVENT_DATE_GMT ,
Outage.MDN,
Outage.EMIN,
Outage.IMEI,
Outage.IMSI,
Outage.RECORD_TYPE,
Outage.EVENT_ID,
Outage.EVENT_TYPE,
Outage.EVENT_NAME,
Outage.EVENT_LOCATION,
Outage.EVENT_DETAILS,
Outage.SERVICE_IMPACTED,
Outage.SERVICE_TYPE,
Outage.START_DATE,
NULL RESOLVED_DATE,
'Not Resolved'  RESOLVED_CATEGORY,
Outage.SEVERITY,
CURRENT_TIMESTAMP AS CREATE_DATE,
'Mediation, OC' AS DATA_SOURCE,
NULL customer_impacted_time_intervals,
NULL outage_customer_status,
NULL outage_customer_location,
CAST('0' AS BIGINT) event_duration,
current_timestamp load_time,
DATE_FORMAT(EVENT_DATE_GMT,'YYYYMMdd') AS EVENT_DAY
FROM
(                 SELECT 
                         OE.OUTAGE_START_TIME EVENT_DATE_GMT
                        ,CMT.MDN
                        ,CMT.EMIN
                        ,CMT.IMEI
                        ,CMT.IMSI
                        ,'NetworkEvent'             Record_Type
                        ,OE.OUTAGE_ID               EVENT_ID
                        ,'Outage'                   EVENT_TYPE
                        ,NULL                       EVENT_NAME
                        ,NULL                       EVENT_LOCATION
                        ,NULL                       EVENT_DETAILS
                        ,'All'                      SERVICE_IMPACTED
                        ,NULL                       SERVICE_TYPE
                        ,OE.OUTAGE_START_TIME              START_DATE
                        ,NULL SEVERITY,
                        ROW_NUMBER() OVER(PARTITION BY OE.OUTAGE_ID,CMT.MDN ORDER BY CMT.LOADTIME DESC) RN
                    FROM ${hiveconf:DATABASE_NAME}.STP_OUTAGE_ENODEB_MDN_DAY_HOUR OE
                        ,${hiveconf:DATABASE_NAME}.STP_MIN_TRANSLATION CMT
                   WHERE OE.OUTAGE_DATE='${hiveconf:NSRPARTITION}'
                         AND OE.OUTAGE_ID=${hiveconf:OUTAGEID}
                         AND OE.CUMULATIVE_SUCCESS_CONNECT_COUNT >=${hiveconf:SUCCESS_THRLD}
                         AND OE.EMIN = CMT.EMIN
) outage
WHERE outage.RN=1;


-----------------------------------------------------------------------------------
--5. Insert details into log table, will be used by next run to get processed hour
-----------------------------------------------------------------------------------

INSERT INTO ${hiveconf:DATABASE_NAME}.STP_OUTAGE_MEDIATION_LOG 
SELECT OUTAGE_ID,
       MAX(START_TIME),
       FROM_UNIXTIME(UNIX_TIMESTAMP(MAX(OUTAGE_START_DATE_HOUR),'yyyyMMddHH')+3600,'yyyyMMddHH'),
       FROM_UNIXTIME(UNIX_TIMESTAMP(MAX(OUTAGE_START_DATE_HOUR_WEEK1),'yyyyMMddHH')+3600,'yyyyMMddHH'),
       FROM_UNIXTIME(UNIX_TIMESTAMP(MAX(OUTAGE_START_DATE_HOUR_WEEK2),'yyyyMMddHH')+3600,'yyyyMMddHH'),
       FROM_UNIXTIME(UNIX_TIMESTAMP(MAX(OUTAGE_START_DATE_HOUR_WEEK3),'yyyyMMddHH')+3600,'yyyyMMddHH'),
       FROM_UNIXTIME(UNIX_TIMESTAMP(MAX(OUTAGE_START_DATE_HOUR_WEEK4),'yyyyMMddHH')+3600,'yyyyMMddHH'),
       MAX(MEDIATION_START_TIME) MEDIATION_START_TIME ,
       CURRENT_TIMESTAMP MEDIATION_END_TIME,
       'COMPLETED' JOB_STATUS,
       '${hiveconf:PROCESS_LOADTIME}' LOAD_TIME,
       MAX(CITY),
       MAX(STATE),
       MAX(NEXT_15MIN_OUTAGE_TIME)
FROM ${hiveconf:DATABASE_NAME}.STP_OUTAGE_MEDIATION_LOG WHERE OUTAGE_ID=${hiveconf:OUTAGEID} GROUP BY OUTAGE_ID;

-------------------------------------------------------------------------------
