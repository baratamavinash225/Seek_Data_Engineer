-- ----------------------------------------------------------------------------------
--
-- This file contains Verizon Business CONFIDENTIAL code.
--
-- All rights Reserved Worldwide
-- US Government Users Restricted Rights - Use, duplication or
-- disclosure restricted by GSA ADP Schedule Contract with Verizon
-- Business Inc.
--
-- DDL         : stp_rtt_customer_event_history_snapshot.hql
--
-- Author      : Sarvani A
--
-- Description : This DDL is to load event data to stp_rtt_customer_event_history table
--
-- Usage:
--   /usr/bin/hive -hiveconf DATABASE_NAME=$STPSCHEMA -hiveconf CURRENT_PART='2019-07-30' -hiveconf FILTER=$STP_RTT_LOAD_CUSTOMER_EVENT_HISTORY_HIVEFILTER -f $STPBASE/sql/stp_rtt_customer_event_history_snapshot.hql
--
-- Comments    : Invokes from shell script
--
-- ----------------------------------------------------------------------------------

--setting compression parameters to true
set hive.exec.compress.output=true;
set hive.exec.compress.intermediate=true;
--NTSBE-125 Change execution engine to TEZ to improve performance
SET hive.exec.orc.split.strategy=BI;
SET mapreduce.map.java.opts=-Xmx6144M;
SET mapreduce.task.io.sort.mb=1024;
SET mapreduce.map.memory.mb=8192;
SET mapreduce.reduce.java.opts=-Xmx6144M;
SET mapreduce.reduce.memory.mb=8192;
SET mapred.output.compress=true;
SET mapreduce.job.reduce.slowstart.completedmaps=0.90;
SET ipc.maximum.data.length=268435456;
SET tez.am.container.idle.release-timeout-min.millis=5000;
SET tez.am.container.idle.release-timeout-max.millis=10000;
SET tez.am.resource.memory.mb=8192;
SET tez.task.resource.memory.mb=8192;
SET tez.runtime.io.sort.mb=1024;
SET tez.runtime.unordered.output.buffer.size-mb=2048;
SET tez.grouping.min-size=16777216;
SET hive.execution.engine=tez;

--Dropping current partitions from Event History TABLE to make sure that latest record is loaded
ALTER TABLE ${hiveconf:DATABASE_NAME}.STP_RTT_CUSTOMER_EVENT_HISTORY DROP IF EXISTS PARTITION (LOAD_DAY='${hiveconf:CURRENT_PART}');

--Insert into event history snapshot table that will be used by APIs
INSERT INTO ${hiveconf:DATABASE_NAME}.STP_RTT_CUSTOMER_EVENT_HISTORY PARTITION (LOAD_DAY)
        SELECT
        DATE_FORMAT(EVENT_DATE_GMT,'yyyy-MM-dd HH:mm:ss') EVENT_DATE_GMT   ,
        MDN              ,
        SUBSCRIBER_ID    ,
        IMEI             ,
        IMSI             ,
        RECORD_TYPE      ,
        EVENT_ID         ,
        EVENT_TYPE       ,
        EVENT_NAME       ,
        EVENT_LOCATION   ,
        EVENT_DETAILS    ,
        SERVICE_IMPACTED ,
        SERVICE_TYPE     ,
        DATE_FORMAT(START_DATE,'yyyy-MM-dd HH:mm:ss') START_DATE,
        DATE_FORMAT(RESOLVED_DATE,'yyyy-MM-dd HH:mm:ss') RESOLVED_DATE,
        RESOLVED_CATEGORY,
        SEVERITY,
        CURRENT_TIMESTAMP AS CREATE_DATE,
        DATA_SOURCE,
        EVENT_CATEGORY,
        CUSTOMER_IMPACTED_TIME_INTERVALS,
        OUTAGE_CUSTOMER_STATUS,
        OUTAGE_CUSTOMER_LOCATION,
        EVENT_DURATION,
        CURRENT_DATE AS LOAD_DAY
        FROM (
                --EVENTS from IVR outages
                SELECT
                EVENT_DATE_GMT,
                MDN,
                SUBSCRIBER_ID,
                IMEI,
                IMSI,
                RECORD_TYPE,
                EVENT_ID,
                EVENT_TYPE,
                EVENT_NAME,
                EVENT_LOCATION,
                EVENT_DETAILS,
                SERVICE_IMPACTED,
                SERVICE_TYPE,
                START_DATE,
                RESOLVED_DATE,
                RESOLVED_CATEGORY,
                SEVERITY,
                DATA_SOURCE,
                'Network Event' EVENT_CATEGORY,
                CUSTOMER_IMPACTED_TIME_INTERVALS,
                OUTAGE_CUSTOMER_STATUS,
                OUTAGE_CUSTOMER_LOCATION,
                EVENT_DURATION
                FROM (
                        SELECT
                        DAT.*,
                        ROW_NUMBER() OVER(PARTITION BY EVENT_DAY,MDN,EVENT_ID ORDER BY CREATE_DATE DESC) RN
                        FROM ${hiveconf:DATABASE_NAME}.STP_RTT_CUSTOMER_EVENT_HISTORY_IVR DAT
                        WHERE EVENT_DAY >= ${hiveconf:FILTER} AND UPPER(EVENT_NAME) NOT LIKE '%TEST%'
                )EVENT_DATA WHERE RN=1
                -- Events from Remedy Table
                UNION ALL
                SELECT
                EVENT_DATE_GMT   ,
                MDN              ,
                SUBSCRIBER_ID    ,
                IMEI             ,
                IMSI             ,
                RECORD_TYPE      ,
                EVENT_ID         ,
                EVENT_TYPE       ,
                EVENT_NAME       ,
                EVENT_LOCATION   ,
                EVENT_DETAILS    ,
                SERVICE_IMPACTED ,
                SERVICE_TYPE     ,
                START_DATE       ,
                RESOLVED_DATE    ,
                RESOLVED_CATEGORY,
                SEVERITY,
                DATA_SOURCE,
                'NRB Ticket' EVENT_CATEGORY,
                NULL CUSTOMER_IMPACTED_TIME_INTERVALS,
                NULL OUTAGE_CUSTOMER_STATUS,
                NULL OUTAGE_CUSTOMER_LOCATION,
                NULL EVENT_DURATION
                FROM
                (SELECT
                        SCR.*,
                        ROW_NUMBER() OVER(PARTITION BY EVENT_DAY,MDN,EVENT_ID ORDER BY CREATE_DATE DESC) RN1
                        FROM ${hiveconf:DATABASE_NAME}.STP_RTT_CUSTOMER_EVENT_HISTORY_REMEDY SCR
                        WHERE EVENT_DAY  >= ${hiveconf:FILTER}
                ) dat WHERE RN1=1
                --Events from Device change
                UNION ALL
                SELECT
                EVENT_DATE_GMT   ,
                MDN              ,
                SUBSCRIBER_ID    ,
                IMEI             ,
                IMSI             ,
                RECORD_TYPE      ,
                EVENT_ID         ,
                EVENT_TYPE       ,
                EVENT_NAME       ,
                EVENT_LOCATION   ,
                EVENT_DETAILS    ,
                SERVICE_IMPACTED ,
                SERVICE_TYPE     ,
                START_DATE       ,
                RESOLVED_DATE    ,
                RESOLVED_CATEGORY,
                SEVERITY,
                DATA_SOURCE,
                'Device Change' EVENT_CATEGORY ,
                NULL CUSTOMER_IMPACTED_TIME_INTERVALS,
                NULL OUTAGE_CUSTOMER_STATUS,
                NULL OUTAGE_CUSTOMER_LOCATION,
                NULL  EVENT_DURATION
                FROM (
                        SELECT
                        SDC.*,
                        ROW_NUMBER() OVER(PARTITION BY EVENT_DAY,MDN,EVENT_ID ORDER BY CREATE_DATE DESC) RN2
                        FROM ${hiveconf:DATABASE_NAME}.STP_RTT_CUSTOMER_EVENT_HISTORY_DEVICE_CHANGE SDC
                        WHERE EVENT_DAY >= ${hiveconf:FILTER}
                ) dc WHERE RN2=1
                --Events from QCI Switch
                UNION ALL
                SELECT
                EVENT_DATE_GMT   ,
                MDN              ,
                SUBSCRIBER_ID    ,
                IMEI             ,
                IMSI             ,
                RECORD_TYPE      ,
                EVENT_ID         ,
                EVENT_TYPE,
                'QCI SWITCH' EVENT_NAME,
                NULL EVENT_LOCATION   ,
                EVENT_DETAILS    ,
                SERVICE_IMPACTED ,
                SERVICE_TYPE     ,
                START_DATE       ,
                RESOLVED_DATE    ,
                RESOLVED_CATEGORY,
                SEVERITY,
                DATA_SOURCE,
                EVENT_CATEGORY  ,
                NULL CUSTOMER_IMPACTED_TIME_INTERVALS,
                NULL OUTAGE_CUSTOMER_STATUS,
                NULL OUTAGE_CUSTOMER_LOCATION,
                NULL  EVENT_DURATION
                FROM(
                        SELECT
                        SQS.*,
                        ROW_NUMBER() OVER(PARTITION BY EVENT_DAY,MDN ORDER BY CREATE_DATE DESC) RN3
                        FROM ${hiveconf:DATABASE_NAME}.STP_RTT_SUBSCRIBER_EVENT_HISTORY_QCI SQS
                        WHERE EVENT_DAY >= ${hiveconf:FILTER}
                )qs
        ) final_data;

--Dropping old partitions from Event History TABLE for data purge
ALTER TABLE ${hiveconf:DATABASE_NAME}.STP_RTT_CUSTOMER_EVENT_HISTORY DROP IF EXISTS PARTITION (LOAD_DAY<'${hiveconf:LAST_PART}');
