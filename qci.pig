-- ----------------------------------------------------------------------------------
--
-- This file contains Verizon Business CONFIDENTIAL code.
--
-- (C) COPYRIGHT 2014 Verizon Business Inc.
-- All rights Reserved Worldwide
-- US Government Users Restricted Rights - Use, duplication or
-- disclosure restricted by GSA ADP Schedule Contract with Verizon
-- Business Inc.
--
-- Script      : stp_rtt_load_customer_events_qci.pig
--
-- Author      : Sarvani A
--
-- Description : This script loads the QCI switch events to snapshot and qci history HDFS locations
--
-- Usage:
--      /usr/bin/pig -Dpig.additional.jars=/usr/hdp/current/pig-client/piggybank.jar -Dexectype=tez -useHCatalog -f $STPBASE/scripts/stp_rtt_load_customer_events_qci.pig -l $STPBASE/logs -param source_schema=$STPSCHEMA  -param p_table=agg_p_cell_sub_kpi_f  -param history_path=$STPHDFSBASE/rtt/stp_rtt_customer_event_history_qci/event_day_tmp=2019-03-12  -param mintranstbl=STP_MIN_TRANSLATION -param snapshot_inpath=$STPHDFSBASE/rtt/stp_rtt_customer_event_qci_snapshot  -param snapshot_path=$STPHDFSBASE/stp_rtt_customer_event_qci_snapshot/2019-03-12'_tmp' -param transdatetime=201903120000  >>$STPBASE/logs/stp_rtt_load_customer_events_qci.20190318.log 2>&1
--
----------------------------------------------------------------------------------------

SET pig.tmpfilecompression false;
--SET opt.multiquery false;
SET hive.exec.orc.split.strategy BI;

SET mapreduce.map.java.opts -Xmx3072M;
SET mapreduce.task.io.sort.mb 1024;
SET pig.maxCombinedSplitSize 67108864;
SET mapreduce.map.memory.mb 4096;
SET mapreduce.reduce.java.opts -Xmx6144M;
SET mapreduce.reduce.memory.mb 8192;
SET mapred.output.compress true;
SET mapreduce.job.reduce.slowstart.completedmaps 0.90;

SET ipc.maximum.data.length 268435456;

SET pig.tez.opt.union false;
SET tez.am.container.idle.release-timeout-min.millis 5000;
SET tez.am.container.idle.release-timeout-max.millis 10000;
SET tez.am.resource.memory.mb 8192;
SET tez.task.resource.memory.mb 8192;
SET tez.runtime.io.sort.mb 1024;
SET tez.runtime.unordered.output.buffer.size-mb 2048;
SET tez.grouping.min-size 16777216;

--Load P-Table
load_rtt_15min_data = LOAD '$source_schema.$p_table' USING org.apache.hive.hcatalog.pig.HCatLoader();

--Filter by current date and hr
p_table_filter_by_date = FILTER load_rtt_15min_data BY (p_date_key==(long)'$transdatetime');

--Filter by mdn,imei,imsi not nulls
p_table_filter_nulls = FILTER p_table_filter_by_date BY (subscriber_key is not NULL and imei is not NULL and imsi is not NULL);

--load necessary columns
parse_p_table = FOREACH p_table_filter_nulls GENERATE
CASE WHEN subscriber_key is not null THEN REPLACE(subscriber_key,'^[1]','') ELSE subscriber_key END as mdn:chararray,
imei as imei:chararray,
imsi as imsi:chararray,
CASE WHEN (INDEXOF(uectxtrelease_qci_array,'8',0) != -1)AND(INDEXOF(uectxtrelease_qci_array,'9',0) == -1) THEN '8'  WHEN (INDEXOF(uectxtrelease_qci_array,'8',0) == -1)AND(INDEXOF(uectxtrelease_qci_array,'9',0) != -1) THEN '9' WHEN (INDEXOF(uectxtrelease_qci_array,'8',0) != -1)AND(INDEXOF(uectxtrelease_qci_array,'9',0) != -1) THEN '9' ELSE 'NA' END AS qci_status:chararray,
SUBSTRING((chararray)p_date_key,0,12) as p_date_key:chararray;

--Remove records having qci_status as 'NA'
p_table_remove_nas = FILTER parse_p_table BY qci_status!='NA';

--Load QCI snapshot data from HDFS out path
load_qci_snapshot_data = LOAD '$snapshot_inpath' USING PigStorage('|') AS (
event_date_gmt:chararray,
mdn:chararray,
imei:chararray,
imsi:chararray,
record_type:chararray,
event_id:chararray,
event_type:chararray,
prev_qci_status:chararray,
latest_qci_status:chararray,
event_details:chararray,
start_date:chararray,
create_date:chararray,
is_first_time_load:chararray,
service_impacted:chararray,
service_type:chararray,
resolved_date:chararray,
resolved_category:chararray,
severity:chararray,
event_category:chararray,
datasource:chararray,
load_time:chararray,
event_date:chararray);

--Join both by customer n check qci_status
qci_rec_join = JOIN p_table_remove_nas BY (mdn,imei,imsi) FULL OUTER, load_qci_snapshot_data BY (mdn,imei,imsi);

--Get values from join
parse_qci_join = FOREACH qci_rec_join GENERATE
    ((load_qci_snapshot_data::mdn is NULL) ? p_table_remove_nas::mdn : load_qci_snapshot_data::mdn) AS mdn:chararray,
    ((load_qci_snapshot_data::imei is NULL) ? p_table_remove_nas::imei : load_qci_snapshot_data::imei) AS imei:chararray,
    ((load_qci_snapshot_data::imsi is NULL) ? p_table_remove_nas::imsi : load_qci_snapshot_data::imsi) AS imsi:chararray,
    'QCI SWITCH' AS record_type:chararray,
    NULL AS event_id:chararray,
    'QCI SWITCH' AS event_type:chararray,
    NULL AS service_impacted:chararray,
    NULL AS service_type:chararray,
    NULL AS resolved_date:chararray,
    NULL AS resolved_category:chararray,
    NULL AS severity:chararray,
    'QCI SWITCH' AS event_category:chararray,
    CASE WHEN (p_table_remove_nas::qci_status is NULL) THEN (load_qci_snapshot_data::latest_qci_status, load_qci_snapshot_data::prev_qci_status, load_qci_snapshot_data::latest_qci_status,(int)0,(int)load_qci_snapshot_data::is_first_time_load,(chararray)load_qci_snapshot_data::start_date,(chararray)load_qci_snapshot_data::create_date,load_qci_snapshot_data::event_details)
             WHEN (load_qci_snapshot_data::latest_qci_status is NULL) and ((p_table_remove_nas::qci_status matches '8') or (p_table_remove_nas::qci_status matches '9')) THEN (p_table_remove_nas::qci_status, 'NA', p_table_remove_nas::qci_status,(int)0,(int)1,(chararray)ToString(ToDate(p_table_remove_nas::p_date_key,'yyyyMMddHHmm'),'yyyy-MM-dd HH:mm:ss'),(chararray)ToString(CurrentTime(),'yyyy-MM-dd HH:mm:ss'),CONCAT('Network detected QCI SWITCH from NA to ',p_table_remove_nas::qci_status))
                 WHEN (p_table_remove_nas::qci_status matches load_qci_snapshot_data::latest_qci_status) THEN (load_qci_snapshot_data::latest_qci_status, load_qci_snapshot_data::prev_qci_status, load_qci_snapshot_data::latest_qci_status,(int)0,(int)0,(chararray)load_qci_snapshot_data::start_date,(chararray)load_qci_snapshot_data::create_date,load_qci_snapshot_data::event_details)
                 WHEN ((p_table_remove_nas::qci_status matches '8') and (load_qci_snapshot_data::latest_qci_status matches '9')) or ((p_table_remove_nas::qci_status matches '9') and (load_qci_snapshot_data::latest_qci_status matches '8')) THEN (p_table_remove_nas::qci_status, load_qci_snapshot_data::latest_qci_status,p_table_remove_nas::qci_status,(int)1,(int)0,(chararray)ToString(ToDate(p_table_remove_nas::p_date_key,'yyyyMMddHHmm'),'yyyy-MM-dd HH:mm:ss'),(chararray)ToString(CurrentTime(),'yyyy-MM-dd HH:mm:ss'),CONCAT('Network detected QCI SWITCH from ',load_qci_snapshot_data::latest_qci_status,' to ',p_table_remove_nas::qci_status))
                 END
                AS snap:(erab:chararray, prev_qci_status: chararray, latest_qci_status: chararray,create_event: int,is_first_time: int,start_date:chararray,create_date:chararray,event_details:chararray);

--parse updated snapshot tuple
new_qci_snapshot = foreach parse_qci_join generate
mdn,
imei,
imsi,
record_type,
event_id,
event_type,
service_impacted,
service_type,
resolved_date,
resolved_category,
severity,
event_category,
'RTT' as datasource,
flatten(snap) as (qci_status, prev_qci_status, latest_qci_status, create_event, is_first_time, start_date, create_date, event_details);

snapshot_grouped = GROUP new_qci_snapshot BY (mdn,imei,imsi);

snapshot_remove_dups = FOREACH snapshot_grouped {
  latest = LIMIT new_qci_snapshot 1;
  GENERATE FLATTEN(latest);
};

--Filter events which are not first_time_load
qci_event_history = filter snapshot_remove_dups by ((is_first_time==0) and (create_event==1));

--Remove create_event flag and set loadtime and eventDay as create_date
final_snapshot = foreach snapshot_remove_dups generate
latest::start_date as event_date_gmt:chararray,
latest::mdn as mdn:chararray,
latest::imei as imei:chararray,
latest::imsi as imsi:chararray,
latest::record_type as record_type:chararray,
latest::event_id as event_id:chararray,
latest::event_type as event_type:chararray,
latest::prev_qci_status as prev_qci_status:chararray,
latest::latest_qci_status as latest_qci_status:chararray,
latest::event_details as event_details:chararray,
latest::start_date as start_date:chararray,
latest::create_date as create_date:chararray,
latest::is_first_time as is_first_time:chararray,
latest::service_impacted as service_impacted:chararray,
latest::service_type as service_type:chararray,
latest::resolved_date as resolved_date:chararray,
latest::resolved_category as resolved_category:chararray,
latest::severity as severity:chararray,
latest::event_category as event_category:chararray,
latest::datasource as datasource:chararray,
latest::create_date as load_time,
ToString(ToDate(latest::start_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd') as event_day;

--Remove create event flag and generate final frame
--VZWHD-1445 - IMEI mismatch in p_table and min_trans. Sol: Taking only 14digits of IMEI
final_event_history = FOREACH qci_event_history GENERATE
latest::start_date as event_date_gmt:chararray,
latest::mdn as mdn:chararray,
SUBSTRING(latest::imei,0,14) as imei:chararray,
latest::imsi as imsi:chararray,
latest::record_type as record_type:chararray,
latest::event_id as event_id:chararray,
latest::event_type as event_type:chararray,
latest::prev_qci_status as prev_qci_status:chararray,
latest::latest_qci_status as latest_qci_status:chararray,
latest::event_details as event_details:chararray,
latest::start_date as start_date:chararray,
latest::create_date as create_date:chararray,
latest::is_first_time as is_first_time:chararray,
latest::service_impacted as service_impacted:chararray,
latest::service_type as service_type:chararray,
latest::resolved_date as resolved_date:chararray,
latest::resolved_category as resolved_category:chararray,
latest::severity as severity:chararray,
latest::event_category as event_category:chararray,
latest::datasource as datasource:chararray,
latest::create_date as load_time,
ToString(ToDate(latest::start_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd') as event_day;

--LOAD stp_min_translation
min_translation_snapshot = LOAD '$source_schema.$mintranstbl' USING org.apache.hive.hcatalog.pig.HCatLoader() AS (
mdn:chararray,
imei:chararray,
imsi:chararray,
emin:chararray,
nm_first:chararray,
nm_last:chararray,
billing_address:chararray,
loadtime:datetime
);

--Get latest EMIN
min_translation_grouped = GROUP min_translation_snapshot BY (mdn,imei,imsi);

min_translation_latest = FOREACH min_translation_grouped
{
  sorted = ORDER min_translation_snapshot BY loadtime DESC;
  latest = LIMIT sorted 1;
  GENERATE FLATTEN(latest);
};
--VZWHD-1445 - IMEI mismatch in p_table and min_trans. Sol: Taking only 14digits of IMEI
min_translation_trimmed = FOREACH min_translation_latest GENERATE
latest::mdn  AS mdn:chararray,
SUBSTRING(latest::imei,0,14) AS imei:chararray,
latest::imsi AS imsi:chararray,
latest::emin AS emin:chararray;

--Correlate with stp_min_translation to get emin
final_event_history_emin = JOIN final_event_history BY (mdn,imei,imsi), min_translation_trimmed BY (mdn,imei,imsi);

stp_event_history_qci = FOREACH final_event_history_emin GENERATE
final_event_history::event_date_gmt AS        event_date_gmt:chararray,
final_event_history::mdn  AS                  mdn:chararray,
min_translation_trimmed::emin AS              emin:chararray,
final_event_history::imei  AS                 imei:chararray,
final_event_history::imsi  AS                 imsi:chararray,
final_event_history::record_type  AS          record_type :chararray,
final_event_history::event_id  AS             event_id:chararray,
final_event_history::event_type  AS           event_type:chararray,
final_event_history::prev_qci_status  AS      prev_qci_status:chararray,
final_event_history::latest_qci_status  AS    latest_qci_status:chararray,
final_event_history::event_details  AS        event_details:chararray,
final_event_history::start_date  AS           start_date:chararray,
final_event_history::create_date  AS          create_date:chararray,
final_event_history::is_first_time  AS        is_first_time:chararray,
final_event_history::service_impacted  AS     service_impacted:chararray,
final_event_history::service_type  AS         service_type:chararray,
final_event_history::resolved_date  AS        resolved_date:chararray,
final_event_history::resolved_category  AS    resolved_category:chararray,
final_event_history::severity  AS             severity:chararray,
final_event_history::event_category  AS       event_category:chararray,
final_event_history::datasource  AS           datasource:chararray,
final_event_history::load_time  AS            load_time:chararray;


--Store snapshot to temporary location on HDFS. On successful Pig script execution, it will be moved to final HDFS location.
STORE final_snapshot INTO '$snapshot_path' USING PigStorage('|');

--Store qci_events to temporary location on HDFS. On successful Pig script execution, it will be moved to final HDFS location.
STORE stp_event_history_qci INTO '$history_path' USING PigStorage('|');
