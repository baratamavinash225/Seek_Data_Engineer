--
-- This file contains Verizon Business  CONFIDENTIAL code.
--
-- (C) COPYRIGHT 2014 Verizon Business Inc.
-- All rights Reserved Worldwide
-- US Government Users Restricted Rights - Use, duplication or
-- disclosure restricted by GSA ADP Schedule Contract with Verizon
-- Business Inc.
--
-- Script      : stp_rtt_druid_ingestion_vmas_scores_summary_hourly.pig
--
-- Author      : Akhilesh Varma
--
-- Usage:
--   pig -Dmapred.job.queue.name=etl-pig -x tez -useHCatalog -f stp_hourly_score_data_druid.pig -param source_schema='npi_cem_db' -param vmas_kpi_agg_tbl='stp_rtt_vmas_kpi_scores_subscriber_agg_hourly' -param subsc_area_device_tbl='stp_rtt_subscriber_area_device_scores_hourly' -param score_dt='20190320' -param score_hr='08' -param hdfs_out_path='/data/npicem/stp/RTT/stp_vmas_subscriber_device_scores_hourly' -param out_delim='|'
--
-- ----------------------------------------------------------------------------------
-- THE STORY --
-- ----------------------------------------------------------------------------------
--
SET pig.tmpfilecompression false;
SET opt.multiquery false;
set pig.tez.opt.union false
set mapreduce.map.java.opts -Xmx3072M
set mapreduce.map.memory.mb 3584
set mapreduce.reduce.java.opts -Xmx3072M
set mapreduce.reduce.memory.mb 3584
set mapreduce.task.io.sort.mb 1024
set pig.maxCombinedSplitSize 67108864
set hive.exec.orc.split.strategy BI

-- THE STORY --

-- ----------------------------------------------------------------------------------
--
-- 1. Load the subscriber_table snapshot
--

vmas_kpi_scores_data_filtered_hour = LOAD '$vmas_kpi_agg_dir' USING PigStorage('|') AS(
score_date_hr:chararray,
mdn:chararray,
imsi:chararray,
imei:chararray,
make:chararray,
model:chararray,
attach_failure_pct:double,
attach_failure_cnt:long,
attach_attempts_cnt:long,
rrc_setup_failure_pct:double,
rrc_setup_failure_cnt:long,
rrc_setup_attempts_cnt:long,
srf_pct:double,
service_request_failures_cnt:long,
service_request_attempts_cnt:long,
pcf_pct:double,
session_setup_failures_cnt:long,
session_setup_attempts_cnt:long,
cd_pct:double,
context_drops_cnt:long,
context_events_cnt:long,
sip_dropped_calls_pct:double,
volte_voice_calls_dropped_cnt:long,
volte_voice_setup_incomplete_calls_cnt:long,
rrc_radio_drop_pct:double,
radio_bearer_drops_cnt:long,
radio_bearer_setup_attempts_cnt:long,
downlink_throughput_kbps:double,
uplink_throughput_kbps:double,
downlink_data_volume:long,
downlink_active_time_ms:long,
uplink_data_volume:long,
uplink_active_time_ms:long,
travelling_indicator:long,
travelling_indicator_cnt:long,
volume_weighted_uplink_thpt_kbps:double,
volume_weighted_downlink_thpt_kbps:double,
load_time:chararray,
subscriber_id:long,
attach_failure_pct_weight:double,
rrc_setup_failure_pct_weight:double,
srf_pct_weight:double,
pcf_pct_weight:double,
cd_pct_weight:double,
sip_dropped_calls_pct_weight:double,
rrc_radio_drop_pct_weight:double,
downlink_throughput_kbps_weight:double,
uplink_throughput_kbps_weight:double,
ues_voice_retainability:double,
ues_voice_reliability:double,
ues_data:double,
ues_allserv:double,
preference_ratio:double);
--score_dt as score_dt:chararray,
--score_hr as score_hr:chararray;


--vmas_kpi_scores_data_filtered_hour = FILTER vmas_kpi_scores_data BY (score_dt=='$score_dt' and score_hr=='$score_hr');

-- ----------------------------------------------------------------------------------
--
-- 2. Generate the final data set with loadtime.

same_area_same_device_tbl = LOAD '$source_schema.$subsc_area_device_tbl' USING org.apache.hive.hcatalog.pig.HCatLoader();
same_area_same_device = FOREACH same_area_same_device_tbl GENERATE
subscriber_id as subscriber_id:long,
enb_id_list as enb_id_list:chararray,
enb_simple_area_agg_score as enb_simple_area_agg_score:double,
enb_area_subscriber_count as enb_area_subscriber_count:int,
manufacturer as make:chararray,
model as model:chararray,
enb_simple_device_agg_score as enb_simple_device_agg_score:double,
enb_device_subscriber_count as enb_device_subscriber_count:int,
load_time as load_time:chararray,
score_dt as score_dt:chararray,
score_hr as score_hr:chararray;

same_area_same_device_filtered_hour = FILTER same_area_same_device BY (score_dt=='$score_dt' and score_hr=='$score_hr');
-- ---------------------------------------------------------------------------------------------------------------
-- 3.Correlation
-- ---------------------------------------------------------------------------------------------------------------


vmas_kpi_same_area_same_device_hour_join = JOIN vmas_kpi_scores_data_filtered_hour BY (subscriber_id) LEFT OUTER, same_area_same_device_filtered_hour BY (subscriber_id);


vmas_kpi_same_area_same_device_join_records = FOREACH vmas_kpi_same_area_same_device_hour_join GENERATE
vmas_kpi_scores_data_filtered_hour::score_date_hr as score_date_hr,
vmas_kpi_scores_data_filtered_hour::subscriber_id as subscriber_id,
vmas_kpi_scores_data_filtered_hour::mdn as mdn,
vmas_kpi_scores_data_filtered_hour::imsi as imsi,
vmas_kpi_scores_data_filtered_hour::imei as imei,
vmas_kpi_scores_data_filtered_hour::make as make,
vmas_kpi_scores_data_filtered_hour::model as model,
vmas_kpi_scores_data_filtered_hour::attach_failure_pct as attach_failure_pct,
vmas_kpi_scores_data_filtered_hour::attach_failure_cnt as attach_failure_cnt,
vmas_kpi_scores_data_filtered_hour::attach_attempts_cnt as attach_attempts_cnt,
vmas_kpi_scores_data_filtered_hour::rrc_setup_failure_pct as rrc_setup_failure_pct,
vmas_kpi_scores_data_filtered_hour::rrc_setup_failure_cnt as rrc_setup_failure_cnt,
vmas_kpi_scores_data_filtered_hour::rrc_setup_attempts_cnt as rrc_setup_attempts_cnt,
vmas_kpi_scores_data_filtered_hour::srf_pct as srf_pct,
vmas_kpi_scores_data_filtered_hour::service_request_failures_cnt as service_request_failures_cnt,
vmas_kpi_scores_data_filtered_hour::service_request_attempts_cnt as service_request_attempts_cnt,
vmas_kpi_scores_data_filtered_hour::pcf_pct as pcf_pct,
vmas_kpi_scores_data_filtered_hour::session_setup_failures_cnt as session_setup_failures_cnt,
vmas_kpi_scores_data_filtered_hour::session_setup_attempts_cnt as session_setup_attempts_cnt,
vmas_kpi_scores_data_filtered_hour::cd_pct as cd_pct,
vmas_kpi_scores_data_filtered_hour::context_drops_cnt as context_drops_cnt,
vmas_kpi_scores_data_filtered_hour::context_events_cnt as context_events_cnt,
vmas_kpi_scores_data_filtered_hour::sip_dropped_calls_pct as sip_dropped_calls_pct,
vmas_kpi_scores_data_filtered_hour::volte_voice_calls_dropped_cnt as volte_voice_calls_dropped_cnt,
vmas_kpi_scores_data_filtered_hour::volte_voice_setup_incomplete_calls_cnt as volte_voice_setup_incomplete_calls_cnt,
vmas_kpi_scores_data_filtered_hour::rrc_radio_drop_pct as rrc_radio_drop_pct,
vmas_kpi_scores_data_filtered_hour::radio_bearer_drops_cnt as radio_bearer_drops_cnt,
vmas_kpi_scores_data_filtered_hour::radio_bearer_setup_attempts_cnt as radio_bearer_setup_attempts_cnt,
vmas_kpi_scores_data_filtered_hour::downlink_throughput_kbps as downlink_throughput_kbps,
vmas_kpi_scores_data_filtered_hour::uplink_throughput_kbps as uplink_throughput_kbps,
vmas_kpi_scores_data_filtered_hour::downlink_data_volume as downlink_data_volume,
vmas_kpi_scores_data_filtered_hour::downlink_active_time_ms as downlink_active_time_ms,
vmas_kpi_scores_data_filtered_hour::uplink_data_volume as uplink_data_volume,
vmas_kpi_scores_data_filtered_hour::uplink_active_time_ms as uplink_active_time_ms,
vmas_kpi_scores_data_filtered_hour::travelling_indicator as travelling_indicator,
vmas_kpi_scores_data_filtered_hour::travelling_indicator_cnt as travelling_indicator_cnt,
vmas_kpi_scores_data_filtered_hour::volume_weighted_uplink_thpt_kbps as volume_weighted_uplink_thpt_kbps,
vmas_kpi_scores_data_filtered_hour::volume_weighted_downlink_thpt_kbps as volume_weighted_downlink_thpt_kbps,
vmas_kpi_scores_data_filtered_hour::attach_failure_pct_weight as attach_failure_pct_weight,
vmas_kpi_scores_data_filtered_hour::rrc_setup_failure_pct_weight as rrc_setup_failure_pct_weight,
vmas_kpi_scores_data_filtered_hour::srf_pct_weight as srf_pct_weight,
vmas_kpi_scores_data_filtered_hour::pcf_pct_weight as pcf_pct_weight,
vmas_kpi_scores_data_filtered_hour::cd_pct_weight as cd_pct_weight,
vmas_kpi_scores_data_filtered_hour::sip_dropped_calls_pct_weight as sip_dropped_calls_pct_weight,
vmas_kpi_scores_data_filtered_hour::rrc_radio_drop_pct_weight as rrc_radio_drop_pct_weight,
vmas_kpi_scores_data_filtered_hour::downlink_throughput_kbps_weight as downlink_throughput_kbps_weight,
vmas_kpi_scores_data_filtered_hour::uplink_throughput_kbps_weight as uplink_throughput_kbps_weight,
vmas_kpi_scores_data_filtered_hour::ues_voice_retainability as ues_voice_retainability,
vmas_kpi_scores_data_filtered_hour::ues_voice_reliability as ues_voice_reliability,
vmas_kpi_scores_data_filtered_hour::ues_data as ues_data,
vmas_kpi_scores_data_filtered_hour::ues_allserv as ues_allserv,
vmas_kpi_scores_data_filtered_hour::preference_ratio as preference_ratio,
same_area_same_device_filtered_hour::enb_simple_area_agg_score as enb_simple_area_agg_score,
same_area_same_device_filtered_hour::enb_area_subscriber_count as enb_area_subscriber_count,
same_area_same_device_filtered_hour::enb_simple_device_agg_score as enb_simple_device_agg_score,
same_area_same_device_filtered_hour::enb_device_subscriber_count as enb_device_subscriber_count,
vmas_kpi_scores_data_filtered_hour::load_time as load_time,
'$score_dt' as score_dt:chararray,
'$score_hr' as score_hr:chararray;


-- ---------------------------------------------------------------------------------------------------------------
--
-- x. Store into hdfs path
--
STORE vmas_kpi_same_area_same_device_join_records INTO '$hdfs_out_path/score_date_druid=$score_dt/score_hr_druid=$score_hr' USING PigStorage('$out_delim');
