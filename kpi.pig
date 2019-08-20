------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
-- Script name: stp_load_rtt_kpi_summary_daily.pig
--
-- Description: This script is used to roll up RTT KPI summary hourly to daily for customer(MDN/IMEI/IMSI)
--
-- Usage: pig -useHCatalog -f stp_load_rtt_kpi_summary_daily.pig -param inputpath='$STPHDFSBASE/RTT/stp_rtt_kpi_summary_hourly/trans_dt=<yyyy-MM-dd>' -param delim='|' -param outputpath='$STPHDFSBASE/RTT/stp_rtt_kpi_summary_daily' -param rtt_date_yyyyMMdd=<yyyy-MM-dd>
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

SET pig.tmpfilecompression false;
SET opt.multiquery false;
SET hive.exec.orc.split.strategy BI;
SET pig.optimizer.rules.disabled 'ColumnMapKeyPrune';

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

--Loading day specific stp_rtt_kpi_summary_hourly data from hdfs

rtt_hrly_tbl = LOAD '$inputpath' USING PigStorage('$delim') AS (
trans_dt_hr:int,
mdn:chararray,
imsi:chararray,
imei:chararray,
make:chararray,
model:chararray,
attach_failure_pct:long,
attach_failure_cnt:long,
attach_attempts_cnt:long,
rrc_setup_failure_pct:long,
rrc_setup_failure_cnt:long,
rrc_setup_attempts_cnt:long,
srf_pct:long,
service_request_failures_cnt:long,
service_request_Attempts_cnt:long,
pcf_pct:long,
session_setup_failures_cnt:long,
session_setup_attempts_cnt:long,
cd_pct:long,
context_drops_cnt:long,
context_events_cnt:long,
sip_dropped_calls_pct:long,
volte_voice_calls_dropped_cnt:long,
volte_voice_setup_incomplete_calls_cnt:long,
rrc_radio_drop_pct:long,
radio_bearer_drops_cnt:long,
radio_bearer_setup_attempts_cnt:long,
downlink_throughput_kbps:double,
uplink_thoughput_kbps:double,
downlink_data_volume:long,
downlink_active_time_ms:long,
uplink_data_volume:long,
uplink_active_time_ms:long,
travelling_indicator_hourly:long,
travelling_indicator_record_cnt:long,
volume_weighted_uplink_thpt_kbps:double,
volume_weighted_downlink_thpt_kbps:double,
load_time:datetime,
subscriber_id:chararray,
callattempts_network:long,
callattempts_wifi:long,
calldropincludingho_network:long,
calldropincludingho_wifi:long,
seer_network:long,
seer_wifi:long,
callswithaleg_network:long,
callswithaleg_wifi:long,
attempts_network:long,
attempts_wifi:long,
failures_network:long,
failures_wifi:long,
secondsofuse_network:long,
secondsofuse_wifi:long,
totalgaplength_network_uplink:long,
totalgaplength_network_downlink:long,
totalgaplength_wifi_uplink:long,
totalgaplength_wifi_downlink:long,
callduration_network_uplink:long,
callduration_network_downlink:long,
callduration_wifi_uplink:long,
callduration_wifi_downlink:long,
mos_network_downlink_num:double,
mos_network_uplink_num:double,
mos_wifi_downlink_num:double,
mos_wifi_uplink_num:double,
mos_network_downlink_den:long,
mos_network_uplink_den:long,
mos_wifi_downlink_den:long,
mos_wifi_uplink_den:long,
mos_network_downlink:double,
mos_network_uplink:double,
mos_wifi_downlink:double,
mos_wifi_uplink:double,
sipcalldroprateincludingho_network:double,
sipcalldroprateincludingho_wifi:double,
seer_kpi_network:double,
seer_kpi_wifi:double,
rtpgapratio_network_uplink:double,
rtpgapratio_network_downlink:double,
rtpgapratio_wifi_uplink:double,
rtpgapratio_wifi_downlink:double
);


--filter  day specific rtt hourly data

--filter_rtt_hrly_tbl = FILTER rtt_hrly_tbl BY trans_dt_hr >= (long)CONCAT((chararray)REPLACE('$rtt_date_yyyyMMdd','-',''),'00') AND trans_dt_hr <= (long)CONCAT((chararray)REPLACE('$rtt_date_yyyyMMdd','-',''),'23');


--weighted avarage calculation

weight_avg_rtt_hrly_tbl = FOREACH rtt_hrly_tbl GENERATE
trans_dt_hr,
mdn,
imsi,
imei,
subscriber_id,
make,
model,
attach_failure_pct,
attach_failure_cnt,
attach_attempts_cnt,
rrc_setup_failure_pct,
rrc_setup_failure_cnt,
rrc_setup_attempts_cnt,
srf_pct,
service_request_failures_cnt,
service_request_Attempts_cnt,
pcf_pct,
session_setup_failures_cnt,
session_setup_attempts_cnt,
cd_pct,
context_drops_cnt,
context_events_cnt,
sip_dropped_calls_pct,
volte_voice_calls_dropped_cnt,
volte_voice_setup_incomplete_calls_cnt,
rrc_radio_drop_pct,
radio_bearer_drops_cnt,
radio_bearer_setup_attempts_cnt,
downlink_throughput_kbps,
uplink_thoughput_kbps,
downlink_data_volume,
downlink_active_time_ms,
uplink_data_volume,
uplink_active_time_ms,
travelling_indicator_hourly*travelling_indicator_record_cnt AS mul_travelling_ind,
travelling_indicator_record_cnt,
volume_weighted_uplink_thpt_kbps*uplink_data_volume AS mul_volume_weighted_uplink_thpt_kbps,
volume_weighted_downlink_thpt_kbps*downlink_data_volume AS mul_volume_weighted_downlink_thpt_kbps,
callattempts_network,
callattempts_wifi,
calldropincludingho_network,
calldropincludingho_wifi,
seer_network,
seer_wifi,
callswithaleg_network,
callswithaleg_wifi,
attempts_network,
attempts_wifi,
failures_network,
failures_wifi,
secondsofuse_network,
secondsofuse_wifi,
totalgaplength_network_uplink,
totalgaplength_network_downlink,
totalgaplength_wifi_uplink,
totalgaplength_wifi_downlink,
callduration_network_uplink,
callduration_network_downlink,
callduration_wifi_uplink,
callduration_wifi_downlink,
mos_network_downlink_num,
mos_network_uplink_num,
mos_wifi_downlink_num,
mos_wifi_uplink_num,
mos_network_downlink_den,
mos_network_uplink_den,
mos_wifi_downlink_den,
mos_wifi_uplink_den,
mos_network_downlink,
mos_network_uplink,
mos_wifi_downlink,
mos_wifi_uplink,
sipcalldroprateincludingho_network,
sipcalldroprateincludingho_wifi,
seer_kpi_network,
seer_kpi_wifi,
rtpgapratio_network_uplink,
rtpgapratio_network_downlink,
rtpgapratio_wifi_uplink,
rtpgapratio_wifi_downlink;


--Rollup hourly KPI count to daily for customer(MDN/IMEI/IMSI)

agg_customer_kpi_cnt = FOREACH (GROUP weight_avg_rtt_hrly_tbl BY (mdn,imei,imsi,subscriber_id))
                        {
                        sum_attach_failure_cnt = SUM(weight_avg_rtt_hrly_tbl.attach_failure_cnt);
                        sum_attach_attempts_cnt = SUM(weight_avg_rtt_hrly_tbl.attach_attempts_cnt);
                        sum_rrc_setup_failure_cnt = SUM(weight_avg_rtt_hrly_tbl.rrc_setup_failure_cnt);
                        sum_rrc_setup_attempts_cnt = SUM(weight_avg_rtt_hrly_tbl.rrc_setup_attempts_cnt);
                        sum_service_request_failures_cnt = SUM(weight_avg_rtt_hrly_tbl.service_request_failures_cnt);
                        sum_service_request_Attempts_cnt = SUM(weight_avg_rtt_hrly_tbl.service_request_Attempts_cnt);
                        sum_session_setup_failures_cnt = SUM(weight_avg_rtt_hrly_tbl.session_setup_failures_cnt);
                        sum_session_setup_attempts_cnt = SUM(weight_avg_rtt_hrly_tbl.session_setup_attempts_cnt);
                        sum_context_drops_cnt = SUM(weight_avg_rtt_hrly_tbl.context_drops_cnt);
                        sum_context_events_cnt = SUM(weight_avg_rtt_hrly_tbl.context_events_cnt);
                        sum_volte_voice_calls_dropped_cnt = SUM(weight_avg_rtt_hrly_tbl.volte_voice_calls_dropped_cnt);
                        sum_volte_voice_setup_incomplete_calls_cnt = SUM(weight_avg_rtt_hrly_tbl.volte_voice_setup_incomplete_calls_cnt);
                        sum_radio_bearer_drops_cnt = SUM(weight_avg_rtt_hrly_tbl.radio_bearer_drops_cnt);
                        sum_radio_bearer_setup_attempts_cnt = SUM(weight_avg_rtt_hrly_tbl.radio_bearer_setup_attempts_cnt);
                        sum_downlink_data_volume = SUM(weight_avg_rtt_hrly_tbl.downlink_data_volume);
                        sum_downlink_active_time_ms = SUM(weight_avg_rtt_hrly_tbl.downlink_active_time_ms);
                        sum_uplink_data_volume= SUM(weight_avg_rtt_hrly_tbl.uplink_data_volume);
                        sum_uplink_active_time_ms = SUM(weight_avg_rtt_hrly_tbl.uplink_active_time_ms);
                        weighted_avg_travelling_indicator = SUM(weight_avg_rtt_hrly_tbl.mul_travelling_ind);
                        sum_travelling_indicator_hourly = SUM(weight_avg_rtt_hrly_tbl.travelling_indicator_record_cnt);
                        weighted_avg_uplink = SUM(weight_avg_rtt_hrly_tbl.mul_volume_weighted_uplink_thpt_kbps);
                        weighted_avg_downlink = SUM(weight_avg_rtt_hrly_tbl.mul_volume_weighted_downlink_thpt_kbps);
                        make = MAX(weight_avg_rtt_hrly_tbl.make);
                        model = MAX(weight_avg_rtt_hrly_tbl.model);
                        sum_callattempts_network = SUM(weight_avg_rtt_hrly_tbl.callattempts_network);
                        sum_callattempts_wifi = SUM(weight_avg_rtt_hrly_tbl.callattempts_wifi);
                        sum_calldropincludingho_network = SUM(weight_avg_rtt_hrly_tbl.calldropincludingho_network);
                        sum_calldropincludingho_wifi = SUM(weight_avg_rtt_hrly_tbl.calldropincludingho_wifi);
                        sum_seer_network = SUM(weight_avg_rtt_hrly_tbl.seer_network);
                        sum_seer_wifi = SUM(weight_avg_rtt_hrly_tbl.seer_wifi);
                        sum_callswithaleg_network = SUM(weight_avg_rtt_hrly_tbl.callswithaleg_network);
                        sum_callswithaleg_wifi = SUM(weight_avg_rtt_hrly_tbl.callswithaleg_wifi);
                        sum_attempts_network = SUM(weight_avg_rtt_hrly_tbl.attempts_network);
                        sum_attempts_wifi = SUM(weight_avg_rtt_hrly_tbl.attempts_wifi);
                        sum_failures_network = SUM(weight_avg_rtt_hrly_tbl.failures_network);
                        sum_failures_wifi = SUM(weight_avg_rtt_hrly_tbl.failures_wifi);
                        sum_secondsofuse_network = SUM(weight_avg_rtt_hrly_tbl.secondsofuse_network);
                        sum_secondsofuse_wifi = SUM(weight_avg_rtt_hrly_tbl.secondsofuse_wifi);
                        sum_totalgaplength_network_uplink = SUM(weight_avg_rtt_hrly_tbl.totalgaplength_network_uplink);
                        sum_totalgaplength_network_downlink = SUM(weight_avg_rtt_hrly_tbl.totalgaplength_network_downlink);
                        sum_totalgaplength_wifi_uplink = SUM(weight_avg_rtt_hrly_tbl.totalgaplength_wifi_uplink);
                        sum_totalgaplength_wifi_downlink = SUM(weight_avg_rtt_hrly_tbl.totalgaplength_wifi_downlink);
                        sum_callduration_network_uplink = SUM(weight_avg_rtt_hrly_tbl.callduration_network_uplink);
                        sum_callduration_network_downlink = SUM(weight_avg_rtt_hrly_tbl.callduration_network_downlink);
                        sum_callduration_wifi_uplink  = SUM(weight_avg_rtt_hrly_tbl.callduration_wifi_uplink );
                        sum_callduration_wifi_downlink = SUM(weight_avg_rtt_hrly_tbl.callduration_wifi_downlink);
                        sum_mos_network_downlink_num = SUM(weight_avg_rtt_hrly_tbl.mos_network_downlink_num);
                        sum_mos_network_uplink_num = SUM(weight_avg_rtt_hrly_tbl.mos_network_uplink_num);
                        sum_mos_wifi_downlink_num = SUM(weight_avg_rtt_hrly_tbl.mos_wifi_downlink_num);
                        sum_mos_wifi_uplink_num = SUM(weight_avg_rtt_hrly_tbl.mos_wifi_uplink_num);
                        sum_mos_network_downlink_den = SUM(weight_avg_rtt_hrly_tbl.mos_network_downlink_den);
                        sum_mos_network_uplink_den = SUM(weight_avg_rtt_hrly_tbl.mos_network_uplink_den);
                        sum_mos_wifi_downlink_den = SUM(weight_avg_rtt_hrly_tbl.mos_wifi_downlink_den);
                        sum_mos_wifi_uplink_den = SUM(weight_avg_rtt_hrly_tbl.mos_wifi_uplink_den);
                        sum_mos_network_downlink = SUM(weight_avg_rtt_hrly_tbl.mos_network_downlink);
                        sum_mos_network_uplink = SUM(weight_avg_rtt_hrly_tbl.mos_network_uplink);
                        sum_mos_wifi_downlink = SUM(weight_avg_rtt_hrly_tbl.mos_wifi_downlink);
                        sum_mos_wifi_uplink = SUM(weight_avg_rtt_hrly_tbl.mos_wifi_uplink);
                        sum_sipcalldroprateincludingho_network = SUM(weight_avg_rtt_hrly_tbl.sipcalldroprateincludingho_network);
                        sum_sipcalldroprateincludingho_wifi = SUM(weight_avg_rtt_hrly_tbl.sipcalldroprateincludingho_wifi);
                        sum_seer_kpi_network = SUM(weight_avg_rtt_hrly_tbl.seer_kpi_network);
                        sum_seer_kpi_wifi = SUM(weight_avg_rtt_hrly_tbl.seer_kpi_wifi);
                        sum_rtpgapratio_network_uplink = SUM(weight_avg_rtt_hrly_tbl.rtpgapratio_network_uplink);
                        sum_rtpgapratio_network_downlink = SUM(weight_avg_rtt_hrly_tbl.rtpgapratio_network_downlink);
                        sum_rtpgapratio_wifi_uplink = SUM(weight_avg_rtt_hrly_tbl.rtpgapratio_wifi_uplink);
                        sum_rtpgapratio_wifi_downlink = SUM(weight_avg_rtt_hrly_tbl.rtpgapratio_wifi_downlink);
                        GENERATE
                        group.mdn AS mdn,
                        group.imei AS imei,
                        group.imsi AS imsi,
                        group.subscriber_id AS subscriber_id,
                        (chararray)make AS make,
                        (chararray)model AS model,
                        (long)sum_attach_failure_cnt AS attach_failure_cnt,
                        (long)sum_attach_attempts_cnt AS attach_attempts_cnt,
                        (long)sum_rrc_setup_failure_cnt AS rrc_setup_failure_cnt,
                        (long)sum_rrc_setup_attempts_cnt AS rrc_setup_attempts_cnt,
                        (long)sum_service_request_failures_cnt AS service_request_failures_cnt,
                        (long)sum_service_request_Attempts_cnt AS service_request_Attempts_cnt,
                        (long)sum_session_setup_failures_cnt AS session_setup_failures_cnt,
                        (long)sum_session_setup_attempts_cnt AS session_setup_attempts_cnt,
                        (long)sum_context_drops_cnt AS context_drops_cnt,
                        (long)sum_context_events_cnt AS context_events_cnt,
                        (long)sum_volte_voice_calls_dropped_cnt AS volte_voice_calls_dropped_cnt,
                        (long)sum_volte_voice_setup_incomplete_calls_cnt AS volte_voice_setup_incomplete_calls_cnt,
                        (long)sum_radio_bearer_drops_cnt AS radio_bearer_drops_cnt,
                        (long)sum_radio_bearer_setup_attempts_cnt AS radio_bearer_setup_attempts_cnt,
                        (long)sum_downlink_data_volume AS downlink_data_volume,
                        (long)sum_downlink_active_time_ms AS downlink_active_time_ms,
                        (long)sum_uplink_data_volume AS uplink_data_volume,
                        (long)sum_uplink_active_time_ms AS uplink_active_time_ms,
                        (long)weighted_avg_travelling_indicator AS weighted_avg_travelling_indicator,
                        (long)sum_travelling_indicator_hourly AS sum_travelling_indicator_hourly,
                        (double)weighted_avg_uplink AS weighted_avg_uplink,
                        (double)weighted_avg_downlink AS weighted_avg_downlink,
                        (long)sum_callattempts_network AS callattempts_network,
                        (long)sum_callattempts_wifi AS callattempts_wifi,
                        (long)sum_calldropincludingho_network AS calldropincludingho_network,
                        (long)sum_calldropincludingho_wifi AS calldropincludingho_wifi,
                        (long)sum_seer_network AS seer_network,
                        (long)sum_seer_wifi AS seer_wifi,
                        (long)sum_callswithaleg_network AS callswithaleg_network,
                        (long)sum_callswithaleg_wifi AS callswithaleg_wifi,
                        (long)sum_attempts_network AS attempts_network,
                        (long)sum_attempts_wifi AS attempts_wifi,
                        (long)sum_failures_network AS failures_network,
                        (long)sum_failures_wifi AS failures_wifi,
                        (long)sum_secondsofuse_network AS secondsofuse_network,
                        (long)sum_secondsofuse_wifi AS secondsofuse_wifi,
                        (long)sum_totalgaplength_network_uplink AS totalgaplength_network_uplink,
                        (long)sum_totalgaplength_network_downlink AS totalgaplength_network_downlink,
                        (long)sum_totalgaplength_wifi_uplink AS totalgaplength_wifi_uplink,
                        (long)sum_totalgaplength_wifi_downlink AS totalgaplength_wifi_downlink,
                        (long)sum_callduration_network_uplink AS callduration_network_uplink,
                        (long)sum_callduration_network_downlink AS callduration_network_downlink,
                        (long)sum_callduration_wifi_uplink  AS callduration_wifi_uplink ,
                        (long)sum_callduration_wifi_downlink AS callduration_wifi_downlink,
                        (double)sum_mos_network_downlink_num AS mos_network_downlink_num,
                        (double)sum_mos_network_uplink_num AS mos_network_uplink_num,
                        (double)sum_mos_wifi_downlink_num AS mos_wifi_downlink_num,
                        (double)sum_mos_wifi_uplink_num AS mos_wifi_uplink_num,
                        (long)sum_mos_network_downlink_den AS mos_network_downlink_den,
                        (long)sum_mos_network_uplink_den AS mos_network_uplink_den,
                        (long)sum_mos_wifi_downlink_den AS mos_wifi_downlink_den,
                        (long)sum_mos_wifi_uplink_den AS mos_wifi_uplink_den,
                        (double)sum_mos_network_downlink AS mos_network_downlink,
                        (double)sum_mos_network_uplink AS mos_network_uplink,
                        (double)sum_mos_wifi_downlink AS mos_wifi_downlink,
                        (double)sum_mos_wifi_uplink AS mos_wifi_uplink,
                        (double)sum_sipcalldroprateincludingho_network AS sipcalldroprateincludingho_network,
                        (double)sum_sipcalldroprateincludingho_wifi AS sipcalldroprateincludingho_wifi,
                        (double)sum_seer_kpi_network AS seer_kpi_network,
                        (double)sum_seer_kpi_wifi AS seer_kpi_wifi,
                        (double)sum_rtpgapratio_network_uplink AS rtpgapratio_network_uplink,
                        (double)sum_rtpgapratio_network_downlink AS rtpgapratio_network_downlink,
                        (double)sum_rtpgapratio_wifi_uplink AS rtpgapratio_wifi_uplink,
                        (double)sum_rtpgapratio_wifi_downlink AS rtpgapratio_wifi_downlink;
                        };
--Deriving percentage ,speeds and travelling_indicator columns

rtt_kpi_summary_daily = FOREACH agg_customer_kpi_cnt GENERATE
REPLACE('$rtt_date_yyyyMMdd','-','') AS trans_date,
mdn,
imsi,
imei,
make,
model,
ROUND_TO((((double)attach_failure_cnt/(double)attach_attempts_cnt)*100),2) AS attach_failure_pct,
attach_failure_cnt,
attach_attempts_cnt,
ROUND_TO((((double)rrc_setup_failure_cnt/(double)rrc_setup_attempts_cnt)*100),2) AS rrc_setup_failure_pct,
rrc_setup_failure_cnt,
rrc_setup_attempts_cnt,
ROUND_TO((((double)service_request_failures_cnt/(double)service_request_Attempts_cnt)*100),2) AS srf_pct,
service_request_failures_cnt,
service_request_Attempts_cnt,
ROUND_TO((((double)session_setup_failures_cnt/(double)session_setup_attempts_cnt)*100),2) AS pcf_pct,
session_setup_failures_cnt,
session_setup_attempts_cnt,
ROUND_TO((((double)context_drops_cnt/(double)context_events_cnt)*100),2) AS cd_pct,
context_drops_cnt,
context_events_cnt,
'' AS sip_dropped_calls_pct,
volte_voice_calls_dropped_cnt,
volte_voice_setup_incomplete_calls_cnt,
ROUND_TO((((double)radio_bearer_drops_cnt/(double)radio_bearer_setup_attempts_cnt)*100),2) AS rrc_radio_drop_pct,
radio_bearer_drops_cnt,
radio_bearer_setup_attempts_cnt,
ROUND_TO(((double)downlink_data_volume*8)/(double)downlink_active_time_ms,2) AS downlink_throughput_kbps,
ROUND_TO(((double)uplink_data_volume*8)/(double)downlink_active_time_ms,2) AS uplink_throughput_kbps,
downlink_data_volume,
downlink_active_time_ms,
uplink_data_volume,
uplink_active_time_ms,
(weighted_avg_travelling_indicator/sum_travelling_indicator_hourly) AS travelling_indicator,
(weighted_avg_uplink/uplink_data_volume) AS volume_weighted_uplink_thpt_kbps,
(weighted_avg_downlink/downlink_data_volume) AS volume_weighted_downlink_thpt_kbps,
ToString(CurrentTime(),'yyyyMMdd HH:mm:ss') AS load_time,
subscriber_id,
callattempts_network,
callattempts_wifi,
calldropincludingho_network,
calldropincludingho_wifi,
seer_network,
seer_wifi,
callswithaleg_network,
callswithaleg_wifi,
attempts_network,
attempts_wifi,
failures_network,
failures_wifi,
secondsofuse_network,
secondsofuse_wifi,
totalgaplength_network_uplink,
totalgaplength_network_downlink,
totalgaplength_wifi_uplink,
totalgaplength_wifi_downlink,
callduration_network_uplink,
callduration_network_downlink,
callduration_wifi_uplink,
callduration_wifi_downlink,
mos_network_downlink_num,
mos_network_uplink_num,
mos_wifi_downlink_num,
mos_wifi_uplink_num,
mos_network_downlink_den,
mos_network_uplink_den,
mos_wifi_downlink_den,
mos_wifi_uplink_den,
(mos_network_downlink_num/mos_network_downlink_den) AS mos_network_downlink ,
(mos_network_uplink_num/mos_network_uplink_den) AS mos_network_uplink,
(mos_wifi_downlink_num/mos_wifi_downlink_den) AS mos_wifi_downlink,
(mos_wifi_uplink_num/mos_wifi_uplink_den) AS mos_wifi_uplink,
sipcalldroprateincludingho_network AS sipcalldroprateincludingho_network,
sipcalldroprateincludingho_wifi AS sipcalldroprateincludingho_wifi,
(seer_network/attempts_network) AS seer_kpi_network ,
(seer_wifi/attempts_wifi) AS seer_kpi_wifi,
(totalgaplength_network_uplink/callduration_network_uplink) AS rtpgapratio_network_uplink,
(totalgaplength_network_downlink/callduration_network_downlink) AS rtpgapratio_network_downlink,
(totalgaplength_wifi_uplink/callduration_wifi_uplink) AS rtpgapratio_wifi_uplink,
(totalgaplength_wifi_downlink/callduration_wifi_downlink) AS rtpgapratio_wifi_downlink;


--Store final dataset to hdfspath
STORE rtt_kpi_summary_daily INTO '$outputpath/trans_dt=$rtt_date_yyyyMMdd' USING PigStorage('|');
