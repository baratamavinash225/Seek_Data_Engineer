/usr/apps/nsp_sandbox/stp/scripts $ cat stp_rtt_load_same_area_device_hourly.pig
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Script name: stp_rtt_load_same_area_device_hourly.pig
--
-- Description: This script is used for calculating the same area and same device scores by correlating hourly the VMAS scores and NSR data
--
-- Usage: pig -useHCatalog -f stp_rtt_load_same_area_device_hourly.pig
--            -param dbschema=alisy3p
--            -param nsr_rtt_hourly_table=STP_RTT_NSR_HOURLY_AGG
--            -param nsr_date_yyyymmddhh='2019032005'
--            -param vmas_rtt_hourly_table=STP_RTT_VMAS_KPI_SCORES_RAW_HOURLY
--            -param scores_date_hr='2019062401'
--            -param stp_rtt_mdn_enb_scores=stp_rtt_mdn_enb_scores_hourly
--            -param stp_rtt_enb_area_scores=stp_rtt_enb_area_scores_hourly
--            -param stp_rtt_enb_device_scores=stp_rtt_enb_device_scores_hourly
--            -param stp_rtt_subscriber_area_device_scores=stp_subscriber_area_device_scores_hourly
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

SET pig.tmpfilecompression false;
SET pig.tez.opt.union false;
SET pig.maxCombinedSplitSize 67108864;
SET mapreduce.map.java.opts -Xmx3072M;
SET mapreduce.map.memory.mb 3584;
SET mapreduce.reduce.java.opts -Xmx3072M;
SET mapreduce.reduce.memory.mb 3584;
SET mapreduce.task.io.sort.mb 1024;
SET mapred.output.compress true;


--Loading NSR_RTT_HOURLY table with appropriate partition
--nsr_rtt_data_hourly =  LOAD '$dbschema.$nsr_rtt_hourly_table'  USING org.apache.hive.hcatalog.pig.HCatLoader() AS (
--  filter_nsr_rtt_data_hourly =  LOAD 'hdfs://ey9hdpnpuat/data/npicem/stp/RTT/stp_rtt_nsr_hourly_agg/date_time_key=2019032008' USING PigStorage('|') AS (
--filter_nsr_rtt_data_hourly =  LOAD '$dbschema.$nsr_rtt_hourly_table/date_time_key==$nsr_date_yyyymmddhh'
filter_nsr_rtt_data_hourly =  LOAD '$stp_rtt_nsr_hourly_agg_path=$nsr_date_yyyymmddhh'
                     USING PigStorage('$stp_rtt_nsr_hourly_agg_delimiter') AS (
                            mdn:chararray
                            ,imei:chararray
                            ,imsi:chararray
                            ,cell_tower:chararray
                            ,uplink_data_volume_bytes:long
                            ,downlink_data_volume_bytes:long
                            ,load_time:chararray
                            ,date_time_key:long
                            );
--filter_nsr_rtt_data_hourly = FILTER nsr_rtt_data_hourly
--                            BY date_time_key==$nsr_date_yyyymmddhh;

--Loading RTT Vmas Scores Raw table with appropriate score date partition
--vmas_scores_data= LOAD '$dbschema.$vmas_rtt_hourly_table' USING org.apache.hive.hcatalog.pig.HCatLoader() AS (
--filter_vmas_scores_data =  LOAD 'hdfs://ey9hdpnpuat/data/npicem/stp/RTT/stp_rtt_vmas_kpi_scores_raw_hourly/score_dt=20190320/score_hr=08'   USING PigStorage('|') AS (
--filter_vmas_scores_data =  LOAD '$dbschema.$vmas_rtt_hourly_table/score_date_hr==ToString(ToDate('$scores_date_hr','yyyyMMddHH'))'
filter_vmas_scores_data =  LOAD '$stp_rtt_vmas_kpi_scores_raw_hourly_path=$dt_score/score_hr=$hr_score'
                     USING PigStorage('$stp_rtt_nsr_hourly_agg_delimiter')  AS (
                             score_date_hr:chararray
                            ,subscriber_id:long
                            ,mdn:chararray
                            ,imsi:chararray
                            ,imei:chararray
                            ,make:chararray
                            ,model:chararray
                            ,attach_failure_pct:double
                            ,attach_failure_cnt:long
                            ,attach_attempts_cnt:long
                            ,rrc_setup_failure_pct:double
                            ,rrc_setup_failure_cnt:long
                            ,rrc_setup_attempts_cnt:long
                            ,srf_pct:double
                            ,service_request_failures_cnt:long
                            ,service_request_attempts_cnt:long
                            ,pcf_pct:double
                            ,session_setup_failures_cnt:long
                            ,session_setup_attempts_cnt:long
                            ,cd_pct:double
                            ,context_drops_cnt:long
                            ,context_events_cnt:long
                            ,sip_dropped_calls_pct:double
                            ,volte_voice_calls_dropped_cnt:long
                            ,volte_voice_setup_incomplete_calls_cnt:long
                            ,rrc_radio_drop_pct:double
                            ,radio_bearer_drops_cnt:long
                            ,radio_bearer_setup_attempts_cnt:long
                            ,downlink_throughput_kbps:double
                            ,uplink_throughput_kbps:double
                            ,downlink_data_volume:long
                            ,downlink_active_time_ms:long
                            ,uplink_data_volume:long
                            ,uplink_active_time_ms:long
                            ,travelling_indicator:long
                            ,travelling_indicator_cnt:long
                            ,volume_weighted_uplink_thpt_kbps:double
                            ,volume_weighted_downlink_thpt_kbps:double
                            ,attach_failure_pct_weight:double
                            ,rrc_setup_failure_pct_weight:double
                            ,srf_pct_weight:double
                            ,pcf_pct_weight:double
                            ,cd_pct_weight:double
                            ,sip_dropped_calls_pct_weight:double
                            ,rrc_radio_drop_pct_weight:double
                            ,downlink_throughput_kbps_weight:double
                            ,uplink_throughput_kbps_weight:double
                            ,ues_voice_retainability:double
                            ,ues_voice_reliability:double
                            ,ues_data:double
                            ,ues_allserv:double
                            ,preference_ratio:double
                            ,load_time:chararray
                            ,score_dt:chararray
                            ,score_hr:chararray
                        );

--filter_vmas_scores_data = FILTER vmas_scores_data
--                            BY (score_date_hr==ToString(ToDate('$scores_date_hr','yyyyMMddHH')));

--Joining NSR and VMAS Scores
join_nsr_vmas_scores = JOIN filter_nsr_rtt_data_hourly BY (mdn),
                           filter_vmas_scores_data BY (mdn);

--Generating only the required columns with formatted EnodeBId
stp_rtt_mdn_enb_scores = FOREACH join_nsr_vmas_scores
                                    GENERATE SUBSTRING(filter_nsr_rtt_data_hourly::cell_tower,6,13) AS enb_id
                                        ,filter_nsr_rtt_data_hourly::mdn AS mdn
                                        ,filter_vmas_scores_data::subscriber_id AS subscriber_id
                                        ,filter_vmas_scores_data::make AS manufacturer
                                        ,filter_vmas_scores_data::model AS model
                                        ,filter_vmas_scores_data::ues_data AS subscriber_enb_agg_score
                                        ,ToString(CurrentTime(),'yyyyMMdd HH:mm:ss') AS load_time;
--                                        ,filter_vmas_scores_data::score_dt AS score_dt
--                                        ,filter_vmas_scores_data::score_hr AS score_hr;


--Deriving scores for enodeb's
group_enb_same_area= GROUP stp_rtt_mdn_enb_scores
                        BY enb_id;

stp_rtt_enb_area_scores= FOREACH group_enb_same_area
                            GENERATE group AS enb_id
                                    ,AVG(stp_rtt_mdn_enb_scores.subscriber_enb_agg_score) AS enb_area_agg_score
                                    ,(int)COUNT(stp_rtt_mdn_enb_scores.mdn) AS enb_area_subscriber_count
                                    ,MIN(stp_rtt_mdn_enb_scores.subscriber_enb_agg_score) AS min_enb_area_agg_score
                                    ,MAX(stp_rtt_mdn_enb_scores.subscriber_enb_agg_score) AS max_enb_area_agg_score
                                    ,ToString(CurrentTime(),'yyyyMMdd HH:mm:ss')  AS load_time;
--                                    ,stp_rtt_mdn_enb_scores::score_dt AS score_dt
--                                    ,stp_rtt_mdn_enb_scores::score_hr AS score_hr;


--Deriving Subscrber level scores from EnodeB level scores
join_enb_mdn_area_scores = JOIN stp_rtt_mdn_enb_scores BY enb_id,
                                stp_rtt_enb_area_scores BY enb_id;

stp_subs_enb_agg_scores = FOREACH join_enb_mdn_area_scores
                                    GENERATE stp_rtt_mdn_enb_scores::subscriber_id AS subscriber_id
                                    ,stp_rtt_mdn_enb_scores::enb_id AS enb_id
                                    ,stp_rtt_enb_area_scores::enb_area_agg_score AS enb_area_agg_score
                                    ,stp_rtt_enb_area_scores::enb_area_subscriber_count AS enb_area_subscriber_count;

group_at_subs_area= GROUP stp_subs_enb_agg_scores
                       BY subscriber_id;

stp_rtt_enb_area_agg_scores = FOREACH group_at_subs_area
                                { dist_enb_id = DISTINCT stp_subs_enb_agg_scores.enb_id;
                                        GENERATE group AS subscriber_id
                                                 ,BagToString(dist_enb_id, ',')                            AS enb_id_list
                                        ,AVG(stp_subs_enb_agg_scores.enb_area_agg_score) AS enb_simple_area_agg_score
                                        ,SUM(stp_subs_enb_agg_scores.enb_area_subscriber_count) AS enb_area_subscriber_count;
                                                    };


-- Deriving Device level scores
group_enb_same_device= GROUP stp_rtt_mdn_enb_scores
                         BY (enb_id,manufacturer,model);

stp_rtt_enb_device_scores= FOREACH group_enb_same_device
                            GENERATE group.enb_id AS enb_id
                                    ,group.manufacturer AS manufacturer
                                    ,group.model AS model
                                    ,AVG(stp_rtt_mdn_enb_scores.subscriber_enb_agg_score) AS enb_device_agg_score
                                    ,(int)COUNT(stp_rtt_mdn_enb_scores.mdn) AS enb_device_subscriber_count
                                    ,MIN(stp_rtt_mdn_enb_scores.subscriber_enb_agg_score) AS min_enb_device_agg_score
                                    ,MAX(stp_rtt_mdn_enb_scores.subscriber_enb_agg_score) AS max_enb_device_agg_score
                                    ,ToString(CurrentTime(),'yyyyMMdd HH:mm:ss') AS load_time;


join_enb_mdn_device_scores = JOIN stp_rtt_mdn_enb_scores BY (enb_id,manufacturer,model),
                                  stp_rtt_enb_device_scores BY (enb_id,manufacturer,model);

stp_subs_enb_device_agg_scores = FOREACH join_enb_mdn_device_scores
                                    GENERATE stp_rtt_mdn_enb_scores::subscriber_id AS subscriber_id
                                            ,stp_rtt_mdn_enb_scores::enb_id AS enb_id
                                            ,stp_rtt_enb_device_scores::manufacturer AS manufacturer
                                            ,stp_rtt_enb_device_scores::model AS model
                                            ,stp_rtt_enb_device_scores::enb_device_agg_score AS enb_device_agg_score
                                            ,stp_rtt_enb_device_scores::enb_device_subscriber_count AS enb_device_subscriber_count;

--Deriving Subsriber level Device scores from Enodeb Device level scores
group_at_subs_device= GROUP stp_subs_enb_device_agg_scores
                         BY subscriber_id;

stp_rtt_enb_device_agg_scores = FOREACH group_at_subs_device
                                 GENERATE group AS subscriber_id
                                         ,MAX(stp_subs_enb_device_agg_scores.manufacturer)                  AS manufacturer
                                         ,MAX(stp_subs_enb_device_agg_scores.model)                         AS model
                                         ,AVG(stp_subs_enb_device_agg_scores.enb_device_agg_score) AS enb_simple_device_agg_score
                                         ,SUM(stp_subs_enb_device_agg_scores.enb_device_subscriber_count)   AS enb_device_subscriber_count;


--Joining both Device and Area level scores for each subscriber
join_subs_area_device = JOIN stp_rtt_enb_area_agg_scores BY(subscriber_id),
                             stp_rtt_enb_device_agg_scores BY(subscriber_id);

stp_rtt_subscriber_area_device_scores = FOREACH join_subs_area_device
                                            GENERATE stp_rtt_enb_area_agg_scores::subscriber_id AS subscriber_id
                                                    ,stp_rtt_enb_area_agg_scores::enb_id_list AS enb_id_list
                                                    ,stp_rtt_enb_area_agg_scores::enb_simple_area_agg_score AS enb_simple_area_agg_score
                                                    ,(int)stp_rtt_enb_area_agg_scores::enb_area_subscriber_count AS enb_area_subscriber_count
                                                    ,stp_rtt_enb_device_agg_scores::manufacturer AS manufacturer
                                                    ,stp_rtt_enb_device_agg_scores::model AS model
                                                    ,stp_rtt_enb_device_agg_scores::enb_simple_device_agg_score AS enb_simple_device_agg_score
                                                    ,(int)stp_rtt_enb_device_agg_scores::enb_device_subscriber_count   AS enb_device_subscriber_count
                                                    ,ToString(CurrentTime(),'yyyyMMdd HH:mm:ss')                       AS load_time;

--Remove the same score_date partition if available already
--rmf hdfs://ey9hdpnpuat/data/npicem/stp/RTT/stp_rtt_mdn_enb_scores_hourly/score_dt=20190320/score_hr=08
--rmf $STPHDFSBASE/RTT/stp_rtt_mdn_enb_scores_hourly/score_dt=20190320/score_hr=08
rmf $stp_rtt_hourly_mdn_enb_scores_path=$dt_score/score_hr=$hr_score
rmf $stp_rtt_hourly_enb_area_scores_path=$dt_score/score_hr=$hr_score
rmf $stp_rtt_hourly_enb_device_scores_path=$dt_score/score_hr=$hr_score
rmf $stp_rtt_hourly_subscriber_area_device_scores_path=$dt_score/score_hr=$hr_score


--Storing the respective relation into Hive Tables
STORE  stp_rtt_mdn_enb_scores
INTO   '$dbschema.$stp_rtt_mdn_enb_scores'
USING  org.apache.hive.hcatalog.pig.HCatStorer('score_dt=$dt_score,score_hr=$hr_score');

STORE  stp_rtt_enb_area_scores
INTO   '$dbschema.$stp_rtt_enb_area_scores'
USING  org.apache.hive.hcatalog.pig.HCatStorer('score_dt=$dt_score,score_hr=$hr_score');

STORE  stp_rtt_enb_device_scores
INTO   '$dbschema.$stp_rtt_enb_device_scores'
USING  org.apache.hive.hcatalog.pig.HCatStorer('score_dt=$dt_score,score_hr=$hr_score');

STORE  stp_rtt_subscriber_area_device_scores
INTO   '$dbschema.$stp_rtt_subscriber_area_device_scores'
USING  org.apache.hive.hcatalog.pig.HCatStorer('score_dt=$dt_score,score_hr=$hr_score');
