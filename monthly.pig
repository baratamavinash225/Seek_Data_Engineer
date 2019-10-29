--
-- This file contains Verizon Business  CONFIDENTIAL code.
--
-- (C) COPYRIGHT 2014 Verizon Business Inc.
-- All rights Reserved Worldwide
-- US Government Users Restricted Rights - Use, duplication or
-- disclosure restricted by GSA ADP Schedule Contract with Verizon
-- Business Inc.
--
-- Script      : stp_lte_sdr_agg_monthly.pig
--
-- Author      : Akhilesh Varma
--
-- Usage:
--   pig -Dmapred.job.queue.name=etl-pig -x tez -useHCatalog -f stp_monthly_soi_enb_feed.pig -param source_schema='npi_cem_db' -param source_table='<daily_feed_table>' -param trans_mnth='2019-06' -param first_trans_dt='2019-08-01' -param last_trans_dt='2019-08-31' -param hdfs_out_path='/data/npicem/stp/RTT/stp_monthly_soi_enb' -param out_delim='|'

-- ----------------------------------------------------------------------------------
-- THE STORY --
-- ----------------------------------------------------------------------------------
--
SET pig.tmpfilecompression false;
SET opt.multiquery false;
SET hive.exec.orc.split.strategy BI;

--SET mapreduce.map.java.opts -Xmx3072M;
SET mapreduce.map.java.opts -Xmx6144M;
SET mapreduce.task.io.sort.mb 1024;
SET pig.maxCombinedSplitSize 67108864;
--SET mapreduce.map.memory.mb 4096;
SET mapreduce.map.memory.mb 8192;
SET mapreduce.reduce.java.opts -Xmx6144M;
SET mapreduce.reduce.memory.mb 8192;
SET mapred.output.compress true;
SET mapreduce.job.reduce.slowstart.completedmaps 0.90;

SET ipc.maximum.data.length 268435456;
SET tez.am.container.idle.release-timeout-min.millis 5000;
SET tez.am.container.idle.release-timeout-max.millis 10000;
SET tez.am.resource.memory.mb 8192;
SET tez.task.resource.memory.mb 8192;
SET tez.runtime.io.sort.mb 1024;
SET tez.runtime.unordered.output.buffer.size-mb 2048;
SET tez.grouping.min-size 16777216;

-- THE STORY --

-- ----------------------------------------------------------------------------------
--
-- 1. Load the Daily table feed snapshot

daily_agg_enb_tbl = LOAD '$source_schema.$source_table' USING org.apache.hive.hcatalog.pig.HCatLoader();

-- 2. FILTER records needed for a monthly

daily_agg_enb = FILTER daily_agg_enb_tbl BY (trans_dt >='$first_trans_dt' and trans_dt <= '$last_trans_dt');

daily_agg_enb_month = FOREACH daily_agg_enb GENERATE
-- CONCAT(SUBSTRING(trans_dt, 2, 4),'-', SUBSTRING(trans_dt, 4, 8)) as trans_mnth:chararray,
'$trans_mnth' as trans_mnth:chararray,
mdn as mdn:chararray,
enb as enb:chararray,
usagetype as usagetype:chararray,
(double)totalmobilebytes as totalmobilebytes:double,
(double)secondsofuse as secondsofuse:double;

-- 3. Filter data sets with Voice, Data based on usage type.

daily_agg_enb_month_voice_tbl = FILTER daily_agg_enb_month BY (usagetype == 'voice');

daily_agg_enb_month_voice = FOREACH daily_agg_enb_month_voice_tbl GENERATE
trans_mnth,
mdn,
enb,
usagetype,
totalmobilebytes,
secondsofuse;

daily_agg_enb_month_data_tbl = FILTER daily_agg_enb_month BY (usagetype == 'data');

daily_agg_enb_month_data = FOREACH daily_agg_enb_month_data_tbl GENERATE
trans_mnth,
mdn,
enb,
usagetype,
totalmobilebytes,
secondsofuse;

-- ---------------------------------------------------------------------------------------------------------------
-- 3.Max sum for ENB

daily_agg_enb_month_voice_grp = FOREACH (GROUP daily_agg_enb_month_voice BY (trans_mnth, mdn, enb, usagetype))
                        {
                                                        sum_seconds_of_use = SUM(daily_agg_enb_month_voice.secondsofuse);
                                                        GENERATE
                                                        group.trans_mnth AS trans_mnth,
                                                        group.mdn AS mdn,
                                                        group.enb AS enb,
                                                        group.usagetype AS usagetype,
                                                        sum_seconds_of_use AS usage;
                        };
						

-- daily_agg_enb_month_voice_max = FOREACH (GROUP daily_agg_enb_month_voice_grp BY (trans_mnth, mdn, enb, usagetype))
--                                                  {
--                                                           ordered_data = ORDER daily_agg_enb_month_voice_grp BY mdn, enb, usage DESC;
--                                                           max_record = LIMIT ordered_data 1;
--                                                           GENERATE FLATTEN(max_record);
--                                                  }

daily_agg_enb_month_voice_sum_grp= GROUP daily_agg_enb_month_voice_grp
                         BY (trans_mnth, mdn, usagetype);

daily_agg_enb_month_voice_max = FOREACH daily_agg_enb_month_voice_sum_grp
                            GENERATE group.mdn AS mdn
                                    ,group.usagetype AS usagetype
									,group.trans_mnth AS trans_mnth
                                    ,daily_agg_enb_month_voice_grp.enb AS enb
                                    ,MAX(daily_agg_enb_month_voice_grp.usage) AS usage;


												

daily_agg_enb_month_voice_max_records = FOREACH daily_agg_enb_month_voice_max GENERATE
mdn as mdn:chararray,
usagetype as usagetype:chararray,
enb as enb:chararray,
trans_mnth as trans_mnth:chararray;

daily_agg_enb_month_data_grp = FOREACH (GROUP daily_agg_enb_month_data BY (trans_mnth, mdn, enb, usagetype))
                        {
                                                        sum_total_mobile_bytes = SUM(daily_agg_enb_month_data.totalmobilebytes);
                                                        GENERATE
                                                        group.trans_mnth AS trans_mnth,
                                                        group.mdn AS mdn,
                                                        group.enb AS enb,
                                                        group.usagetype AS usagetype,
                                                        sum_total_mobile_bytes AS usage;
                        };



-- daily_agg_enb_month_data_max = FOREACH (GROUP daily_agg_enb_month_data_grp BY (trans_mnth, mdn,enb, usagetype))
--                                                 {
--                                                          ordered_data = ORDER daily_agg_enb_month_data_grp BY mdn, enb,usage DESC;
--                                                          max_record_data = LIMIT ordered_data 1;
--                                                          GENERATE FLATTEN(max_record_data);
--                                                 }
--

												 

												 
daily_agg_enb_month_data_sum_grp= GROUP daily_agg_enb_month_data_grp
                         BY (trans_mnth, mdn, usagetype);

daily_agg_enb_month_data_max = FOREACH daily_agg_enb_month_data_sum_grp
                            GENERATE group.mdn AS mdn
                                    ,group.usagetype AS usagetype
									,group.trans_mnth AS trans_mnth
                                    ,daily_agg_enb_month_data_grp.enb AS enb
                                    ,MAX(daily_agg_enb_month_data_grp.usage) AS usage;


												

daily_agg_enb_month_data_max_records = FOREACH daily_agg_enb_month_data_max GENERATE
mdn as mdn:chararray,
usagetype as usagetype:chararray,
enb as enb:chararray,
trans_mnth as trans_mnth:chararray;

-- ---------------------------------------------------------------------------------------------------------------

-- 4.Union of the records of voice and data

monthly_agg_max_enb = UNION daily_agg_enb_month_voice_max_records, daily_agg_enb_month_data_max_records;

-- ---------------------------------------------------------------------------------------------------------------
--
-- x. Store into hdfs path
--
STORE monthly_agg_max_enb INTO '$hdfs_out_path/trans_mnth=$trans_mnth' USING PigStorage('$out_delim');
