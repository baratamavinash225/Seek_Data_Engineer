--
-- This file contains Verizon Business  CONFIDENTIAL code.
--
-- (C) COPYRIGHT 2014 Verizon Business Inc.
-- All rights Reserved Worldwide
-- US Government Users Restricted Rights - Use, duplication or
-- disclosure restricted by GSA ADP Schedule Contract with Verizon
-- Business Inc.
--
-- Script      : stp_monthly_soi_enb_feed.pig
--
-- Author      : Akhilesh Varma
--
-- Usage:
--   pig -Dmapred.job.queue.name=etl-pig -x tez -useHCatalog -f stp_monthly_soi_enb_feed.pig -param source_schema='npi_cem_db' -param daily_feed_table='<daily_feed_table>' -param feed_year_month='201906' -param hdfs_out_path='/data/npicem/stp/RTT/stp_monthly_soi_enb' -param out_delim='|'
--
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

daily_agg_enb_tbl = LOAD '$source_schema.$daily_feed_table' USING org.apache.hive.hcatalog.pig.HCatLoader(); 
daily_agg_enb = FOREACH daily_agg_enb_tbl GENERATE
CONCAT(SUBSTRING(date, 4, 5),'-', SUBSTRING(date, 0, 3)) as date:chararray,
mdn as mdn:chararray,
enb as enb:chararray,
pdn as pdn:chararray,
ratinggroup as ratinggroup:chararray,
usagetype as usagetype:chararray,
frommobilebytes as frommobilebytes:chararray,
tomobilebytes as tomobilebytes:chararray,
totalmobilebytes as totalmobilebytes:chararray,
secondsofuse as secondsofuse:chararray;


-- 2. Filter records that are needed for a month

daily_agg_enb_month = FILTER daily_agg_enb BY (date matches '$feed_year_month*');


-- ----------------------------------------------------------------------------------
--
-- 3. Filter data sets with Voice, Data based on usage type.

daily_agg_enb_month_voice = FILTER daily_agg_enb_month BY (usagetype == 'Voice');

daily_agg_enb_month_data = FILTER daily_agg_enb_month BY (usagetype == 'Data');

-- ---------------------------------------------------------------------------------------------------------------
-- 3.Max sum for ENB

daily_agg_enb_month_voice_grp = FOREACH (GROUP daily_agg_enb_month_voice BY (mdn, usagetype, date))
                        {
							sum_seconds_of_use = MAX(SUM(secondsofuse));
							GENERATE
							group.date AS date,
							group.mdn AS mdn,
							group.usagetype AS usagetype,
							(long)sum_seconds_of_use AS enb;
                        };
						
daily_agg_enb_month_data_grp = FOREACH (GROUP daily_agg_enb_month_data BY (mdn, usagetype, date))
                        {
							sum_total_mobile_bytes = MAX(SUM(totalmobilebytes));
							GENERATE
							group.date AS date,
							group.mdn AS mdn,
							group.usagetype AS usagetype,
							(long)sum_total_mobile_bytes AS enb;
                        };

-- ---------------------------------------------------------------------------------------------------------------

4.Union of the records of voice and data

monthly_agg_max_enb = UNION daily_agg_enb_month_voice_grp daily_agg_enb_month_data_grp; 

-- ---------------------------------------------------------------------------------------------------------------
--
-- x. Store into hdfs path
--
STORE monthly_agg_max_enb INTO '$hdfs_out_path/feed_year_month=$feed_year_month' USING PigStorage('$out_delim');
