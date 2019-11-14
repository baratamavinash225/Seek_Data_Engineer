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
-- Script      : stp_load_customer_profile.pig
--
-- Author      : Krishna Harish Katuru (Harry)
--
-- Description : This script is used to load the customer profile table
--
-- Usage:
--   pig -useHCatalog -f stp_load_customer_profile.pig -param sourcefile=<path-to-input-file> -param destschema=$HIVEDB -param custprofiletbl=STP_CUSTOMER_PROFILE -param filename=<filename> -param delim=, -param filedate=<YYYYMMDD-hhmmss>
--
-- ----------------------------------------------------------------------------------

-- PROLOGUE --

SET pig.tmpfilecompression false;
SET opt.multiquery false;
SET hive.exec.orc.split.strategy BI

SET mapreduce.map.java.opts -Xmx3072M
SET mapreduce.task.io.sort.mb 1024
SET pig.maxCombinedSplitSize 67108864
SET mapreduce.map.memory.mb 4096
SET mapreduce.reduce.java.opts -Xmx6144M
SET mapreduce.reduce.memory.mb 8192
SET mapred.output.compress true
SET mapreduce.job.reduce.slowstart.completedmaps 0.90

SET ipc.maximum.data.length 268435456

SET pig.tez.opt.union false
SET tez.am.container.idle.release-timeout-min.millis 5000
SET tez.am.container.idle.release-timeout-max.millis 10000
SET tez.am.resource.memory.mb 8192
SET tez.task.resource.memory.mb 8192
SET tez.runtime.io.sort.mb 1024
SET tez.runtime.unordered.output.buffer.size-mb 2048
SET tez.grouping.min-size 16777216
SET tez.grouping.max-size 1073741824

-- THE STORY --

-- ----------------------------------------------------------------------------------
--
-- 1. Load all the files in the source directory
--

cust_prof_input_raw = LOAD '$sourcefilename' USING org.apache.pig.piggybank.storage.CSVExcelStorage('$delim', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (
cust_id:chararray,
cust_line_seq_id:chararray,
acct_num:chararray,
mdn:chararray,
emin:chararray,
nm_first:chararray,
nm_last:chararray,
esn_num:chararray,
cust_type:chararray,
vzw_imsi:chararray,
vd_imsi:chararray,
c_prod_nm:chararray,
eqp_device_id:chararray,
line_act_dt:chararray,
last_upd_dt:chararray,
mtn_status_ind:chararray,
addr_line1:chararray,
addr_line2:chararray,
addr_line3:chararray,
addr_line4:chararray,
city_nm:chararray,
state_cd:chararray,
zip5_cd:chararray,
zip4_cd:chararray,
manufacturer:chararray,
model:chararray,
sor_product_family:chararray,
data_tier:chararray,
device_grouping:chararray
);

cust_prof_input_filtered = FILTER cust_prof_input_raw BY (cust_id != '' AND mdn != '' AND emin != '' AND cust_id IS NOT NULL AND mdn IS NOT NULL AND emin IS NOT NULL);
--DUMP cust_prof_input_raw;

-- ----------------------------------------------------------------------------------
--
-- 2. Load the target table for an eventual full outer join
--

cust_prof_transformed = FOREACH cust_prof_input_filtered GENERATE
TRIM(REPLACE(cust_id,'"',''))                                                     AS cust_id:chararray,
TRIM(REPLACE(cust_line_seq_id,'"',''))                                            AS cust_line_seq_id:chararray,
TRIM(REPLACE(acct_num,'"',''))                                                    AS acct_num:chararray,
TRIM(REPLACE(mdn,'"',''))                                                         AS mdn:chararray,
TRIM(REPLACE(emin,'"',''))                                                        AS emin:chararray,
TRIM(REPLACE(REPLACE(nm_first,'"',''),'\\|',''))                                  AS nm_first:chararray,
TRIM(REPLACE(REPLACE(nm_last,'"',''),'\\|',''))                                   AS nm_last:chararray,
TRIM(REPLACE(esn_num,'"',''))                                                     AS esn_num:chararray,
TRIM(REPLACE(cust_type,'"',''))                                                   AS cust_type:chararray,
TRIM(REPLACE(vzw_imsi,'"',''))                                                    AS vzw_imsi:chararray,
TRIM(REPLACE(vd_imsi,'"',''))                                                     AS vd_imsi:chararray,
TRIM(REPLACE(c_prod_nm,'"',''))                                                   AS c_prod_nm:chararray,
TRIM(REPLACE(eqp_device_id,'"',''))                                               AS eqp_device_id:chararray,
TRIM(REPLACE(line_act_dt,'"',''))                                                 AS line_act_dt:chararray,
TRIM(REPLACE(last_upd_dt,'"',''))                                                 AS last_upd_dt:chararray,
TRIM(REPLACE(mtn_status_ind,'"',''))                                              AS mtn_status_ind:chararray,
TRIM(REPLACE(REPLACE(addr_line1,'"',''),'\\|',''))                                AS addr_line1:chararray,
TRIM(REPLACE(REPLACE(addr_line2,'"',''),'\\|',''))                                AS addr_line2:chararray,
TRIM(REPLACE(REPLACE(addr_line3,'"',''),'\\|',''))                                AS addr_line3:chararray,
TRIM(REPLACE(REPLACE(addr_line4,'"',''),'\\|',''))                                AS addr_line4:chararray,
UPPER(TRIM(REPLACE(REPLACE(city_nm,'"',''),'\\|','')))                            AS city_nm:chararray,
UPPER(TRIM(REPLACE(REPLACE(state_cd,'"',''),'\\|','')))                           AS state_cd:chararray,
UPPER(TRIM(REPLACE(REPLACE(zip5_cd,'"',''),'\\|','')))                            AS zip5_cd:chararray,
UPPER(TRIM(REPLACE(REPLACE(zip4_cd,'"',''),'\\|','')))                            AS zip4_cd:chararray,
CONCAT( ((addr_line1 == '') ? '' : REPLACE(REPLACE(addr_line1,'"',''),'\\|',''))
       ,((addr_line1 == '') ? '' : ', ')
       ,((addr_line2 == '') ? '' : REPLACE(REPLACE(addr_line2,'"',''),'\\|',''))
       ,((addr_line2 == '') ? '' : ', ')
       ,((addr_line3 == '') ? '' : REPLACE(REPLACE(addr_line3,'"',''),'\\|',''))
       ,((addr_line3 == '') ? '' : ', ')
       ,((addr_line4 == '') ? '' : REPLACE(REPLACE(addr_line4,'"',''),'\\|',''))
       ,((addr_line4 == '') ? '' : ', ')
       ,((city_nm == '') ? '' : REPLACE(REPLACE(city_nm,'"',''),'\\|',''))
       ,((city_nm == '') ? '' : ', ')
       ,((state_cd == '') ? '' : REPLACE(REPLACE(state_cd,'"',''),'\\|',''))
       ,((state_cd == '') ? '' : '  ')
       ,((zip5_cd == '') ? '' : REPLACE(REPLACE(zip5_cd,'"',''),'\\|',''))
       ,((zip5_cd == '') ? '' : '-')
       ,((zip4_cd == '') ? '' : REPLACE(REPLACE(zip4_cd,'"',''),'\\|',''))
      )                                                                           AS billing_address:chararray,
UPPER(TRIM(REPLACE(manufacturer,'"','')))                                         AS manufacturer:chararray,
UPPER(TRIM(REPLACE(model,'"','')))                                                AS model:chararray,
TRIM(REPLACE(sor_product_family,'"',''))                                          AS sor_product_family:chararray,
TRIM(REPLACE(data_tier,'"',''))                                                   AS data_tier:chararray,
TRIM(REPLACE(device_grouping,'"',''))                                             AS device_grouping:chararray,
CurrentTime()                                                                     AS createtime:datetime,
CurrentTime()                                                                     AS loadtime:datetime,
$filename                                                                         AS filename:chararray,
ToDate((chararray)$filedate,'yyyyMMdd-HHmmss')                                    AS filedate:datetime;

--ToDate(STRSPLIT(STRSPLIT((chararray)$filename,'\\.',2).$0,'-',4).$3,'yyyyMMdd-HHmmss') AS filedate:datetime;

--DUMP cust_prof_transformed;

--cust_prof_grouped = GROUP cust_prof_transformed BY (cust_id,mdn,emin);
cust_prof_grouped = GROUP cust_prof_transformed BY (emin);

cust_prof_dedup = FOREACH cust_prof_grouped {
  cust_prof_sorted = ORDER cust_prof_transformed BY filedate DESC;
  latest = LIMIT cust_prof_sorted 1;
  GENERATE FLATTEN(latest);
};

-- ----------------------------------------------------------------------------------
--
-- 3. Load the target table for an eventual full outer join
--

cust_prof_snapshot = LOAD '$destschema.$custprofiletbl' USING org.apache.hive.hcatalog.pig.HCatLoader() AS (
cust_id:chararray,
cust_line_seq_id:chararray,
acct_num:chararray,
mdn:chararray,
emin:chararray,
nm_first:chararray,
nm_last:chararray,
esn_num:chararray,
cust_type:chararray,
vzw_imsi:chararray,
vd_imsi:chararray,
c_prod_nm:chararray,
eqp_device_id:chararray,
line_act_dt:chararray,
addr_line1:chararray,
addr_line2:chararray,
addr_line3:chararray,
addr_line4:chararray,
city_nm:chararray,
state_cd:chararray,
zip5_cd:chararray,
zip4_cd:chararray,
billing_address:chararray,
manufacturer:chararray,
model:chararray,
sor_product_family:chararray,
data_tier:chararray,
device_grouping:chararray,
createtime:datetime,
loadtime:datetime,
filename:chararray,
filedate:datetime,
last_upd_dt:chararray,
mtn_status_ind:chararray,
partitiondate:chararray
);

-- ----------------------------------------------------------------------------------
--
-- 4. Join the current topped data with target table using a full outer
--

cust_prof_join_with_snapshot = JOIN cust_prof_dedup by (cust_id,mdn,emin) FULL OUTER, cust_prof_snapshot by (cust_id,mdn,emin);

-- ----------------------------------------------------------------------------------
--
-- 5. On the joined data for sets A and B, do the following:
--      a. For A-B and A=B i.e., new and/or unmatched items on left table, and matched items on the right table, have the new values.
--      b. For B-A i.e., unmatched items on the right table, populate as is from right.
--      where A is the input/incoming data set and B is the current snapshot on Hive.
--

cust_prof_new_snapshot = FOREACH cust_prof_join_with_snapshot GENERATE
((cust_prof_dedup::latest::cust_id is NULL)            ? cust_prof_snapshot::cust_id            : cust_prof_dedup::latest::cust_id)            AS cust_id:chararray,
((cust_prof_dedup::latest::cust_line_seq_id is NULL)   ? cust_prof_snapshot::cust_line_seq_id   : cust_prof_dedup::latest::cust_line_seq_id)   AS cust_line_seq_id:chararray,
((cust_prof_dedup::latest::acct_num is NULL)           ? cust_prof_snapshot::acct_num           : cust_prof_dedup::latest::acct_num)           AS acct_num:chararray,
((cust_prof_dedup::latest::mdn is NULL)                ? cust_prof_snapshot::mdn                : cust_prof_dedup::latest::mdn)                AS mdn:chararray,
((cust_prof_dedup::latest::emin is NULL)               ? cust_prof_snapshot::emin               : cust_prof_dedup::latest::emin)               AS emin:chararray,
((cust_prof_dedup::latest::nm_first is NULL)           ? cust_prof_snapshot::nm_first           : cust_prof_dedup::latest::nm_first)           AS nm_first:chararray,
((cust_prof_dedup::latest::nm_last is NULL)            ? cust_prof_snapshot::nm_last            : cust_prof_dedup::latest::nm_last)            AS nm_last:chararray,
((cust_prof_dedup::latest::esn_num is NULL)            ? cust_prof_snapshot::esn_num            : cust_prof_dedup::latest::esn_num)            AS esn_num:chararray,
((cust_prof_dedup::latest::cust_type is NULL)          ? cust_prof_snapshot::cust_type          : cust_prof_dedup::latest::cust_type)          AS cust_type:chararray,
((cust_prof_dedup::latest::vzw_imsi is NULL)           ? cust_prof_snapshot::vzw_imsi           : cust_prof_dedup::latest::vzw_imsi)           AS vzw_imsi:chararray,
((cust_prof_dedup::latest::vd_imsi is NULL)            ? cust_prof_snapshot::vd_imsi            : cust_prof_dedup::latest::vd_imsi)            AS vd_imsi:chararray,
((cust_prof_dedup::latest::c_prod_nm is NULL)          ? cust_prof_snapshot::c_prod_nm          : cust_prof_dedup::latest::c_prod_nm)          AS c_prod_nm:chararray,
((cust_prof_dedup::latest::eqp_device_id is NULL)      ? cust_prof_snapshot::eqp_device_id      : cust_prof_dedup::latest::eqp_device_id)      AS eqp_device_id:chararray,
((cust_prof_dedup::latest::line_act_dt is NULL)        ? cust_prof_snapshot::line_act_dt        : cust_prof_dedup::latest::line_act_dt)        AS line_act_dt:chararray,
((cust_prof_dedup::latest::addr_line1 is NULL)         ? cust_prof_snapshot::addr_line1         : cust_prof_dedup::latest::addr_line1)         AS addr_line1:chararray,
((cust_prof_dedup::latest::addr_line2 is NULL)         ? cust_prof_snapshot::addr_line2         : cust_prof_dedup::latest::addr_line2)         AS addr_line2:chararray,
((cust_prof_dedup::latest::addr_line3 is NULL)         ? cust_prof_snapshot::addr_line3         : cust_prof_dedup::latest::addr_line3)         AS addr_line3:chararray,
((cust_prof_dedup::latest::addr_line4 is NULL)         ? cust_prof_snapshot::addr_line4         : cust_prof_dedup::latest::addr_line4)         AS addr_line4:chararray,
((cust_prof_dedup::latest::city_nm is NULL)            ? cust_prof_snapshot::city_nm            : cust_prof_dedup::latest::city_nm)            AS city_nm:chararray,
((cust_prof_dedup::latest::state_cd is NULL)           ? cust_prof_snapshot::state_cd           : cust_prof_dedup::latest::state_cd)           AS state_cd:chararray,
((cust_prof_dedup::latest::zip5_cd is NULL)            ? cust_prof_snapshot::zip5_cd            : cust_prof_dedup::latest::zip5_cd)            AS zip5_cd:chararray,
((cust_prof_dedup::latest::zip4_cd is NULL)            ? cust_prof_snapshot::zip4_cd            : cust_prof_dedup::latest::zip4_cd)            AS zip4_cd:chararray,
((cust_prof_dedup::latest::billing_address is NULL)    ? cust_prof_snapshot::billing_address    : cust_prof_dedup::latest::billing_address)    AS billing_address:chararray,
((cust_prof_dedup::latest::manufacturer is NULL)       ? cust_prof_snapshot::manufacturer       : cust_prof_dedup::latest::manufacturer)       AS manufacturer:chararray,
((cust_prof_dedup::latest::model is NULL)              ? cust_prof_snapshot::model              : cust_prof_dedup::latest::model)              AS model:chararray,
((cust_prof_dedup::latest::sor_product_family is NULL) ? cust_prof_snapshot::sor_product_family : cust_prof_dedup::latest::sor_product_family) AS sor_product_family:chararray,
((cust_prof_dedup::latest::data_tier is NULL)          ? cust_prof_snapshot::data_tier          : cust_prof_dedup::latest::data_tier)          AS data_tier:chararray,
((cust_prof_dedup::latest::device_grouping is NULL)    ? cust_prof_snapshot::device_grouping    : cust_prof_dedup::latest::device_grouping)    AS device_grouping:chararray,
((cust_prof_dedup::latest::createtime is NULL)         ? cust_prof_snapshot::createtime         : cust_prof_dedup::latest::createtime)         AS createtime:datetime,
((cust_prof_dedup::latest::loadtime is NULL)           ? cust_prof_snapshot::loadtime           : cust_prof_dedup::latest::loadtime)           AS loadtime:datetime,
((cust_prof_dedup::latest::filename is NULL)           ? cust_prof_snapshot::filename           : cust_prof_dedup::latest::filename)           AS filename:chararray,
((cust_prof_dedup::latest::filedate is NULL)           ? cust_prof_snapshot::filedate           : cust_prof_dedup::latest::filedate)           AS filedate:datetime,
CASE WHEN (cust_prof_dedup::latest::last_upd_dt is NULL and cust_prof_snapshot::last_upd_dt is NULL)       THEN ''
     WHEN (cust_prof_dedup::latest::last_upd_dt is NULL and NOT (cust_prof_snapshot::last_upd_dt is NULL)) THEN cust_prof_snapshot::last_upd_dt
     ELSE                                                                                                             cust_prof_dedup::latest::last_upd_dt
     END                                                                                                                                       AS last_upd_dt:chararray,
CASE WHEN (cust_prof_dedup::latest::mtn_status_ind is NULL and cust_prof_snapshot::mtn_status_ind is NULL)       THEN ''
     WHEN (cust_prof_dedup::latest::mtn_status_ind is NULL and NOT (cust_prof_snapshot::mtn_status_ind is NULL)) THEN cust_prof_snapshot::mtn_status_ind
     ELSE                                                                                                             cust_prof_dedup::latest::mtn_status_ind
     END                                                                                                                                       AS mtn_status_ind:chararray;

--DUMP cust_prof_new_snapshot;

-- EPILOGUE --

-- ----------------------------------------------------------------------------------
--
-- x. Store into the destination tables
--

STORE cust_prof_new_snapshot INTO '$destschema.$custprofiletbl' USING org.apache.hive.hcatalog.pig.HCatStorer('partitiondate=$partitiondate');

-- THE END --
