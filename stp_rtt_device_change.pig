-- ----------------------------------------------------------------------------------
-- (C) COPYRIGHT 2014 Verizon Business Inc.
-- All rights Reserved Worldwide US Government Users Restricted Rights - Use, duplication or
-- disclosure restricted by GSA ADP Schedule Contract with Verizon Business Inc.
--
-- Script      : stp_rtt_device_change.pig
-- Author      : Ahtesham Ali
-- Description : This script is used to load the customer profile table
-- Usage       : pig -useHCatalog    -param stp_db='npi_cem_db'       -param source_table='stp_master_subscriber_profile'    -param output_table='stp_rtt_customer_event_history_device_change'     stp_rtt_device_change.pig
-- ----------------------------------------------------------------------------------
SET pig.tmpfilecompression false;
SET pig.tez.opt.union false;
SET pig.maxCombinedSplitSize 67108864;
SET mapreduce.map.java.opts -Xmx3072M;
SET mapreduce.map.memory.mb 3584;
SET mapreduce.reduce.java.opts -Xmx3072M;
SET mapreduce.reduce.memory.mb 3584;
SET mapreduce.task.io.sort.mb 1024;
SET mapred.output.compress true;


-- Getting last commit time
rmf max_last_commit_time.txt
dest_table = LOAD '$stp_db.$output_table' using org.apache.hive.hcatalog.pig.HCatLoader();
dest_values = GROUP dest_table ALL;
last_commit_time = foreach dest_values Generate (long)MAX(dest_table.event_id) ;
max_last_commit_time = LIMIT last_commit_time 1;

store max_last_commit_time into 'max_last_commit_time.txt' USING PigStorage(',');
last_commit = LOAD 'max_last_commit_time.txt' using PigStorage(',') AS (max_time:long);
dump last_commit;

-- Loading Source table
custProf = LOAD '$stp_db.$source_table' using org.apache.hive.hcatalog.pig.HCatLoader() as (mdn:chararray, imei:chararray, imsi:chararray, subscriber_id:long, manufacturer:chararray, model:chararray, update_time:long, scenario:chararray, vz_customer:chararray, commit_time:long);

-- Getting New Device
deviceChange= filter custProf by commit_time > last_commit.max_time;
newDeviceGrp = GROUP deviceChange BY subscriber_id;
newDeviceDetails = FOREACH newDeviceGrp {
ord = order deviceChange by update_time DESC;
top = Limit ord 1;
Generate flatten(top);
};

-- Getting Old device
oldDeviceData= filter custProf by commit_time <= last_commit.max_time;
oldDeviceDataGrp = GROUP oldDeviceData BY subscriber_id;
oldDeviceDetails = FOREACH oldDeviceDataGrp {
ord = order oldDeviceData by update_time DESC;
top = Limit ord 1;
Generate flatten(top);
};


-- Joining and filtering for Changed devices
custProfJoin= join newDeviceDetails by subscriber_id, oldDeviceDetails by subscriber_id;
changedImei = filter custProfJoin by newDeviceDetails::top::imei != oldDeviceDetails::top::imei;

-- Generating and saving Result
result= FOREACH changedImei GENERATE
ToDate((chararray)newDeviceDetails::top::commit_time,'yyyyMMddHHmmss') As event_date_gmt:datetime,
newDeviceDetails::top::subscriber_id    As      subscriber_id:chararray,
newDeviceDetails::top::imei      As  imei:chararray,
newDeviceDetails::top::mdn       As  mdn:chararray,
newDeviceDetails::top::imsi      As  imsi:chararray,
'DeviceChange'   As     record_type:chararray,
newDeviceDetails::top::commit_time As event_id:chararray,
'Device Change'         As      event_type:chararray,
'NULL'  As      event_name:chararray,
'NULL'  As      event_location:chararray,
CONCAT ('Original Device:', oldDeviceDetails::top::manufacturer,',', oldDeviceDetails::top::model,',', oldDeviceDetails::top::imei,'~New Device:', newDeviceDetails::top::manufacturer,',', newDeviceDetails::top::model,',', newDeviceDetails::top::imei) As   event_details:chararray,
'NULL'   As      service_impacted:chararray,
'NULL'   As      service_type:chararray,
ToDate((chararray)newDeviceDetails::top::commit_time,'yyyyMMddHHmmss') As start_date:datetime,
NULL  As      resolved_date:datetime,
'NULL'  As      resolved_category:chararray,
'NULL'  As      severity:chararray,
CurrentTime() As      create_date:datetime,
'Subscriber Profile'    As      data_source:chararray,
ToString(ToDate((chararray)newDeviceDetails::top::commit_time,'yyyyMMddHHmmss'),'yyyy-MM-dd') As event_day:chararray,
newDeviceDetails::top::commit_time As      event_commit_time:chararray;

store result into '$stp_db.$output_table' using org.apache.hive.hcatalog.pig.HCatStorer();
