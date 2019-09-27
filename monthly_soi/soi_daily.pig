SET pig.tmpfilecompression false;
SET hive.exec.orc.split.strategy BI;

SET mapreduce.map.java.opts -Xmx3072M;
SET mapreduce.map.java.opts -Xmx6144M;
SET mapreduce.task.io.sort.mb 1024;
SET pig.maxCombinedSplitSize 67108864;
SET mapreduce.map.memory.mb 4096;
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

define hex InvokeForInt('java.lang.Integer.parseInt','String int','true');
define reverse org.apache.pig.piggybank.evaluation.string.Reverse();

--Load LTE_SDR data from HIVE
lte_sdr_tbl = LOAD '$LTE_SCHEMA.$LTETBL' USING org.apache.hive.hcatalog.pig.HCatLoader();

fil_lte_sdr_tbl = FILTER lte_sdr_tbl BY (sessionstartdate=='$TRANS_DT');

--Filter columns in need
lte_sdr_data = FOREACH fil_lte_sdr_tbl GENERATE
  '$TRANS_DT' as trans_dt:chararray,
  REPLACE((chararray)servedmsisdn,'\\+','') as servedmsisdn:chararray,
  servedimsi as servedimsi:chararray,
  listofservicedata as listofservicedata:chararray ,
  userlocationinformation as userlocationinformation:chararray,
  accesspointnameni as accesspointnameni:chararray;

--Load subscriber tbl
subscriber_data = LOAD '$SUB_SCHEMA.$SUBTBL' USING org.apache.hive.hcatalog.pig.HCatLoader();

--Filter fields in need
fil_sub_data = FOREACH subscriber_data GENERATE mdn, imsi, update_time;

--Get latest MDN from subscriber profile
get_latest_sub_prof = FOREACH (GROUP fil_sub_data BY imsi) {
        ordered = ORDER fil_sub_data BY update_time DESC;
        latest = LIMIT ordered 1;
        GENERATE FLATTEN(latest);
};

get_latest_sub_data = foreach get_latest_sub_prof generate
        latest::mdn as mdn:chararray,
        latest::imsi as imsi:chararray,
        latest::update_time as update_time:long;

--Filter null MDNs to get MDN by IMSI correlation from stp_master_subscriber_profile
null_mdns = FILTER lte_sdr_data BY (servedmsisdn is NULL) AND (servedimsi is not NULL);

--Filter valid MDN/ENB records
SPLIT lte_sdr_data into null_mdns if (servedmsisdn is null AND servedimsi is not null),valid_mdns if (servedmsisdn is not null AND (SUBSTRING(servedmsisdn,0,2) == '91') AND userlocationinformation is not null AND ((SUBSTRING(userlocationinformation,0,2) == '18') OR (SUBSTRING(userlocationinformation,0,2) == '10')) AND listofservicedata is not null AND accesspointnameni is not null AND accesspointnameni != 'Usage Segmentation' AND accesspointnameni != 'Go90 \\- Legacy' AND accesspointnameni != 'NFL app' AND accesspointnameni != 'Go90\\- NFL Live stream \\- Legacy' AND accesspointnameni != 'Go90 New Version' AND accesspointnameni != 'VzW Messages');

--Get MDN by IMSI
join_subltr = JOIN null_mdns BY servedimsi, get_latest_sub_data BY imsi;

get_mdn = FOREACH join_subltr GENERATE
        null_mdns::trans_dt as trans_dt,
        get_latest_sub_data::mdn as servedmsisdn,
        null_mdns::servedimsi as servedimsi,
        null_mdns::listofservicedata as listofservicedata,
        null_mdns::userlocationinformation as userlocationinformation,
        null_mdns::accesspointnameni as accesspointnameni;


--Merge all records
total_recs = UNION valid_mdns, get_mdn;

--Parse ListOfServiceData
rem_unwanted_chars = foreach total_recs generate
        trans_dt,
        servedmsisdn,
        servedimsi,
        userlocationinformation,
        accesspointnameni,
        FLATTEN(TOKENIZE(REPLACE(REPLACE(REPLACE(SUBSTRING(listofservicedata,1,(int)SIZE(listofservicedata)),'\\)\\(','|'),'\\(',''),'\\)',''),'|')) as losd:chararray;
flatten_losd_cols = foreach rem_unwanted_chars generate
        trans_dt,
        servedmsisdn,
        servedimsi,
        userlocationinformation,
        accesspointnameni,
        FLATTEN(STRSPLIT(losd,';')) AS (
                ratinggroup:chararray,
                afchargingidentifier:chararray,
                frommobilebytes:chararray,
                tomobilebytes:chararray,
                timeoffirstusage:chararray,
                timeoflastusage:chararray,
                serviceconditionchange:chararray,
                qosclassidentifier:chararray,
                bearerchargingid:chararray,
                apnaggregatemaxbitrateul:chararray,
                apnaggregatemaxbitratedl:chararray);

--Parse logic
parse_lte_sdr = FOREACH flatten_losd_cols GENERATE
trans_dt,
servedmsisdn as mdn_orig,
--Parse MDN
CASE WHEN SUBSTRING(servedmsisdn,0,2) == '91' THEN
        CASE WHEN SUBSTRING(servedmsisdn,(int)(SIZE(servedmsisdn)-2),(int)(SIZE(servedmsisdn)-1)) == 'f'
        THEN
                CASE WHEN SIZE(REPLACE(CONCAT(SUBSTRING(reverse(SUBSTRING(servedmsisdn,2,4)),1,2),reverse(SUBSTRING(servedmsisdn,4,6)),reverse(SUBSTRING(servedmsisdn,6,8)),reverse(SUBSTRING(servedmsisdn,8,10)),reverse(SUBSTRING(servedmsisdn,10,12)),reverse(SUBSTRING(servedmsisdn,12,14))),'f',''))==10
                THEN
                        REPLACE(CONCAT(SUBSTRING(reverse(SUBSTRING(servedmsisdn,2,4)),1,2),reverse(SUBSTRING(servedmsisdn,4,6)),reverse(SUBSTRING(servedmsisdn,6,8)),reverse(SUBSTRING(servedmsisdn,8,10)),reverse(SUBSTRING(servedmsisdn,10,12)),reverse(SUBSTRING(servedmsisdn,12,14))),'f','')
                ELSE
                        'invalid_mdn'
                END
        ELSE
                CASE WHEN SIZE(CONCAT(SUBSTRING(reverse(SUBSTRING(servedmsisdn,2,4)),1,2),reverse(SUBSTRING(servedmsisdn,4,6)),reverse(SUBSTRING(servedmsisdn,6,8)),reverse(SUBSTRING(servedmsisdn,8,10)),reverse(SUBSTRING(servedmsisdn,10,12)),reverse(SUBSTRING(servedmsisdn,12,14))))==10
                THEN
                        CONCAT(SUBSTRING(reverse(SUBSTRING(servedmsisdn,2,4)),1,2),reverse(SUBSTRING(servedmsisdn,4,6)),reverse(SUBSTRING(servedmsisdn,6,8)),reverse(SUBSTRING(servedmsisdn,8,10)),reverse(SUBSTRING(servedmsisdn,10,12)),reverse(SUBSTRING(servedmsisdn,12,14)))
                ELSE
                        'invalid_mdn'
                END
        END
ELSE

        'invalid_mdn'

END as mdn:chararray,
--Parse ENB
CASE WHEN SUBSTRING(userlocationinformation,0,2) == '18'
THEN
        SUBSTRING((chararray)hex(SUBSTRING(userlocationinformation,19,24),16), (int)SIZE((chararray)hex(SUBSTRING(userlocationinformation,19,24),16))-3, (int)SIZE((chararray)hex(SUBSTRING(userlocationinformation,19,24),16)))
WHEN SUBSTRING(userlocationinformation,0,2) == '10'
THEN
        SUBSTRING((chararray)hex(SUBSTRING(userlocationinformation,9,14),16), (int)SIZE((chararray)hex(SUBSTRING(userlocationinformation,9,14),16))-3, (int)SIZE((chararray)hex(SUBSTRING(userlocationinformation,9,14),16)))
ELSE
'invalid_enb'
END as enb:chararray,

--Parse UsageType
CASE WHEN (accesspointnameni == 'vzwims' OR accesspointnameni == 'ims')  AND
                  (ratinggroup == '102' OR ratinggroup == '103' OR ratinggroup == '104' OR ratinggroup == '105' OR ratinggroup == '109')
THEN
        ('voice',(long)((long)SUBSTRING(timeoflastusage,0,13) - (long)SUBSTRING(timeoffirstusage,0,13)),null)
ELSE
        ('data',null,(long)((long)frommobilebytes + (long)tomobilebytes))
END as snap:(usagetype:chararray, secondsofuse:long, totalmobilebytes:long);

final_data = FOREACH parse_lte_sdr GENERATE trans_dt, mdn, enb, FLATTEN(snap) as (usagetype:chararray, secondsofuse:long, totalmobilebytes:long);

--filter invalid_mdn and invalid_enb records
SPLIT final_data into invalid_mdns if (mdn == 'invalid_mdn'), invalid_enbs if (enb == 'invalid_enb'), valid_data if (mdn != 'invalid_mdn' AND enb != 'invalid_enb');

--group by date,mdn,enb,usagetype
grouped = GROUP valid_data BY (trans_dt,mdn,enb,usagetype);

--Agg daily
final_agg = foreach grouped GENERATE FLATTEN(group) as (trans_dt,mdn,enb,usagetype), SUM(valid_data.totalmobilebytes) as totalmobilebytes, SUM(valid_data.secondsofuse) as secondsofuse;

STORE final_agg into '$HDFSOUTDIR' USING PigStorage('|');
STORE invalid_mdns into '$HDFS_INVALID_MDN_OUTDIR' USING PigStorage('|');
STORE invalid_enbs into '$HDFS_INVALID_ENB_OUTDIR' USING PigStorage('|');
