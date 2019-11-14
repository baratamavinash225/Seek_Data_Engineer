------------------------------------------------------------------------------------------------------------------------------------------------
--
-- Script Name: LCI_AUDIT_UPDATE.hql
--
-- Author: Krishna Harish Katuru (Harry)
--
-- Usage:
--   hive -hiveconf:DATABASE_NAME=$HIVE_DB_NAME -f STP_LOAD_SLOW_CHANGING_DIMENSIONS.hql
--
--
------------------------------------------------------------------------------------------------------------------------------------------------
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.compress.output=true;
SET hive.exec.compress.intermediate=true;

--
-- 1. Truncate the SCD Temp Table.
--

-- select * from ${hiveconf:DATABASE_NAME}.${hiveconf:cust_profile};

TRUNCATE TABLE ${hiveconf:DATABASE_NAME}.${hiveconf:scd_temp};

-- New records inserted into temp table
INSERT INTO ${hiveconf:DATABASE_NAME}.${hiveconf:scd_temp}
SELECT SRC.FILE_DATE FROM_DATE_TIME, TO_DATE ('9999-12-31') TO_DATE_TIME, SRC.MDN, SRC.EMIN, SRC.IMEI, SRC.IMSI, SRC.MANUFACTURER, SRC.MODEL,
       CURRENT_TIMESTAMP CREATETIME, date_format(CURRENT_TIMESTAMP,'yyyyMMddHHmmss') LOADTIME
       FROM (
             SELECT MDN, EMIN, IMEI, IMSI, MANUFACTURER, MODEL, FILE_DATE
                    FROM (
                          SELECT MDN, EMIN, EQP_DEVICE_ID IMEI, VZW_IMSI IMSI, MANUFACTURER, MODEL, FILEDATE FILE_DATE, ROW_NUMBER()
                                 OVER ( PARTITION BY MDN, EMIN, EQP_DEVICE_ID, VZW_IMSI, MANUFACTURER, MODEL
                                        ORDER BY  CREATETIME DESC
                                      ) AS RN
                                 FROM ${hiveconf:DATABASE_NAME}.${hiveconf:cust_profile}
                                 WHERE EQP_DEVICE_ID IS NOT NULL AND VZW_IMSI IS NOT NULL
                         ) SRC
                    WHERE RN = 1
            ) SRC
            LEFT OUTER JOIN ${hiveconf:DATABASE_NAME}.${hiveconf:scd} TAR ON SRC.EMIN = TAR.EMIN AND
                                                   SRC.MODEL = TAR.MODEL AND
                                                   SRC.MANUFACTURER = TAR.MANUFACTURER AND
                                                   TAR.PARTITIONDATE = '${hiveconf:scd_max_partition}'
                                                WHERE TAR.EMIN IS NULL;

-- select * from ${hiveconf:DATABASE_NAME}.${hiveconf:scd_temp};

--
-- 2. Bring the current partion data from scd into temp table.
--

INSERT INTO ${hiveconf:DATABASE_NAME}.${hiveconf:scd_temp}
SELECT FROM_DATE_TIME, TO_DATE_TIME, MDN, EMIN, IMEI, IMSI, MANUFACTURER, MODEL,
       CREATETIME, LOADTIME FROM ${hiveconf:DATABASE_NAME}.${hiveconf:scd} WHERE PARTITIONDATE='${hiveconf:scd_max_partition}';

-- select * from ${hiveconf:DATABASE_NAME}.${hiveconf:scd_temp};

--
-- 3. Insert into SCD table with new partition value using temp table
--

INSERT INTO ${hiveconf:DATABASE_NAME}.${hiveconf:scd} PARTITION(PARTITIONDATE)
SELECT FROM_DATE_TIME, NEW_TO_TIME TO_DATE_TIME, MDN, EMIN, IMEI, IMSI, MANUFACTURER, MODEL,
       CREATETIME, date_format(CURRENT_TIMESTAMP,'yyyyMMddHHmmss') loadtime,date_format(CURRENT_TIMESTAMP,'yyyyMMddHHmm') PARTITIONDATE
       FROM (
             SELECT LEAD (FROM_DATE_TIME, 1 ,TO_DATE ('9999-12-31'))
                    OVER (PARTITION BY A.EMIN ORDER BY FROM_DATE_TIME) NEW_TO_TIME, A.*
                    FROM ${hiveconf:DATABASE_NAME}.${hiveconf:scd_temp} A) dat;

--
-- THE END!!
--
