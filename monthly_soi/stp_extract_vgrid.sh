#!/bin/sh
################################################################################
################################################################################
##### Author: Vijay Teku
##### This script generates Outage_feed Data for VGRID on a daily basis
################################################################################
################################################################################
#set -x

if [[ -z $STPBASE ]]
then
  echo "STPBASE not set, cannot proceed further."
  exit 1
fi
. $STPBASE/config/load_stp_config.cfg

PROCESS=$(basename ${0%.*})
#process_variables
LOGDIR=$STPBASE/logs
PIDFILE=$OUTAGE_FEED_VGRID_EXTRACT_PID_FILE
LOGFILE=$STP_OUTAGE_FEED_VGRID_EXTRACT_LOG_FILE
output_path=$LOAD_OUTAGE_FEED_NDL_VGRID_DIR
VGRID_HDFS_PATH=$OUTAGE_MDN_FEED_HDFS_PATH_TO_VGRID
###############################################################################
##########Function:Usage
###############################################################################
Usage()
{
  echo "Usage: stp_outage_feed_soi_extract.sh <load day>[yyyyMMdd]"
  echo "Ex: sh stp_outage_feed_soi_extract.sh 20190801"
}
################################################################################
##### Function: InstanceCheck
################################################################################
InstanceCheck()
{
  ## See if the PID file exists and if it doesn't, we're good.
  if [[ -a $PIDFILE ]]
  then
    ## Now get the pid from the PID file and see if the PID is active, and
    ## relevant to this process.
    pid=$(cat $PIDFILE 2>>$LOGFILE)
    ps -o args= -p $pid | grep $PROCESS > /dev/null 2>&1
    if [[ $? == 0 ]]
    then
      if [[ -t 0 ]]
      then
        echo "*** $(basename $0) is already running ***"
        ps -fp $pid
      else
        scriptlogger $LOGFILE $PROCESS $$  "[INFO] *** $(basename $0) Already running:\n $(ps -fp $pid)"
      fi
      ## Duplicate instance, so we have to exit.
      exit 1
    fi
  fi
  echo $$ > $PIDFILE
}
################################################################################
################################################################################
##### Function: PrepareEnv
PrepareEnv()
{
  ## Make the necessary directories.
  mkdir -p $LOGDIR
}
################################################################################
################################################################################
##### Function: WritePIDFile
WritePIDFile()
{
  ## Write the PID to PID file.
  echo $$ > $PIDFILE
}
################################################################################
################################################################################
##### Function: ValidateArgs
ValidateArgs()
{
  if [[ $# -eq 0 ]]
  then
  #no_particular_date is given
    scriptlogger $LOGFILE $PROCESS $$ "[INFO] No arguments passed, extracting data for previous_day"
    extract_dt=`date +%Y%m%d -d "1 day ago"`
    scriptlogger $LOGFILE $PROCESS $$ "[INFO] Process starting for $extract_dt"
    ExtractQueries $extract_dt
  elif [[ $# -eq 1 ]]
  then
    #particular_date is given
    extract_dt=$1
    scriptlogger $LOGFILE $PROCESS $$ "[INFO] extract_dt=$extract_dt arguments are passed."
    #Run the Core logic
    scriptlogger $LOGFILE $PROCESS $$ "[INFO] Process starting for extract_dt=$extract_dt"
    ExtractQueries $extract_dt
  else
    Usage
  fi
}
checkDate()
{
  if [[ $(date "+%Y-%m-%d" -d "$1") == "$1" ]]
  then
    scriptlogger $LOGFILE $PROCESS $$  "[INFO] Valid argument. Proceeding to process."
  else
    Usage
  fi
}
################################################################################
################################################################################
##### Function: DropIfExistsPartition
DropIfExistsPartition()
{
  extract_dt=$1
  if [ -d "$output_path/$extract_dt" ];
  then
    rm -r $output_path/$extract_dt >> $LOGFILE 2>&1
    hdfs dfs -rm -r $VGRID_HDFS_PATH/$extract_dt >> $LOGFILE 2>&1
       if [[  $? -ne 0  ]]
       then
         scriptlogger $LOGFILE $PROCESS $$ "[ERROR] Failed to remove NDL_extract folder $output_path/$extract_dt and $VGRID_HDFS_PATH/$extract_dt"
         rm -f $PIDFILE
         exit 1
       else
        scriptlogger $LOGFILE $PROCESS $$ "[INFO] Successfully removed NDL_extract folder $output_path/$extract_dt and $VGRID_HDFS_PATH/$extract_dt. Good to go."
         mkdir -p $output_path/$extract_dt
         scriptlogger $LOGFILE $PROCESS $$ "[INFO] Successfully created NDL_extract folder $output_path/$extract_dt "
         return 0
       fi
  else
   scriptlogger $LOGFILE $PROCESS $$ "[INFO] No prior partition found $output_path/$extract_dt and $VGRID_HDFS_PATH/$extract_dt. Good to go."
   mkdir -p $output_path/$extract_dt
   scriptlogger $LOGFILE $PROCESS $$ "[INFO] Successfully created NDL_extract folder $output_path/$extract_dt. Good to go."
   return 0
  fi
}
################################################################################
################################################################################
##### Function: ExtractQueries
ExtractQueries()
{
extract_dt=$1
load_day1=`date -d $extract_dt "+%Y-%m-%d"`
extract_dt2=`date -d "$extract_dt"'+1 day' '+%Y%m%d'`
DropIfExistsPartition $extract_dt
scriptlogger $LOGFILE $PROCESS $$ "[INFO ] Beginning the processing for outage_feed_extraction for $extract_dt"
/usr/bin/hive -e "set hive.cli.print.header=true;
select
a.outage_id as OUTAGE_ID,
a.outage_name as OUTAGE_NAME,
count(distinct e.enodeb) as NUM_OF_CELLTOWERS ,
a.city as CITY,
a.state STATE,
case when r.resolution_time is null then 'Not_Resolved' else 'Resolved' end Resolved_Status ,
a.start_time as START_TIME,
max(r.resolution_time) as RESOLUTION_TIME ,
count(distinct m.mdn) as NUM_OF_LIKELY_MDNS ,
max(a.load_time) as LOAD_DATE_TIME,
max(r.load_time) as UPDATE_DATE_TIME
from (select o.*, ROW_NUMBER() OVER(PARTITION BY o.outage_id ORDER BY o.load_time DESC) RN from  $HIVEDB.stp_ivr_outages o) a
left outer join $HIVEDB.stp_ivr_outage_enodebs e on a.outage_id = e.outage_id
left outer join $HIVEDB.stp_ivr_outage_mdns m on a.outage_id = m.outage_id
left outer join $HIVEDB.stp_ivr_outage_resolutions r on a.outage_id=r.outage_id
where (r.outage_id is NOT null  or r.outage_id is NULL) and a.RN=1 and a.start_time like '%$load_day1%' and upper(outage_name) not like '%TEST%'
group by a.outage_id, a.outage_name , a.city, a.state, case when r.resolution_time is null then 'Not_Resolved' else 'Resolved' end, a.start_time
order by a.start_time" | sed 's/[\t]/|/g'  > $output_path/$extract_dt/network_outages_$extract_dt.psv
  rc=$?
  if [[ $rc -ne 0 ]]
  then
    scriptlogger $LOGFILE $PROCESS $$ "ERROR while extracting outage_feed_data, please check the log."
    rm -f $PIDFILE >>$LOGFILE 2>&1
    exit 1
    else
      /usr/bin/hive -e "set hive.cli.print.header=true;
      SELECT
      DISTINCT EVENT_ID AS EVENT_ID,
      MDN
      FROM $HIVEDB.STP_15MIN_OUTAGE_MDN_VMB
      WHERE EVENT_DATE_GMT LIKE '%$load_day1%' AND event_id in (select distinct outage_id from $HIVEDB.stp_ivr_outages where start_time like '%$load_day1%'AND upper(outage_name) NOT LIKE '%TEST%')
      AND (trans_dt LIKE '%$extract_dt%' or trans_dt LIKE '%$extract_dt2%') AND UPPER(EVENT_NAME) NOT LIKE '%TEST%' AND outage_15min_time like '%$extract_dt%' ORDER BY EVENT_ID,MDN" | sed 's/[\t]/|/g'  > $output_path/$extract_dt/outage_mdns_$extract_dt.psv
  fi
  if [[ $? -ne 0 ]]
  then
    scriptlogger $LOGFILE $PROCESS $$ "[ERROR] Failed to extract outage_data for $extract_dt. See log for more details."
      rm -f $PIDFILE >>$LOGFILE 2>&1
      return 1
  else
        chmod 755 -R $output_path/$extract_dt
        scriptlogger $LOGFILE $PROCESS $$ "[INFO ] Successfully extracted outage_data for $extract_dt at $output_path/$extract_dt"
        scriptlogger $LOGFILE $PROCESS $$ "[INFO ]  Copying outage_data from NDL_BASE($output_path/$extract_dt) to NDL_HDFS($VGRID_HDFS_PATH) for date $extract_dt"
      hdfs dfs -put $output_path/$extract_dt $VGRID_HDFS_PATH
      if [[ $? -ne 0 ]]
      then
       scriptlogger $LOGFILE $PROCESS $$ "[ERROR] Failed to copy outage_feed_data from NDL_BASE_PATH to NDL_HDFS for VGRID.Please investigate.."
       rm -f $PIDFILE >>$LOGFILE 2>&1
       return 1
      else
         scriptlogger $LOGFILE $PROCESS $$ "[INFO ] Successfully copied outage_feed_data from NDL_BASE_PATH to NDL_HDFS for VGRID for date $extract_dt"
         rm -f $PIDFILE >>$LOGFILE 2>&1
         return 0
       fi
  fi
}
################################################################################
################################################################################
##### Function: Main
Main()
{
  scriptlogger $LOGFILE $PROCESS $$  "----- Process START -----"

  PrepareEnv

  InstanceCheck

  WritePIDFile

  ValidateArgs $@

  scriptlogger $LOGFILE $PROCESS $$  "----- Process ENDS -----"
}

#################################################################################
#################################################################################
#################################################################################
#################################################################################
Main $@
rc=$?
