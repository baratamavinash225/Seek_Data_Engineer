#!/usr/bin/ksh
################################################################################
################################################################################
##### Author: Akhilesh Varma
#####    This script is using to invoke Druid ingestion
################################################################################
################################################################################
if [[ -z $STPBASE ]]
then
  echo "STPBASE not set, cannot proceed further."
  exit 1
fi

if [[ -z $CURR_ENV ]]
then
  echo "CURR_ENV not set, cannot proceed further."
  exit 1
else
  echo "CURR_ENV=$CURR_ENV"
fi

Date=`date "+%Y%m%d"`
. $STPBASE/config/load_stp_config.cfg
#Global Variables
PROCESS=$STP_HOURLY_SCORE_DATA_DRUID_PROCESS
LOGDIR=$STP_HOURLY_SCORE_DATA_DRUID_LOGDIR
#LOGFILE=$STP_HOURLY_SCORE_DATA_DRUID_LOGFILE
#PIDDIR=$STP_HOURLY_SCORE_DATA_DRUID_PERMITDIR
PIDFILE=""
HOURLY_SCORE_DATA_SCHEMA=$STP_HOURLY_SCORE_DATA_SCHEMA
HOURLY_SCORE_DATA_VMAS_TBL=$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY
HOURLY_SCORE_DATA_SAME_AREA_SAME_DEVICE_TBL=$STP_HOURLY_SCORE_DATA_DRUID_SAME_AREA_SAME_DEVICE_TBL
TRANSPATH=$STP_RTT_VMAS_SCORES_DRUID_SUMMARY_HIVE_TRANSFORMED_DATA_DIR
TASKFILE=$STP_HOURLY_SCORE_DATA_DRUID_INDEX_TASK
STP_VMAS_SUBSCRIBER_DEVICE_PATH=$STP_RTT_VMAS_SCORES_DRUID_SUMMARY_HOURLY_TBL_PATH
PIGMODE="-x tez -useHCatalog"
###############################################################################
##########Function:Usage
###############################################################################
Usage()
{
  echo "Usage:$0 Ex:$0(or)"
  echo "(or)"
  echo "$0 <run_date>[yyyyMMdd] <run_hour>[HH] Ex:$0 20180809 08"
}
################################################################################
##### Function: InstanceCheck
InstanceCheck()
{
  ## See if the PID file exists and if it doesn't, we're good.
  if [[ -a $PIDFILE ]]
  then
    ## Now get the pid from the PID file and see if the PID is active, and
    ## relevant to this process.
    pid=$(cat $PIDFILE 2>>$LOGFILE)
    ps -o args= -p $pid | grep $PROCESS |grep $1|grep $2> /dev/null 2>&1
    if [[ $? == 0 ]]
    then
      if [[ -t 0 ]]
      then
        echo "*** $(basename $0) is already running ***"
        ps -fp $pid
      else
        scriptLogger $LOGFILE $PROCESS $$  "[INFO]" " *** $(basename $0) Already running:\n $(ps -fp $pid)"
      fi
      ## Duplicate instance, so we have to exit.
      exit 1
    fi
  fi
  echo $$ > $PIDFILE
}

TransformkpihourlyTranslationData()
{

  run_date=$1
  run_hr=$2
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Starting stp_hourly_druid transformation $STP_HOURLY_SCORE_DATA_DRUID_PIG"

  /usr/bin/pig $PIGMODE -param source_schema=$STP_HOURLY_SCORE_DATA_SCHEMA \
                           -param vmas_kpi_agg_dir=$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_HDFS_OP_PATH/score_dt=$run_date/score_hr=$run_hr \
                           -param subsc_area_device_tbl=$STP_HOURLY_SCORE_DATA_DRUID_SAME_AREA_SAME_DEVICE_TBL \
                           -param hdfs_out_path=$STP_VMAS_SUBSCRIBER_DEVICE_PATH \
                           -param namenode=$STP_NAMENODE \
                           -param out_delim=$STP_HOURLY_SCORE_DATA_DRUID_HIVE_DELIM \
                           -param score_dt=$run_date \
                           -param score_hr=$run_hr \
                           -f $STP_HOURLY_SCORE_DATA_DRUID_PIG $LOGDIR >>$LOGFILE 2>&1

  if [[ $? -ne 0 ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " Failed generate hourly data output at $STP_VMAS_SUBSCRIBER_DEVICE_PATH/score_dt=$run_date/score_hr=$run_hr. See log for more details."
    rm -f $PIDFILE >>$LOGFILE 2>&1
    exit 1
  else
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " generated hourly data output at $STP_VMAS_SUBSCRIBER_DEVICE_PATH/score_dt=$run_date/score_hr=$run_hr."

    #scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Sync Hive table $STP_RTT_VMAS_SCORES_DRUID_SUMMARY_HOURLY_TBL"
    #scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Metasync Process start -------------"
    #/usr/bin/hive -e "msck repair table $STP_HOURLY_SCORE_DATA_SCHEMA.$STP_RTT_VMAS_SCORES_DRUID_SUMMARY_HOURLY_TBL" >>$LOGFILE 2>&1
    #rc=$?
    #if [[ $rc -ne 0 ]]
    #then
     # scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " ERROR while loading $STP_HOURLY_SCORE_DATA_SCHEMA.$STP_RTT_VMAS_SCORES_DRUID_SUMMARY_HOURLY_TBL. Exiting..."
      #rm -f $PIDFILE >>$LOGFILE 2>&1
      #exit 1
    #else
      #scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Metasync Process end -------------"
      #scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Successfully loaded hourly data to hive table $STP_RTT_VMAS_SCORES_DRUID_SUMMARY_HOURLY_TBL for date:$run_date and hour:$run_hr."
    #fi
  fi
}

##############################################################################################
##############################################################################################
######Function:DropIfExistsHDFSPartition
DropIfExistsHDFSPartition()
{
        date=$1
        hr=$2
        hdfs dfs -test -d "$STP_VMAS_SUBSCRIBER_DEVICE_PATH/score_date_druid=$date/score_hr_druid=$hr" >> $LOGFILE 2>&1
        if [[ $? -ne 0 ]]
        then
        scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " No prior partition found "$STP_VMAS_SUBSCRIBER_DEVICE_PATH/score_date_druid=$date/score_hr_druid=$hr" Good to go."
        return 0
        else
                scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Running DRUID INGESTION VMAS hourly kpi SUMMARY scores for $date and $hr"
                scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Truncating $date$hr previous run data at $STP_VMAS_SUBSCRIBER_DEVICE_PATH/score_date_druid=$date/score_hr_druid=$hr"
                hdfs dfs -rm -r -skipTrash "$STP_VMAS_SUBSCRIBER_DEVICE_PATH/score_date_druid=$date/score_hr_druid=$hr" >>$LOGFILE
                /usr/bin/hive -e "ALTER TABLE $STP_HOURLY_SCORE_DATA_SCHEMA.$STP_RTT_VMAS_SCORES_DRUID_SUMMARY_HOURLY_TBL DROP IF EXISTS PARTITION(score_date_druid='$date',score_hr_druid='$hr')" >>$LOGFILE
                if [[ $? -ne 0 ]]
                then
                                scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " Failed to clean directory $STP_VMAS_SUBSCRIBER_DEVICE_PATH/score_date_druid=$date/score_hr_druid=$hr"
				rm -f $PIDFILE >>$LOGFILE 2>&1
                                return 1
                else
                                scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Successfully removed HDFS partition $STP_VMAS_SUBSCRIBER_DEVICE_PATH/score_date_druid=$date/score_hr_druid=$hr. Good to go."
                                return 0

                fi
        fi
}

##############################################################################################
##############################################################################################
######Function:druid_ingestion
##############################################################################################
druid_ingestion()
{
  #kinit -kt /etc/security/keytabs/druid.headless.keytab druid-carondlp1@CARONDLPA.HDP.VZWNET.COM

  TransformkpihourlyTranslationData $1 $2
  record_count=$(hdfs dfs -du -s $STP_VMAS_SUBSCRIBER_DEVICE_PATH/score_date_druid=$1/score_hr_druid=$2/|awk '{ total+=$1 } END { print total }')
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "record_count $record_count "
  if [[ $record_count -le 8 ]]
  then
  	scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "No input data found $STP_VMAS_SUBSCRIBER_DEVICE_PATH/score_date_druid=$1/score_hr_druid=$2. Skipping druid Ingestion.."
	return 0
  fi

  #Starting ingestion
  start_date=`date -d "$1" "+%Y-%m-%d"`
  druid_start_time=`echo $start_date"T"$2":00:00.000Z"`
  echo "$druid_start_time"
  e_time=`date -d "$1 $2 +1 hours" "+%Y-%m-%d %H"`
  e_date=`date -d "$e_time" "+%Y-%m-%d"`
  e_hr=`date -d "$e_time" "+%H"`
  druid_end_time=`echo $e_date"T"$e_hr":00:00.000Z"`
  input_for_druid=$STP_VMAS_SUBSCRIBER_DEVICE_PATH/score_date_druid=$1/score_hr_druid=$2/*
   #Replacing input path, timeperiod and inputpath
   sed "s/timeperiod/$druid_start_time\/$druid_end_time/"  $STPBASE/config/$TASKFILE.variable > $STPBASE/config/$TASKFILE.$1$2
   sed -i "s#inputPath#$input_for_druid#" $STPBASE/config/$TASKFILE.$1$2

  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Start ingesting $STP_HOURLY_SCORE_DATA_SCHEMA.$STP_RTT_VMAS_SCORES_DRUID_SUMMARY_HOURLY_TBL to druid"
  #Replacing timeperiod and inputpath
  sed "s/timeperiod/$druid_start_time\/$druid_end_time/"  $STPBASE/config/$TASKFILE.variable > $STPBASE/config/$TASKFILE.$1$2

  sed -i "s#inputPath#$input_for_druid#" $STPBASE/config/$TASKFILE.$1$2
  #Druid Ingest cmd
  #response=`curl -X 'POST' -H 'Content-Type:application/json' --negotiate -u: -v -d @$STPBASE/config/$TASKFILE  http://$DRUID_OVERLORD_IP_PORT/druid/indexer/v1/task`
  if [[ $CURR_ENV == "PROD" ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Processing CURR_ENV val: $CURR_ENV"
    response=`curl -X 'POST' -H 'Content-Type:application/json' --negotiate -u: -v -d @$STPBASE/config/$TASKFILE.$1$2  http://$DRUID_OVERLORD_IP_PORT/druid/indexer/v1/task`
  elif [[ $CURR_ENV == "UAT" ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Processing CURR_ENV val: $CURR_ENV"
    response=`curl -X 'POST' -H 'Content-Type:application/json' -d @$STPBASE/config/$TASKFILE.$1$2  http://$DRUID_OVERLORD_IP_PORT/druid/indexer/v1/task`
  elif [[ $CURR_ENV == "SIT" ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Processing CURR_ENV val: $CURR_ENV"
    response=`curl -X 'POST' -H 'Content-Type:application/json' -d @$STPBASE/config/$TASKFILE.$1$2  http://$DRUID_OVERLORD_IP_PORT/druid/indexer/v1/task`
  elif [[ $CURR_ENV == "DEV" ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Processing CURR_ENV val: $CURR_ENV"
    response=`curl -X 'POST' -H 'Content-Type:application/json' -d @$STPBASE/config/$TASKFILE.$1$2  http://$DRUID_OVERLORD_IP_PORT/druid/indexer/v1/task`
  else
    scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " Invalid Environment. CURR_ENV val: $CURR_ENV"
    rm -rf  $PIDFILE >>$LOGFILE 2>&1
    exit 1
  fi
  #Getting Task id
  task_id=`echo "$response"|sed 's/"//g'|awk -F":index" '{print $2}'|sed 's/}//'`
  if [[ $response =~ "\"task\":" ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Druid Overlord TaskId:$TASKFILE Response:$response"
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Wait for $STP_DRUID_SUBS_TRANS_TASK_WAIT_TIME secs to complete druid TaskId"
    sleep $STP_DRUID_SUBS_TRANS_TASK_WAIT_TIME
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Checking druid TaskId status:index$task_id"
    druid_status_check $response $task_id
  else
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Druid Overlord TaskId:$TASKFILE Response:$response"
    #scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Finished Druid subs translation job"
    #exit
  fi
  return 0
}
#############################################################################################
#############################################################################################
###### Function:druid_status_check
#############################################################################################
druid_status_check()
{
  response=$1
  #Getting Task id
  task_id=$2
#checking Druid Task Id status
  druid_status_url="http://$DRUID_OVERLORD_IP_PORT/druid/indexer/v1/task/index$task_id/status"
  run_type=$STP_DRUID_SUBS_TRANS_STATUS_CHECK_TYPE
  no_of_status_check_tries=0
  while [[ 1 ]]
  do
    #res=`curl -X GET --negotiate -u: -v $druid_status_url`
    if [[ $CURR_ENV == "PROD" ]]
    then
      res=`curl -X GET --negotiate -u: -v $druid_status_url`
    elif [[ $CURR_ENV == "UAT" ]]
    then
      res=`curl -X GET $druid_status_url`
    elif [[ $CURR_ENV == "SIT" ]]
    then
      res=`curl -X GET $druid_status_url`
    elif [[ $CURR_ENV == "DEV" ]]
    then
      res=`curl -X GET $druid_status_url`
    fi
    #status=`echo $res|awk -F"," '{print $3}'|cut -d ":" -f2|sed 's/"//g'`
     logFile=`echo "index$task_id"|sed 's/:/_/g'`
    if [[ $res =~ "SUCCESS" ]]
    then
      scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Finished ingesting $STPSCHEMA.stp_rtt_vmas_scores_druid_summary_hourly Table to druid"
      return 0
    elif [[ $res =~ "FAILED" ]]
    then
      scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " Failed to ingest $STPSCHEMA.stp_rtt_vmas_scores_druid_summary_hourly to druid"
      scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Verify druid log at $STP_NAMENODE/$DRUID_LOG_PATH/$logFile"
      rm -rf $PIDFILE >>$LOGFILE 2>&1
      return 1
    elif [[ $res =~ "RUNNING" ]]
    then
      scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Druid TaskId still running"
      scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Wait for  $STP_DRUID_SUBS_TRANS_TASK_WAIT_TIME secs to complete druid TaskId"
      sleep $STP_DRUID_SUBS_TRANS_TASK_WAIT_TIME
    else
      scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Druid TaskId:index$task_id status response:$res"
      scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Verify druid log at $STP_NAMENODE/$DRUID_LOG_PATH/$logFile"
      return 0
    fi
    if [[ $run_type != "continuous" ]]
    then
      (( no_of_status_check_tries++ ))
      if [[ $no_of_status_check_tries -eq $STP_DRUID_SUBS_TRANS_STATUS_CHECK_MAX_TRIES && $status =~ "RUNNING" ]]
      then
        scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Max of no of tries reached still TaskId running"
        scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Exiting from process....."
        return 0
      fi
    fi
  done
return 0
}
################################################################################
################################################################################
##### Function: Main
main()
{
  LOGFILE=$LOGDIR/$PROCESS.$1$2.$Date.log
  PIDFILE=$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_PERMIT_DIR/$PROCESS.$1$2.pid
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " ----- Process START -----"
  InstanceCheck $1 $2
 
  if [[ $# -eq 2 ]]
  then
    run_date=$1
    run_hour=$2

    if echo $run_date | egrep -q '^[0-9]{4}[0-9]{2}[0-9]{2}$' && echo $run_hour | egrep -q '^[0-9]{2}$'
    then
          DropIfExistsHDFSPartition $run_date $run_hour
          druid_ingestion "$run_date" "$run_hour"
  	  mv $STPBASE/config/$TASKFILE.$1$2 $LOGDIR
	#	  exit $?
    else
      scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " command line Date arguments expected format yyyyMMdd HH"
    fi
  else
    Usage
  fi
  rm -rf $PIDFILE >>$LOGFILE 2>&1 
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " ----- Process End ------"
  return 0
}
#################################################################################
#################################################################################
#################################################################################
#################################################################################
main $1 $2
