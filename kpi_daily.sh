#!/bin/sh

# Author    : Sushmitha P
# Purpose   : Used to aggregate RTT hourly data on daily level and store into hdfs path and to create hive table
# Usage     : ./stp_load_rtt_kpi_summary_daily.sh 20190326

if [[ -z $STPBASE ]]
then
  echo "STPBASE not set, cannot proceed further."
  exit 1
fi

. $STPBASE/config/load_stp_config.cfg

Usage()
{
  echo "Usage:$0"
  echo " sh stp_load_rtt_kpi_summary_daily.sh [<yyyy-mm-dd]>"
  echo "Ex: $0 2019-03-26"
}

PROCESS="stp_load_rtt_kpi_summary_daily"
LOGFILE=$STP_LOAD_RTT_KPI_SUMMARY_DAILY_LOG_FILE
PIDFILE=$STP_LOAD_RTT_KPI_SUMMARY_DAILY_PID_FILE
LOGDIR=$STP_LOAD_RTT_KPI_SUMMARY_DAILY_LOG_DIR
PIGSCRIPT=$STP_LOAD_RTT_KPI_SUMMARY_DAILY_PIG
PIGMODE=$STP_LOAD_RTT_KPI_SUMMARY_DAILY_PIG_MODE
HDFSINPUTPATH=$STP_RTT_KPI_SUMMARY_HRLY_HDFS_PATH
HDFSOUTPATH=$STP_RTT_KPI_SUMMARY_DAILY_HDFS_PATH
TARGETTABLE_SCHEMA=$STP_LOAD_RTT_KPI_SUMMARY_DAILY_OUTPUT_HIVE_TABLE_SCHEMA
TARGETTABLE=$STP_RTT_KPI_SUMMARY_DAILY_EXT_TABLE
TARGETSTATUSFILE=$STP_RTT_KPI_SUMMARY_DAILY_STATUS_SHELL_VERSION
DELIM=$STP_RTT_KPI_SUMMARY_DAILY_DELIM

### Declare Global Variables
################################################################################
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
    ps -o args= -p $pid | grep $PROCESS > /dev/null 2>&1
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
##### Function: checkDate
checkDate()
{
  if [[ $(date "+%Y-%m-%d" -d "$1") == "$1" ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Valid argument. Proceeding to process"
  else
    Usage
  fi
}

################################################################################
################################################################################
##### Function: ValidateArgs
ValidateArgs()
{
  if [[ $# -eq 0 ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " No arguments passed, calculating trans_date based on current date by taking latency as 1"
    trans_dt=`date -d @$(( $(date +"%s") - 24*3600)) +"%Y-%m-%d"`
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Process starting for $trans_dt"
    CoreLogic $trans_dt  
  elif [[ $# -eq 1 ]]
  then
    #Only date is given
    run_dt=$1
    checkDate $run_dt
    #Loop for all Hrs in a day
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Only rundate=$run_dt argument is passed, preparing to run for $run_dt"
    CoreLogic $run_dt
     else
    Usage
  fi
}

################################################################################
################################################################################
##### Function: DropIfExistsHDFSPartition
DropIfExistsHDFSPartition()
{
  trans_date=$1
  trans_hr=$2
  hadoop fs -test -d "$HDFSOUTPATH/trans_dt=$trans_date" >> $LOGFILE 2>&1
  
  if [[ $? -ne 0 ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " NO prior partition found $HDFSOUTPATH/trans_dt=$trans_date Good to go."
	return 0
  else
    hadoop fs -rm -r -skipTrash "$HDFSOUTPATH/trans_dt=$trans_date" >> $LOGFILE 2>&1
    if [[  $? -ne 0  ]]
    then
      scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " Failed to remove HDFS folder $HDFSOUTPATH/trans_dt=$trans_date"
        rm -f $PIDFILE
        exit 1
    else
      scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Successfully removed HDFS partition $HDFSOUTPATH/trans_dt=$trans_date Good to go."
        return 0
    fi
  fi
}

################################################################################
################################################################################
##### Function: RunRttKpiDailyAggregation 
RunRttKpiDailyAggregation ()
{
  trans_date=$1
  DropIfExistsHDFSPartition $runDate
 
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " /usr/bin/pig -Dpig.additional.jars=$PIGGYBANK_JAR -Dexectype=$PIGMODE -useHCatalog -f $PIGSCRIPT -l $LOGDIR \
               -param inputpath=$HDFSINPUTPATH/trans_dt=$trans_date \
               -param delim=$DELIM \
               -param outputpath=$HDFSOUTPATH \
               -param rtt_date_yyyyMMdd=$trans_date >>$LOGFILE 2>&1 "
 

  /usr/bin/pig -Dpig.additional.jars=$PIGGYBANK_JAR -Dexectype=$PIGMODE -useHCatalog -f $PIGSCRIPT -l $LOGDIR \
               -param inputpath=$HDFSINPUTPATH/trans_dt=$trans_date \
               -param delim=$DELIM \
               -param outputpath=$HDFSOUTPATH \
               -param rtt_date_yyyyMMdd=$trans_date >>$LOGFILE 2>&1

  if [[ $? -ne 0 ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " Failed to aggregate RTT Kpi for Daily at $HDFSOUTPATH/trans_dt=$trans_date. See log for more details."
	return 1
  else
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Successfully aggregate RTT Kpi for Daily at $HDFSOUTPATH/trans_dt=$trans_date"
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Metasync Process start -------------"
    hive -e "msck repair table $TARGETTABLE_SCHEMA.$TARGETTABLE" >>$LOGFILE 2>&1
    rc=$?
    if [[ $rc -ne 0 ]]
    then
      scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " Metasync Process Failed ------------"
      return 1
   fi 
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Metasync Process end -------------"
    
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " hive -hiveconf dbschema=$TARGETTABLE_SCHEMA -hiveconf tablename=$TARGETTABLE -hiveconf trans_dt=$trans_date -f $TARGETSTATUSFILE >>$LOGFILE 2>&1"
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " hive -hiveconf dbschema=$TARGETTABLE_SCHEMA -hiveconf tablename=$TARGETTABLE -hiveconf trans_dt=$trans_date -f $TARGETSTATUSFILE >>$LOGFILE 2>&1"
    hive -hiveconf dbschema=$TARGETTABLE_SCHEMA -hiveconf tablename=$TARGETTABLE -hiveconf trans_dt=$trans_date -f $TARGETSTATUSFILE >>$LOGFILE 2>&1
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " stp_rtt_kpi_summary_status table updated successfully -------------"

    return 0
  fi
}
################################################################################
################################################################################
##### Function: CheckIfExistsHDFSPath
CheckIfExistsHDFSPath()
{ 
  hadoop fs -test -d "$1"
  if [[ $? -ne 0 ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " Input path not found at $1. Exiting." 
    exit 1
  else
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Input path found at $1  Good to go."
    return 0
  fi
}

################################################################################
################################################################################
##### Function: CoreLogic
CoreLogic()
{
  runDate=$1
 
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Beginning the processing for RTT KPI SUMMARY for Daily for $runDate"
  CheckIfExistsHDFSPath $HDFSINPUTPATH/trans_dt=$runDate
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Checking RTT KPI Summary Hourly path in HDFS"
  RunRttKpiDailyAggregation $runDate
  rm -f $PIDFILE
  return $?
}


################################################################################
################################################################################
##### Function: Main
Main()
{
  scriptLogger $LOGFILE $PROCESS $$  "[INFO]" " ----- Process START -----"

  PrepareEnv

  InstanceCheck

  WritePIDFile
 
  ValidateArgs $@

  scriptLogger $LOGFILE $PROCESS $$  "[INFO]" " ----- Process ENDS -----"
}

#################################################################################
#################################################################################
#################################################################################
#################################################################################

Main $@
