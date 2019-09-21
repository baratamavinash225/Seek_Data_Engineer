#!/bin/sh

# Author    : Akhilesh Varma
# Purpose   : Used to aggregate SOI monthly data and store into hdfs path and to create hive table
# Usage     : ./stp_monthly_soi_enb_feed.sh 201906

if [[ -z $STPBASE ]]
then
  echo "STPBASE not set, cannot proceed further."
  exit 1
fi

. $STPBASE/config/load_stp_config.cfg

Usage()
{
  echo "Usage:$0"
  echo " sh stp_monthly_soi_enb_feed.sh [<yyyy]>"
  echo "Ex: $0 201903"
}

PROCESS="stp_monthly_soi_enb_feed"
LOGFILE=$STP_MONTHLY_SOI_ENB_FEED_LOG_FILE
PIDFILE=$STP_MONTHLY_SOI_ENB_FEED_PID_FILE
HIVE_SCHEMA=$STP_SOI_ENB_FEED_HIVE_TABLE_SCHEMA
SOURCE_TBL=$STP_LTE_SDR_AGG_DAILY
PIGSCRIPT=$STP_MONTHLY_SOI_ENB_FEED_PIG
PIGMODE=$STP_MONTHLY_SOI_ENB_FEED_PIG_MODE
HDFSINPUTPATH=$STP_MONTHLY_SOI_ENB_FEED_HDFS_PATH
HDFSOUTPATH=$STP_MONTHLY_SOI_ENB_FEED_HDFS_PATH
TARGETTABLE=$STP_MONTHLY_SOI_ENB_FEED_EXT_TABLE
DELIM=$STP_MONTHLY_SOI_ENB_FEED_DELIM

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
##### Function: WritePIDFile
WritePIDFile()
{
  ## Write the PID to PID file.
  echo $$ > $PIDFILE
}
################################################################################
################################################################################
##### Function: validateDateMonth
validateDateMonth()
{
  if echo $1 | egrep -q '^[0-9]{4}[0-9]{2}$'
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
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " No arguments passed, calculating the soi month based on current date by a month latency"
    previousYearMonth=`date -d "$(date +%Y-%m-1) -1 month" +%Y%m`
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Process starting for $previousYearMonth"
    CoreLogic $previousYearMonth  
  elif [[ $# -eq 1 ]]
  then
    #Only date is given
    monthYearToRun=$1
    validateDateMonth $monthYearToRun
    #Loop for all Hrs in a day
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Only trans_mnth=$monthYearToRun argument is passed, preparing to run for $monthYearToRun"
    CoreLogic $monthYearToRun
     else
    Usage
  fi
}

################################################################################
################################################################################
##### Function: DropIfExistsHDFSPartition
DropIfExistsHDFSPartition()
{
  trans_mnth=$1
  hadoop fs -test -d "$HDFSOUTPATH/trans_mnth=$trans_mnth" >> $LOGFILE 2>&1
  
  if [[ $? -ne 0 ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " NO prior partition found $HDFSOUTPATH/trans_mnth=$trans_mnth Good to go."
	return 0
  else
    hadoop fs -rm -r -skipTrash "$HDFSOUTPATH/trans_mnth=$trans_mnth" >> $LOGFILE 2>&1
    if [[  $? -ne 0  ]]
    then
      scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " Failed to remove HDFS folder $HDFSOUTPATH/trans_mnth=$trans_mnth"
        rm -f $PIDFILE
        exit 1
    else
      scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Successfully removed HDFS partition $HDFSOUTPATH/trans_mnth=$trans_mnth Good to go."
        return 0
    fi
  fi
}

################################################################################
################################################################################
##### Function: RunSoiMonthlyRollup 
RunSoiMonthlyRollup ()
{
  trans_mnth=$1
  DropIfExistsHDFSPartition $trans_mnth
 
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " /usr/bin/pig -Dpig.additional.jars=$PIGGYBANK_JAR -Dexectype=$PIGMODE -useHCatalog -f $PIGSCRIPT -l $LOGDIR \
			   -param source_schema=$HIVE_SCHEMA \
               -param source_table=$SOURCE_TBL \
			   -param trans_month=$trans_mnth \
			   -param hdfs_out_path=$HDFSOUTPATH
			   -param out_delim=$OUTPUT_DELIMITER >>$LOGFILE 2>&1 "
 

	/usr/bin/pig -Dpig.additional.jars=$PIGGYBANK_JAR -Dexectype=$PIGMODE -useHCatalog -f $PIGSCRIPT -l $LOGDIR \
				   -param source_schema=$HIVE_SCHEMA \
				   -param source_table=$SOURCE_TBL \
				   -param trans_month=$trans_mnth \
				   -param hdfs_out_path=$HDFSOUTPATH
				   -param out_delim=$OUTPUT_DELIMITER >>$LOGFILE 2>&1

  if [[ $? -ne 0 ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " Failed to aggregate SOI monthly feed at $HDFSOUTPATH/trans_mnth=$trans_mnth. See log for more details."
	return 1
  else
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Successfully aggregate SOI monthly at $HDFSOUTPATH/trans_mnth=$trans_mnth"
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Metasync Process start -------------"
    hive -e "msck repair table $HIVE_SCHEMA.$TARGETTABLE" >>$LOGFILE 2>&1
    rc=$?
    if [[ $rc -ne 0 ]]
    then
      scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " Metasync Process Failed ------------"
      return 1
   fi 
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Metasync Process end -------------"

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
    scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " OUTPUT path not found at $1. Exiting." 
    exit 1
  else
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " OUTPUT path found at $1  Good to go."
    return 0
  fi
}

################################################################################
################################################################################
##### Function: CoreLogic
CoreLogic()
{
  trans_mnth=$1
 
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Beginning the processing for SOI MONTHLY FEED for $trans_mnth"
  CheckIfExistsHDFSPath $HDFSINPUTPATH/trans_mnth=$trans_mnth
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Checking SOI Monthly feed path in HDFS"
  RunSoiMonthlyRollup $trans_mnth
  rm -f $PIDFILE
  return $?
}


################################################################################
################################################################################
##### Function: Main
Main()
{
  scriptLogger $LOGFILE $PROCESS $$  "[INFO]" " ----- Process START -----"

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


