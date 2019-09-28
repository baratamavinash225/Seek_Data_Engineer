#!/bin/sh

# Author    : Akhilesh Varma
# Purpose   : Used to aggregate SOI monthly data and store into hdfs path and to create hive table
# Usage     : ./stp_lte_sdr_agg_monthly.sh 201906

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
OUTPUT_DELIMITER=$STP_MONTHLY_SOI_ENB_FEED_DELIM
SFTPUSER=$STP_RTT_EXTRACT_SOI_DELTA_USER_ID
SFTPDESTINATIONFOLDER=$STP_RTT_EXTRACT_SOI_DELTA_DESTINATION_DIRECTORY
SFTPDESTINATIONSERVER=$STP_RTT_EXTRACT_SOI_DELTA_SERVER_NAME
SOURCEFILE=$STP_MONTHLY_SOI_ENB_EXTRACT
SFTPLOCAL=$STP_MONTHLY_SOI_ENB_EXTRACT_FOLDER


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
  if echo $1 | egrep -q '^[0-9]{4}-[0-9]{2}$'
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
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " No arguments passed, calculating the soi month first and last dates based on current date by a month latency"
	previousYearMonth=$(date -d "`date +%Y%m01` -1 month" +%Y-%m)
	previousMonthFirstDay=$(date -d "`date +%Y%m01` -1 month" +%Y-%m-%d)
	previousMonthLastDay=$(date -d "`date +%Y%m01` -1 day" +%Y-%m-%d)
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Process starting for $previousYearMonth"
    CoreLogic $previousYearMonth $previousMonthFirstDay $previousMonthLastDay
  elif [[ $# -eq 1 ]]
  then
    #Only date is given
    monthYearToRun=$1
	previousMonthFirstDay=`echo $monthYearToRun"-01"`
	#previousMonthLastDay=$(date -d "`date +%Y%m01` -1 day" +%Y-%m-%d)
	previousMonthLastDay=$(date -d "`date +$previousMonthFirstDay` +1 month -1 day" +%Y-%m-%d)
    validateDateMonth $monthYearToRun
    #Loop for all Hrs in a day
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Only trans_mnth=$monthYearToRun argument is passed, preparing to run for $monthYearToRun"
    CoreLogic $monthYearToRun $previousMonthFirstDay $previousMonthLastDay
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
  first_trans_dt=$2
  last_trans_dt=$3
  DropIfExistsHDFSPartition $trans_mnth

  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "  /usr/bin/pig -Dexectype=$PIGMODE -useHCatalog \
                                   -param source_schema=$HIVE_SCHEMA \
                                   -param source_table=$SOURCE_TBL \
                                   -param trans_mnth=$trans_mnth \
								   -param first_trans_dt=$first_trans_dt \
								   -param last_trans_dt=$last_trans_dt \
                                   -param hdfs_out_path=$HDFSOUTPATH \
                                   -param out_delim=$OUTPUT_DELIMITER \
                                   -f $PIGSCRIPT >>$LOGFILE 2>&1"


                /usr/bin/pig -Dexectype=$PIGMODE -useHCatalog \
                                   -param source_schema=$HIVE_SCHEMA \
                                   -param source_table=$SOURCE_TBL \
                                   -param trans_mnth=$trans_mnth \
								   -param first_trans_dt=$first_trans_dt \
								   -param last_trans_dt=$last_trans_dt \
                                   -param hdfs_out_path=$HDFSOUTPATH \
                                   -param out_delim=$OUTPUT_DELIMITER \
                                   -f $PIGSCRIPT >>$LOGFILE 2>&1

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
##### Function: ExtractAndSftp
ExtractAndSftp()
{
  trans_mnth=$1
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Started extracting file from hdfs to local"

  file_name=`echo ${SOURCEFILE/mmyyyy/$trans_mnth}`

  rm -f $SFTPLOCAL/$file_name
  hadoop fs -rm -f $file_name

  hadoop fs -text $HDFSOUTPATH/trans_mnth=$trans_mnth/* | hadoop fs -put -f - $file_name
  if [[ $? -ne 0 ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " Failed extracting file from hdfs to hdfs"
        return 1
  else

  hadoop fs -copyToLocal $file_name $SFTPLOCAL/$file_name
  if [[ $? -ne 0 ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " Failed extracting file from hdfs to local"
        return 1
  fi
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Successfully extracted file from hdfs to local"
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Started sending extracted file to the destination server via sftp"

  sftp $SFTPUSER@$SFTPDESTINATIONSERVER:$SFTPDESTINATIONFOLDER $SFTPLOCAL/$file_name
  if [[ $? -ne 0 ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " Failed to transfer files through sftp"
        return 1
  fi
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Successfully sent files to destination server via sftp"
    return 0
        fi
}



################################################################################
################################################################################
##### Function: CoreLogic
CoreLogic()
{
  trans_mnth=$1
  first_trans_dt=$2
  last_trans_dt=$3

  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Beginning the processing for SOI MONTHLY FEED for $trans_mnth"
  #CheckIfExistsHDFSPath $HDFSINPUTPATH/trans_mnth=$trans_mnth
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Checking SOI Monthly feed path in HDFS"
  RunSoiMonthlyRollup $trans_mnth $first_trans_dt $last_trans_dt
  if [[ $? -eq 0 ]]
  then
	scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Started sftp"
	ExtractAndSftp $trans_mnth
  fi
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
  mkdir -p $SFTPLOCAL
  WritePIDFile

  ValidateArgs $@
  rm -rf $PIDFILE >>$LOGFILE 2>&1
  scriptLogger $LOGFILE $PROCESS $$  "[INFO]" " ----- Process ENDS -----"
}

#################################################################################
#################################################################################
#################################################################################
#################################################################################

Main $@
