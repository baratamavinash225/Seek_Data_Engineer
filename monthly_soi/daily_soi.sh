#!/usr/bin/ksh
################################################################################
################################################################################
##### Author: Sarvani A
#####    This script is using to load the LTE-SDR daily aggregated data to HDFS
################################################################################
################################################################################
if [[ -z $STPBASE ]]
then
  echo "STPBASE not set, cannot proceed further."
  exit 1
fi

. $STPBASE/config/load_stp_config.cfg

### Global Variables for this script
PROCESS=$(basename ${0%.*})
LOGFILE=$STP_LTE_SDR_AGG_DAILY_LOG_FILE
PIDFILE=$STP_LTE_SDR_AGG_DAILY_PID_FILE
LOGDIR=$STP_LTE_SDR_AGG_DAILY_LOG_DIR
PIGMODE=$STP_LTE_SDR_AGG_DAILY_PIG_MODE
PIGSCRIPT=$STP_LTE_SDR_AGG_DAILY_PIG
LTE_SCHEMA=$STP_LTE_SDR_AGG_DAILY_LTE_HIVEDB
LTETBL=$STP_LTE_SDR_AGG_DAILY_INPUT_HIVE_TABLE
SUB_SCHEMA=$STP_LTE_SDR_AGG_DAILY_SUB_HIVEDB
SUBTBL=$STP_LTE_SDR_AGG_DAILY_SUB_HIVE_TABLE
HDFSOUTDIR=$STP_LTE_SDR_AGG_DAILY_HDFS_OUT_PATH
HDFS_INVALID_MDN_OUTDIR=$STP_LTE_SDR_AGG_DAILY_INVALID_MDN_HDFS_OUT_PATH
HDFS_INVALID_ENB_OUTDIR=$STP_LTE_SDR_AGG_DAILY_INVALID_ENB_HDFS_OUT_PATH
TARGET_SCHEMA=$STP_LTE_SDR_AGG_DAILY_TARGET_HIVEDB
OUTTBL=$STP_LTE_SDR_AGG_DAILY_TARGET_HIVE_TABLE
CTLFILE=$STP_LTE_SDR_AGG_DAILY_CTL_FILE
LATENCYMINS=$STP_LTE_SDR_AGG_DAILY_LATENCY_MIN
SOURCEFILE=$STP_DAILY_SOI_ENB_EXTRACT
SFTPLOCAL=$STP_DAILY_SOI_ENB_EXTRACT_FOLDER
HEADERVARIABLE=$STP_DAILY_SOI_ENB_FEED_HEADER
SFTPUSER=tekuvi
#SFTPUSER=$STP_RTT_EXTRACT_SOI_DELTA_USER_ID
SFTPDESTINATIONFOLDER=/data04/npidev/vteku
#SFTPDESTINATIONFOLDER=$STP_RTT_EXTRACT_SOI_DELTA_DESTINATION_DIRECTORY
SFTPDESTINATIONSERVER=ey9omprna021.vzbi.com
#SFTPDESTINATIONSERVER=$STP_RTT_EXTRACT_SOI_DELTA_SERVER_NAME

################################################################################
################################################################################
##### Function: Usage
Usage()
{
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "./stp_lte_sdr_agg_daily.sh $rundate in <yyyy-MM-dd>"
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "                        OR                         "
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "./stp_lte_sdr_agg_daily.sh                         "
  ProcessEnd
}
################################################################################
################################################################################
##### Function: ProcessEnd
ProcessEnd()
{
  rm -f $PIDFILE
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Exiting from Process..."
  exit 1
}
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
        scriptLogger $LOGFILE $PROCESS $$ "[WARN]" " *** $(basename $0) Already running:\n $(ps -fp $pid)"
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
##### Function: DropIfExistsHDFSPartition
DropIfExistsHDFSPartition()
{
  input_dir=$1
  hadoop fs -test -d "$input_dir" >> $LOGFILE 2>&1

  if [[ $? -ne 0 ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "NO prior partition found $input_dir. Good to go."
        return 0
  else
    hadoop fs -rm -r -skipTrash "$input_dir" >> $LOGFILE 2>&1
    if [[  $? -ne 0  ]]
    then
      scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" "Failed to remove HDFS folder $input_dir"
      ProcessEnd
    else
      scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Successfully removed HDFS partition $input_dir. Good to go."
      return 0
    fi
  fi
}

################################################################################
################################################################################
##### Function: aggDaily
aggDaily()
{
  tdate=$1
  input_date=$1
  input_date=${input_date//-}

  DropIfExistsHDFSPartition $HDFSOUTDIR/trans_dt=$tdate
  DropIfExistsHDFSPartition $HDFS_INVALID_MDN_OUTDIR/trans_dt=$tdate
  DropIfExistsHDFSPartition $HDFS_INVALID_ENB_OUTDIR/trans_dt=$tdate

  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "/usr/bin/pig -Dpig.additional.jars=$PIGGYBANK_JAR -Dexectype=$PIGMODE -useHCatalog -f $PIGSCRIPT -l $LOGDIR \
               -param LTE_SCHEMA=$LTE_SCHEMA \
               -param LTETBL=$LTETBL \
               -param SUB_SCHEMA=$SUB_SCHEMA \
               -param SUBTBL=$SUBTBL \
               -param HDFSOUTDIR=$HDFSOUTDIR/trans_dt=$tdate \
               -param HDFS_INVALID_MDN_OUTDIR=$HDFS_INVALID_MDN_OUTDIR/trans_dt=$tdate \
               -param HDFS_INVALID_ENB_OUTDIR=$HDFS_INVALID_ENB_OUTDIR/trans_dt=$tdate \
               -param TRANS_DT=$input_date >>$LOGFILE 2>&1 "

  /usr/bin/pig -Dpig.additional.jars=$PIGGYBANK_JAR -Dexectype=$PIGMODE -useHCatalog -f $PIGSCRIPT -l $LOGDIR \
               -param LTE_SCHEMA=$LTE_SCHEMA \
               -param LTETBL=$LTETBL \
               -param SUB_SCHEMA=$SUB_SCHEMA \
               -param SUBTBL=$SUBTBL \
               -param HDFSOUTDIR=$HDFSOUTDIR/trans_dt=$tdate \
               -param HDFS_INVALID_MDN_OUTDIR=$HDFS_INVALID_MDN_OUTDIR/trans_dt=$tdate \
               -param HDFS_INVALID_ENB_OUTDIR=$HDFS_INVALID_ENB_OUTDIR/trans_dt=$tdate \
               -param TRANS_DT=$input_date >>$LOGFILE 2>&1

  if [[ $? -ne 0 ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" "Failed to aggreagte daily LTE-SDR at $HDFSOUTDIR/trans_dt=$tdate. See log for more details."
    ProcessEnd
  else
    fileSize=`hadoop fs -du -s $HDFSOUTDIR/trans_dt=$tdate/part* | awk '{print $1}'`
    if [[ ( "$fileSize" -eq 8 ) || ( "$fileSize" -eq 12 ) ]]
    then
      scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" "Nothing aggregated in pig for $tdate. May be no input. Please Verify."
      ProcessEnd
    fi
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Successfully aggregated daily LTE-SDR at $HDFSOUTDIR/trans_dt=$tdate."
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Successfully listed bad MDNs to $HDFS_INVALID_MDN_OUTDIR/trans_dt=$tdate."
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Successfully listed bad ENBs to $HDFS_INVALID_ENB_OUTDIR/trans_dt=$tdate."
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Starting MetaSync for $TARGET_SCHEMA.$OUTTBL"
    hive -e "msck repair table $TARGET_SCHEMA.$OUTTBL" >>$LOGFILE 2>&1
    if [[ $? -ne 0 ]]
    then
      scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" "Metasync process failed for $TARGET_SCHEMA.$OUTTBL for $tdate."
      ProcessEnd
    fi
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Successful metasync process for $TARGET_SCHEMA.$OUTTBL for $tdate."

    if [[ $flagToWriteToCtlFile -eq 1 ]]
    then
      WriteToCtlFile $tdate
    fi
    return 0
  fi
}
################################################################################
################################################################################
##### Function: WriteToCtlFile
WriteToCtlFile()
{
  cur_dt="$1 00:00:00"
  cur_ts="$(date -d "$cur_dt" '+%s')"
  write_timestamp=$(($cur_ts+24*60*60))
  write_dt=`date -d "@$write_timestamp" '+%Y-%m-%d'`
  echo $write_dt > $CTLFILE
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Successfully written next run date $write_dt to $CTLFILE"
  return 0
}
################################################################################
################################################################################
##### Function: checkIfInvalidTime
checkIfInvalidTime()
{
  latency=$LATENCYMINS
  curdate=`date -d "-$latency minutes" "+%s"`
  prevDate=`cat $CTLFILE`" 00:00:00"
  prevtimestamp="$(date -d "$prevDate" '+%s')"
  if [[ $prevtimestamp -ge $curdate ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[WARN]" "Process runtime ($prevtimestamp) is >= process last run time ($curdate). Exiting..."
    ProcessEnd
  else
    return 0
  fi
}

################################################################################
################################################################################
##### Function: CoreLogic
CoreLogic()
{
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Beginning processing LTE_SDR daily agg for $run_Date"

  aggDaily $run_Date

  rm -f $PIDFILE >>$LOGFILE 2>&1

  return $?
}


################################################################################
################################################################################
##### Function: ExtractAndSftp
ExtractAndSftp()
{
  #$run_Date
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Started extracting file from hdfs to local"

  file_name=`echo ${SOURCEFILE/mmmm-dd-yy/$run_Date}`

  rm -f $SFTPLOCAL/$file_name
  hadoop fs -rm -f $file_name

  hadoop fs -text $HDFSOUTDIR/trans_dt=$run_Date/* | hadoop fs -put -f - $file_name
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
  
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Adding header to the file"
  headerValue=$HEADERVARIABLE
  sed -i "1s/^/$headerValue/" $SFTPLOCAL/$file_name
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Successfully added header to the file"
  
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Started sending extracted file to the destination server via sftp"
chmod 755 -R $SFTPLOCAL/$file_name
sftp  $SFTPUSER@$SFTPDESTINATIONSERVER <<EOF
    put $SFTPLOCAL/$file_name $SFTPDESTINATIONFOLDER
EOF
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


################################################################################
################################################################################
##### Function: ValidateArgs
ValidateArgs()
{
  if [[ $# -eq 0 ]]
  then
    #Check for last run dt and consider load_date as last run dt + 1
    if [[ -s $CTLFILE ]]
    then
      to_run_time=`cat $CTLFILE`" 00:00:00"
      toruntimestamp="$(date -d "$to_run_time" '+%s')"
      currentLoadInterval=$(($toruntimestamp))
      run_Date=`date -d"@$currentLoadInterval" '+%Y-%m-%d'`
      checkIfInvalidTime
    else
      #Take latency mins config and calculate day
      latency=$LATENCYMINS
      curtimestamp=`date -d "-$latency minutes" "+%s"`
      run_Date=`date -d"@$curtimestamp" '+%Y-%m-%d'`
    fi
    flagToWriteToCtlFile=1
  elif [[ $# -eq 1 ]]
  then
    run_Date=$1
    flagToWriteToCtlFile=1
  else
    Usage
  fi
}

################################################################################
################################################################################
##### Function: Main
Main()
{
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "----- Process START -----"

  InstanceCheck

  WritePIDFile

  ValidateArgs $@

  CoreLogic $@
  
  ExtractAndSftp $@
  
}

#################################################################################
#################################################################################
#################################################################################
#################################################################################

Main $@
