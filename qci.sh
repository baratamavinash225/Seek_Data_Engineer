#!/usr/bin/bash
################################################################################
################################################################################
##### Author: Vishwaraj Bangari
#####    This script is using to load RTT QCI switch events to respective tables
#####    Runs every 15 mins

#####  While running with Spark, dont run on date only or hour only mode
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
PIDFILE=$STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_PID_FILE
LOGDIR=$STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_LOG_DIR
LOGFILE=$STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_LOG_FILE
PIGSCRIPT=$STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_PIG
PIGMODE=$STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_PIG_MODE
SOURCE_SCHEMA=$STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_SOURCE_SCHEMA
TARGET_SCHEMA=$STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_TARGET_HIVEDB
P_TABLE=$STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_INPUT_HIVE_TABLE
QCI_SNAPSHOT_PATH=$STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_HDFS_QCI_SNAPSHOT_OUTPATH
QCI_HISTORY_PATH=$STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_HDFS_QCI_HISTORY_OUTPATH
QCI_SNAPSHOT_TBL=$STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_HIVE_QCI_SNAPSHOT_EXT_OUTTABLE
QCI_HISTORY_TBL=$STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_HIVE_QCI_HISTORY_EXT_OUTTABLE
QCI_EVENT_DESC=$STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_API_DESCRIPTION
SUBSCRIBERTBL=$STP_RTT_KPI_SUMMARY_SUBSCRIBER_PROFILE_TABLE
LATENCYMIN=$STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_LATENCY_MINS
P_TABLE_HDFS_PATH=$STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_P_TABLE_HDFS_PATH
QCI_CTL_FILE=$STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_CTL_FILE
CTL_FLAG=$#
SPARK_CONFIG_FILE=$STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_SPARK_CFG_FILE
################################################################################
################################################################################
##### Function: Usage
Usage()
{
    echo "Unknown arguments $@ passed to the script."
    echo "Usage: stp_load_event_history_snapshot.sh                                                                                          "
    echo "                           OR                                                                                                      "
    echo "Usage: stp_load_event_history_snapshot.sh [trans_dt in yyyy-MM-dd]                                                                 "
    echo "                           OR                                                                                                      "
    echo "Usage: stp_load_event_history_snapshot.sh [trans_dt in yyyy-MM-dd] [trans_hr in HH (00-23)]                                        "
    echo "                           OR                                                                                                      "
    echo "Usage: stp_load_event_history_snapshot.sh [trans_dt in yyyy-MM-dd] [trans_hr in HH (00-23)] [trans_mnt in ss (00 or 15 or 30 or 45)]"
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
##### Function: runAllHours
runAllHours()
{
  target_date=$1
  for hr in {00..23}
  do
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " $target_date - $hr"
    runAllMins $target_date $hr
  done
}
################################################################################
################################################################################
##### Function: runAllMins
runAllMins()
{
  trans_dt=$1
  trans_hr=$2
  for trans_mnt in {00..59..15}
  do
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Process starting for $trans_dt $trans_hr:$trans_mnt"
    CoreLogic $trans_dt $trans_hr $trans_mnt
  done
}
################################################################################
################################################################################
##### Function: checkDate
checkDate()
{
  if [[ $(date "+%Y-%m-%d" -d "$1") == "$1" ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Valid argument. Proceeding to process."
  else
    Usage
  fi
}

################################################################################
################################################################################
##### Function: checkHr
checkHr()
{
  if [[ $(date "+%Y-%m-%d %H" -d "$1 $2") == "$1 $2" ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Valid argument. Proceeding to Process."
  else
    Usage
  fi
}


################################################################################
################################################################################
##### Function: checkMins
checkMins()
{
  if [[ $(date "+%Y-%m-%d %H:%M" -d "$1 $2:$3") == "$1 $2:$3" ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Valid argument. Proceeding to Process."
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
    manualRun=0
    #Take latency mins config and calculate day, hr and nearest 15min
    #check QciCtlFile

    latency=$LATENCYMIN
    curdate=`date -d "-$latency minutes" "+%s"`
    cur_trans_dt=`date -d"@$curdate" '+%Y-%m-%d'`
    cur_trans_hr=`date -d"@$curdate" '+%H'`
    curquarter=$(($curdate - ($curdate % (15 * 60))))
    cur_trans_mnt=`date -d"@$curquarter" '+%M'`
    #if [ -s "$QCI_CTL_FILE" ]
    hadoop fs -test -d "$QCI_CTL_FILE" >> $LOGFILE 2>&1
    if [[ $ -ne 0 ]]
    then
      prevDate=`cat $QCI_CTL_FILE`":00"
      scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Previous successful run at $prevDate"
      prevtimestamp="$(date -d "$prevDate" '+%s')"
      currentLoadInterval=$(($prevtimestamp + 15 * 60))
      trans_dt=`date -d"@$currentLoadInterval" '+%Y-%m-%d'`
      trans_hr=`date -d"@$currentLoadInterval" '+%H'`
      trans_mnt=`date -d"@$currentLoadInterval" '+%M'`

      if [[ $prevtimestamp -gt $curdate ]]
      then
        scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " Timestamp ($prevtimestamp) in $QCI_CTL_FILE is greater than currenttime ($curdate). Exiting."
        exit 0
      fi
      if [[ $trans_dt -eq $cur_trans_dt ]] && [[ $trans_hr -eq $cur_trans_hr  ]] && [[ $trans_mnt -eq $cur_trans_mnt ]]
      then
        scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Already ran for this interval $prevDate. Exiting. "
        exit 0
      else
        scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " $QCI_CTL_FILE created in last run. Taking next interval from $QCI_CTL_FILE"
      fi
    else
      latency=$LATENCYMIN
      trans_dt=$cur_trans_dt
      trans_hr=$cur_trans_hr
      trans_mnt=$cur_trans_mnt
    fi
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Process starting for $trans_dt $trans_hr:$trans_mnt"
    CoreLogic $trans_dt $trans_hr $trans_mnt

  elif [[ $# -eq 1 ]]
  then
    manualRun=0
    #Only date is given
    run_dt=$1
    checkDate $run_dt
    #Loop for all Hrs in a day
    runAllHours $run_dt

  elif [[ $# -eq 2 ]]
  then
    manualRun=0
    #date and hour are given
    run_dt=$1
    run_hr=$2
    checkHr $1 $2
    #Loop for all Hrs in a day
    runAllMins $run_dt $run_hr

  elif [[ $# -eq 3 ]]
  then
    manualRun=1
    checkMins $1 $2 $3
    trans_dt=$1
    trans_hr=$2
    trans_mnt=$3
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Process starting for: $trans_dt $trans_hr:$trans_mnt"
    CoreLogic $trans_dt $trans_hr $trans_mnt
  else
    Usage
  fi
}
################################################################################
################################################################################
##### Function: DropIfExistsHDFSPartition
DropIfExistsHDFSPartition()
{
  hadoop fs -test -d "$1" >> $LOGFILE 2>&1

  if [[ $? -ne 0 ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " NO prior partition found $1. Good to go."
        return 0
  else
    hadoop fs -rm -r -skipTrash "$1" >> $LOGFILE 2>&1
    if [[  $? -ne 0  ]]
    then
      scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " Failed to remove HDFS folder $1"
        rm -f $PIDFILE
        exit 1
    else
      scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Successfully removed HDFS partition $1. Good to go."
    fi
  fi
}

################################################################################
################################################################################
##### Function: DropAllHDFSPartitionsExceptGiven
DropAllHDFSPartitionsExceptGiven()
{

  basePath=$1
  partition=$2
  partitions=`hadoop fs -ls $basePath | sed 1d | perl -wlne'print +(split " ",$_,8)[7]'`
  for name in $partitions
  do
      if [[ $name != *"$partition"* ]]
      then
        hadoop fs -rm -r -skipTrash $name >>$LOGFILE 2>&1
        if [[ $? -ne 0 ]]
        then
          scriptLogger $LOGFILE $PROCESS $$ "[WARN]" " Failed to remove HDFS folder(Previous partition) $name. Continuing to remove others."
        else
          scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Successfully removed HDFS folder(Previous partition) $name."
        fi
      fi
  done
}
################################################################################
################################################################################
##### Function: CallEventQciPigScript
CallEventQciPigScript()
{
  input_date=$1
  tDate=${input_date//-}
  tHr=$2
  tMnt=$3
  part_file_name=`echo "stp_qci_event_history_"$tDate"_"$tHr"_"$tMnt"_"`

  DropIfExistsHDFSPartition $QCI_SNAPSHOT_PATH/$input_date'_tmp'
  DropIfExistsHDFSPartition $QCI_HISTORY_PATH/event_day_tmp=$input_date

  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " /usr/bin/pig -Dpig.additional.jars=$PIGGYBANK_JAR -Dexectype=$PIGMODE
               -Dmapreduce.output.basename=$part_file_name \
               -useHCatalog -t ColumnMapKeyPrune -f $PIGSCRIPT -l $LOGDIR \
               -param source_schema=$SOURCE_SCHEMA \
               -param p_table=$P_TABLE \
               -param history_path=$QCI_HISTORY_PATH/event_day_tmp=$tDate \
               -param snapshot_inpath=$QCI_SNAPSHOT_PATH \
               -param snapshot_path=$QCI_SNAPSHOT_PATH/$input_date'_tmp' \
               -param subscribertbl=$SUBSCRIBERTBL \
               -param transdatetime=$tDate$tHr$tMnt  >>$LOGFILE 2>&1"

  /usr/bin/pig -Dpig.additional.jars=$PIGGYBANK_JAR -Dexectype=$PIGMODE \
               -Dmapreduce.output.basename=$part_file_name \
               -useHCatalog -t ColumnMapKeyPrune -f $PIGSCRIPT -l $LOGDIR \
               -param source_schema=$SOURCE_SCHEMA \
               -param p_table=$P_TABLE \
               -param history_path=$QCI_HISTORY_PATH/event_day_tmp=$tDate \
               -param snapshot_inpath=$QCI_SNAPSHOT_PATH \
               -param snapshot_path=$QCI_SNAPSHOT_PATH/$input_date'_tmp' \
               -param subscribertbl=$SUBSCRIBERTBL \
               -param transdatetime=$tDate$tHr$tMnt  >>$LOGFILE 2>&1


  if [[ $? -ne 0 ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " Failed to generate customer QCI Switch events at $QCI_SNAPSHOT_PATH/$input_date'_tmp' and $QCI_HISTORY_PATH/event_day_tmp=$tDate. See log for more details."
        return 1
  else
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Successfully generated customer QCI Switch events at  $QCI_SNAPSHOT_PATH/$input_date_tmp and $QCI_HISTORY_PATH/event_day_tmp=$tDate. Now moving them to final location"

    DropIfExistsHDFSPartition $QCI_SNAPSHOT_PATH/$input_date
    hdfs dfs -mv $QCI_SNAPSHOT_PATH/$input_date'_tmp' $QCI_SNAPSHOT_PATH/$input_date >>$LOGFILE 2>&1
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Successfully moved $QCI_SNAPSHOT_PATH/$input_date'_tmp' to $QCI_SNAPSHOT_PATH/$input_date".
    #Drop Previous partition
    DropAllHDFSPartitionsExceptGiven $QCI_SNAPSHOT_PATH $input_date
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Sync Hive table $QCI_SNAPSHOT_TBL"
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Metasyn Process start -------------"
    hive -e "msck repair table $SOURCE_SCHEMA.$QCI_SNAPSHOT_TBL" >>$LOGFILE 2>&1
    if [[ $? -ne 0 ]]
    then
    scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " Metasyn Process Failed -------------"
    return 1
    fi
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Metasyn Process end -------------"

    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Successfully loaded customer QCI snapshot to  $QCI_SNAPSHOT_PATH/$input_date"

    if $(hadoop fs -test -d $QCI_HISTORY_PATH/event_day=$tDate) >>$LOGFILE 2>&1
    then
      scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " HDFS Dir $QCI_HISTORY_PATH/event_day=$tDate is existing. Continuing to copy pig output files. ";
    else
      scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " HDFS Dir $QCI_HISTORY_PATH/event_day=$tDate doesn't exist. Creating. ";
      hadoop fs -mkdir $QCI_HISTORY_PATH/event_day=$tDate >>$LOGFILE 2>&1
    fi

    #Remove all previous files on this run
    hdfs dfs -rm $QCI_HISTORY_PATH/event_day=$tDate/stp_qci_event_history_"$tDate"_"$tHr"_"$tMnt"_* >>$LOGFILE 2>&1

    #Move contents of temp dir to final dir
    hdfs dfs -mv $QCI_HISTORY_PATH/event_day_tmp=$tDate/stp_qci_event_history_"$tDate"_"$tHr"_"$tMnt"_* $QCI_HISTORY_PATH/event_day=$tDate/ >>$LOGFILE 2>&1

    #remove temp directory
    hdfs dfs -rm -r $QCI_HISTORY_PATH/event_day_tmp=$tDate >>$LOGFILE 2>&1

    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Successfully moved $QCI_HISTORY_PATH/event_day_tmp=$tDate to $QCI_HISTORY_PATH/event_day=$tDate".

    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Sync Hive table $QCI_HISTORY_TBL"
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Metasyn Process start -------------"
    hive -e "msck repair table $SOURCE_SCHEMA.$QCI_HISTORY_TBL" >>$LOGFILE 2>&1
    if [[ $? -ne 0 ]]
    then
    scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " Metasyn Process Failed -------------"
    return 1
    fi
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Metasyn Process end -------------"

    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Successfully loaded customer QCI History to  $QCI_HISTORY_PATH/event_day=$tDate"
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Process Done. Successfully loaded customer QCI events to $SOURCE_SCHEMA.$QCI_SNAPSHOT_TBL and $SOURCE_SCHEMA.$QCI_HISTORY_TBL hive tables"
    #Adding current successful run timestamp to $QCI_CTL_FILE
        if [[ $CTL_FLAG -eq 0 ]]
        then
                WriteToQciCtlFile $input_date $tHr $tMnt
        else
                scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " As this is manual run no update to control file"
        fi
    return 0

  fi
}

################################################################################
################################################################################
##### Function: CheckIfExistsHDFSPath
CheckIfExistsHDFSPath()
{
  hadoop fs -test -d "$1" >>$LOGFILE 2>&1
  if [[ $? -ne 0 ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " Input path not found at $1. Exiting."
    exit 1
  else
    return 0
  fi
}

################################################################################
################################################################################
##### Function: WriteToQciCtlFile
WriteToQciCtlFile()
{
  write_dt=$1
  write_hr=$2
  write_min=$3
  echo $write_dt $write_hr:$write_min > $QCI_CTL_FILE
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Successfully written timestamp to $write_dt $write_hr:$write_min to $QCI_CTL_FILE"
  return 0
}

################################################################################
################################################################################
##### Function: prepareSparkEnv
PrepareSparkEnv()
{
  sh $STP_RTT_LOAD_QCI_EVENTS_SUBSCRIBER_SPARK_PREP_CFG_FILE $SPARK_CONFIG_FILE >>$LOGFILE 2>&1
  if [[ $? -ne 0 ]]
  then
    scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" "Error converting load_stp_config.cfg to load_stp_config.properties"
    exit 1
  else
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Successful conversion of load_stp_config.cfg to load_stp_config.properties"
    return 0
  fi
}
################################################################################
################################################################################
##### Function: CallEventQciSparkScript
CallEventQciSparkScript()
{

  PrepareSparkEnv

  sDt=$1
  sHr=$2
  sMnt=$3
  if [[ $manualRun -eq 0 ]]
  then
    /usr/hdp/current/spark2-client/bin/spark-submit --files /usr/hdp/current/spark2-client/conf/hive-site.xml  --jars $(echo /usr/hdp/current/spark2-client/jars/*.jar /usr/hdp/2.6.2.0-205/hive2/lib/ojdbc6.jar /data05/CEM/m2/repository/com/typesafe/config/1.3.2/config-1.3.2.jar| tr ' ' ',' ) --class com.verizon.ceam.stpRttSubscriberQciEvents.StpRttLoadSubscriberEventsQci $STP_SPARK_BASE/target/ceamSubscriberEventsQci-1.0.jar --config $SPARK_CONFIG_FILE  >>$LOGFILE 2>&1

    if [[ $? -ne 0 ]]
    then
      scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " Failed to generate QCI EVENTS. See log for more details."
      return 1
    else
      scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Successfully generated QCI Switch events."
    fi
  else
    /usr/hdp/current/spark2-client/bin/spark-submit --files /usr/hdp/current/spark2-client/conf/hive-site.xml  --jars $(echo /usr/hdp/current/spark2-client/jars/*.jar /usr/hdp/2.6.2.0-205/hive2/lib/ojdbc6.jar /data05/CEM/m2/repository/com/typesafe/config/1.3.2/config-1.3.2.jar| tr ' ' ',' ) --class com.verizon.ceam.stpRttSubscriberQciEvents.StpRttLoadSubscriberEventsQci $STP_SPARK_BASE/target/ceamSubscriberEventsQci-1.0.jar --config $SPARK_CONFIG_FILE -m $sDt $sHr $sMnt >>$LOGFILE 2>&1

    if [[ $? -ne 0 ]]
    then
      scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " Failed to generate QCI EVENTS. See log for more details."
      return 1
    else
      scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Successfully generated QCI Switch events."
    fi
  fi
}


################################################################################
################################################################################
##### Function: CoreLogic
CoreLogic()
{
  trans_dt=$1
  trans_hr=$2
  trans_mnt=$3
  idate=${trans_dt//-}
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "  Beginning the processing for QCI switch for $trans_dt $trans_hr:$trans_mnt"
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Checking P_table partition in HDFS"
  CheckIfExistsHDFSPath $P_TABLE_HDFS_PATH/p_date_key=$idate$trans_hr$trans_mnt
  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Checking QCI snapshot path in HDFS"
  CheckIfExistsHDFSPath $QCI_SNAPSHOT_PATH
  #CallEventQciPigScript $trans_dt $trans_hr $trans_mnt
  CallEventQciSparkScript $trans_dt $trans_hr $trans_mnt

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

  scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " ----- Process END -----"
}

#################################################################################
#################################################################################
#################################################################################
#################################################################################

Main $@
rc=$?
exit $rc
