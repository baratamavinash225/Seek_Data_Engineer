#!/bin/bash

##########################################################################################
#
#
#  Description : Script load STP Event History table using all events
#  Developer   : Sarvani A
#  Usage       : stp_rtt_load_customer_event_history_snapshot.sh
#                Debug Mode: sh -x stp_rtt_load_customer_event_history_snapshot.sh
#
#
##########################################################################################


PROCESS=$(basename ${0%.*})
DATE=`date +"%Y%m%d"`

if [[ -z $STPBASE ]]
then
  echo "STPBASE not set cannot proceed further"
  exit 1
fi

if [[ -f $STPBASE/config/load_stp_config.cfg ]]
then
. $STPBASE/config/load_stp_config.cfg
else
  echo "Config file not found cannot proceed further"
  exit 1
fi

LOGDIR=$STP_RTT_LOAD_CUSTOMER_EVENT_HISTORY_LOG_DIR
LOGFILE=$STP_RTT_LOAD_CUSTOMER_EVENT_HISTORY_LOG_FILE
PIDFILE=$STP_RTT_LOAD_CUSTOMER_EVENT_HISTORY_PID_FILE
SCRIPTDIR=$STP_RTT_LOAD_CUSTOMER_EVENT_HISTORY_SCRIPT_DIR
DruidToggle=$STP_RTT_CUSTOMER_EVENT_HISTORY_DRUID_TOGGLE

mkdir -p $LOGDIR/

#check for running instance
if [[ -a $PIDFILE ]]
then
  pid=$(cat $PIDFILE 2>>$LOGFILE)
  ps -o args= -p $pid | grep $PROCESS > /dev/null 2>&1
  if [[ $? == 0 ]]
  then
    if [[ -t 0 ]]
    then
      echo "*** $(basename $0) is already running ***"
      ps -fp $pid
    else
      scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " *** $(basename $0) Already running:\n $(ps -fp $pid)"
    fi
    exit 1
  fi
fi

echo $$ > $PIDFILE 2>>$LOGFILE
scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " ----- Process START -----"

tod_partition=`date +"%Y-%m-%d"`
last_partition=`date -d "-$STP_RTT_LOAD_CUSTOMER_EVENT_HISTORY_PRUNE_DAYS days" +"%Y-%m-%d"`

scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Hive Parameters DB=$STPSCHEMA Today=$tod_partition last=$last_partition PruneDays=$STP_RTT_LOAD_CUSTOMER_EVENT_HISTORY_PRUNE_DAYS $STP_RTT_LOAD_CUSTOMER_EVENT_HISTORY_HIVEFILTER."

/usr/bin/hive -hiveconf "DATABASE_NAME"="$STPSCHEMA" -hiveconf "CURRENT_PART"="$tod_partition" \
-hiveconf "LAST_PART"="$last_partition" \
-hiveconf "FILTER"="$STP_RTT_LOAD_CUSTOMER_EVENT_HISTORY_HIVEFILTER" \
-f $STP_RTT_LOAD_CUSTOMER_EVENT_HISTORY_HIVE_FILE >>$LOGFILE 2>&1

rc=$?
if [[ $rc -ne 0 ]]
then
  scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " ERROR while loading Events history for $tod_partition."
  rm -f $PIDFILE >>$LOGFILE 2>&1
  exit 1
  else
    scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Events table loaded successfully for : $tod_partition ."
    if [[ $DruidToggle -eq 1 ]]
    then
      $STP_RTT_DRUID_INGESTION_CUSTOMER_EVENT_HISTORY_SCRIPT_FILE $tod_partition >> $LOGFILE 2>&1
      rc=$?
      if [[ $rc -ne 0 ]]
      then
        scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" " ERROR while loading Events history for $tod_partition into Druid."
        cat $STP_RTT_DRUID_INGESTION_CUSTOMER_EVENT_HISTORY_LOGFILE >> $LOGFILE
        rm -f $ >>$LOGFILE 2>&1
        exit 1
      else
        cat $STP_RTT_DRUID_INGESTION_CUSTOMER_EVENT_HISTORY_LOGFILE >> $LOGFILE
        scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Events table loaded successfully for : $tod_partition to Druid."
      fi
    fi
fi
rm -f $PIDFILE >>$LOGFILE 2>&1
scriptLogger $LOGFILE $PROCESS $$ "[INFO]" " Done"
exit $rc
