#!/bin/bash
##########################################################################################
#
#  Description : Script load STP Event History table using all 3 type of events
#  Developer   : Brahmanand Singh
#  Usage       : stp_event_history_snapshot.sh to
#                Debug Mode: sh -x stp_event_history_snapshot.sh
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

LOGDIR=$STPBASE/logs/$PROCESS
LOGFILE=$LOGDIR/$PROCESS.$DATE.log
PIDFILE=$STPBASE/permit/$PROCESS.pid
SCRIPTDIR=$STPBASE/scripts
DruidToggle=$STP_EVENT_HISTORY_DRUID_TOGGLE

scriptlogger()
{
DT=`date +"%Y/%m/%d %H:%M:%S"`
echo "[$2][$3][$DT] $4" >> $1
}

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
      scriptlogger $LOGFILE $PROCESS $$ "*** $(basename $0) Already running:\n $(ps -fp $pid)"
    fi
    exit 1
  fi
fi

echo $$ > $PIDFILE 2>>$LOGFILE
scriptlogger $LOGFILE $PROCESS $$ "----- Process START -----"

tod_partition=`date +"%Y-%m-%d"`
last_partition=`date -d "-$LOAD_STP_CUSTOMER_EVENT_HISTORY_PRUNE_DAYS days" +"%Y-%m-%d"`

scriptlogger $LOGFILE $PROCESS $$ "Hive Parameters DB=$HIVEDB Today=$tod_partition last=$last_partition PruneDays=$LOAD_STP_CUSTOMER_EVENT_HISTORY_PRUNE_DAYS $LOAD_STP_CUSTOMER_EVENT_HISTORY_HIVEFILTER."

/usr/bin/hive -hiveconf "DATABASE_NAME"="$HIVEDB" -hiveconf "CURRENT_PART"="$tod_partition" \
-hiveconf "LAST_PART"="$last_partition" \
-hiveconf "FILTER"="$LOAD_STP_CUSTOMER_EVENT_HISTORY_HIVEFILTER" \
-f $LOAD_STP_CUSTOMER_EVENT_HISTORY_HIVE_FILE >>$LOGFILE 2>&1

rc=$?
if [[ $rc -ne 0 ]]
then
  scriptlogger $LOGFILE $PROCESS $$ "ERROR while loading Events history for $tod_partition."
  rm -f $PIDFILE >>$LOGFILE 2>&1
  exit 1
  else
    scriptlogger $LOGFILE $PROCESS $$ "Events table loaded successfully for : $tod_partition ."
    if [[ $DruidToggle -eq 1 ]]
    then
      $STP_DRUID_CUSTOMER_EVENT_HISTORY_SCRIPT_FILE $tod_partition >> $LOGFILE 2>&1
      rc=$?
      if [[ $rc -ne 0 ]]
      then
        scriptlogger $LOGFILE $PROCESS $$ "ERROR while loading Events history for $tod_partition into Druid."
        cat $STP_DRUID_CUSTOMER_EVENT_HISTORY_LOGFILE >> $LOGFILE
        rm -f $ >>$LOGFILE 2>&1
        exit 1
      else
        cat $STP_DRUID_CUSTOMER_EVENT_HISTORY_LOGFILE >> $LOGFILE
        scriptlogger $LOGFILE $PROCESS $$ "Events table loaded successfully for : $tod_partition to Druid."
      fi
    fi
fi
rm -f $PIDFILE >>$LOGFILE 2>&1
scriptlogger $LOGFILE $PROCESS $$ "Done"
exit $rc
