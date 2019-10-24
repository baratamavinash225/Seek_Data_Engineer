#!/usr/bin/bash
#####################################################################
###Author: Rekha
### Create Date:11/09/2018
### Script: start_stp_daily_score.sh
### Description: warpper script to Execute all daily score jobs
######################################################################
if [[ -z $STPBASE ]]
then
  echo "STPBASE env not set,cnanot proceeed further."
  exit 1
fi
#sourcing STP env variables
. $STPBASE/config/load_stp_config.cfg

dt=`date +"%Y%m%d"`
prevDate=`date -d '-1days' +"%Y%m%d"`
stTime=`date +"%Y%m%d%H%M%S"`
#Global variables creation
CONFIGFILE="$STPBASE/config/stp_job_list.cfg"
LOGDIR="$STPBASE/logs/"
LOGFILE="$LOGDIR/start_stp.$dt.log"
PIDFILE="$STPBASE/permit/start_stp.pid"
CTLFILE="$STPBASE/permit/stp_fail_job_list.ctl"
PROCESS=`basename $0`
MAIL_LIST=$STP_SUPPORT_TEAM_MAIL_DISTRO
SEND_MAIL=$STP_SUPPORT_TEAM_SEND_MAIL

METRICSPATH="$STPBASE/metrics"
mkdir -p $METRICSPATH >> $LOGFILE

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
#############################################################################
####### Function:mail_failure_job
#############################################################################
mail_failure_job()
{
  job=`basename $1`
  log=$2
  mailx -s "$job Failed for ${dt}" -a $log $MAIL_LIST </dev/null
  if [[ $? -eq 0 ]]
  then
    scriptlogger $LOGFILE $PROCESS $$ "[INFO] Mail sent successfully..."
  else
    scriptlogger $LOGFILE $PROCESS $$ "[ERROR] Failed to send mail"
  fi
}
#############################################################################
####### Function:core logic
#############################################################################
core_logic()
{
   jobStatus=""
   file=$1
   file_list=`cat $1`
   for line in $file_list
   do
     job=`echo $line|awk -F"|" '{print $1}'`
     log=`echo $line|awk -F"|" '{print $2}'`
     logFlag=`echo $line|awk -F"|" '{print $3}'`
     log_req=`echo $line|awk -F"|" '{print $3}'|awk -F"=" '{print $2}'`
     lg=""
     res=""
     if [[ $jobStatus == "Fail" ]]
     then
       scriptlogger $LOGFILE $PROCESS $$ "[INFO] updating control file with jobs unable to run"
       echo $job"|"$log"|"$logFlag >> $CTLFILE
     else
       scriptlogger $LOGFILE $PROCESS $$ "[INFO] Executing `basename $0` process"
     #  startTime=`date +"%Y%m%d %H:%M:%S"`
     #  start_epochTime=`date +"%s" --date "$startTime"`
       if [[ $log_req == "Y" ]]
       then
         logFile=`eval touch $log;eval ls $log`
         logext=`echo $logFile|awk -F"." '{print $NF}'`
         errFile=`echo $logFile|sed  "s/$logext$/err/"`
         eval $job > $logFile 2> $errFile
         res=$?
         lg=$errFile
       else
         eval $job >> $LOGFILE
         res=$?
         lg=`eval ls $log`
       fi
    #   endTime=`date +"%Y%m%d %H:%M:%S"`
     #  end_epochTime=`date +"%s" --date "$endTime"`
     #  diff=`expr $end_epochTime - $start_epochTime`
       if [[ $res -ne 0 ]]
       then
         scriptlogger $LOGFILE $PROCESS $$ "[ERROR] Failed to execute $job"
         scriptlogger $LOGFILE $PROCESS $$ "[INFO] updating control file with failed job"
         echo $job"|"$log"|"$logFlag > $CTLFILE
         if [[ $SEND_MAIL == "Y" ]]
         then
           scriptlogger $LOGFILE $PROCESS $$ "[INFO] Sending mail on $job failure"
           jb=`eval ls $job`
           mail_failure_job $jb $lg
         fi
         jobStatus="Fail"
       else
         scriptlogger $LOGFILE $PROCESS $$ "[INFO] Successfully executed $job"
         jobStatus="Success"
       fi
    #   scriptlogger $LOGFILE $PROCESS $$ "[INFO] writting $job metrics"
    #   echo "$(basename $job)|$dt|$startTime|$endTime|$diff|$jobStatus" >> $METRICSPATH/stp_daily_score.$stTime.stats
     fi
   done
   if [[ $jobStatus == "Success" && $file == "$CTLFILE" ]]
   then
     scriptlogger $LOGFILE $PROCESS $$ "[INFO] All STP jobs ran successfully"
     scriptlogger $LOGFILE $PROCESS $$ "[INFO] Removing control file"
     rm -rf $CTLFILE
   fi

}
##################################################################################
##################################################################################
##### Function: Main
###################################################################################
main()
{
  scriptlogger $LOGFILE $PROCESS $$ "[INFO] ----- Process START -----"
  InstanceCheck
  ipFile=""
  if [[ -s $CTLFILE ]]
  then
    scriptlogger $LOGFILE $PROCESS $$ "[INFO] Restarting process from previous run failure job"
    ipFile=$CTLFILE
  else
    scriptlogger $LOGFILE $PROCESS $$ "[INFO] Invoking STP score daily job"
    ipFile=$CONFIGFILE
  fi
  core_logic $ipFile
  scriptlogger $LOGFILE $PROCESS $$ "[INFO] ----- Process End ------"
}
#################################################################################
#################################################################################
#################################################################################
#################################################################################

main
