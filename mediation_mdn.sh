#!/usr/bin/ksh
#########################
##This script loads STP-IVR outage MDNs data from HDFS to hive using Pig
##Author: Sarvani A
#########################

if [[ -z $STPBASE ]]
then
        echo "STPBASE not set, cannot proceed further."
        exit 1
fi

kinit -kt  /usr/apps/SVC-NetOutage/keytabs/SVC-NetOutage.headless.keytab SVC-NetOutage@CARONDLPA.HDP.VZWNET.COM

. $STPBASE/config/load_stp_config.cfg
outages_interm_start_time=$(date "+%Y%m%d%H%M%S")
### Global Variables for this script
PROCESS=$(basename ${0%.*})
LOGFILE=$LOAD_STP_IVR_OUTAGE_MDNS_LOG_FILE
PIGLOG=$LOAD_STP_IVR_OUTAGE_MDNS_PIG_LOG_FILE
PIDFILE=$LOAD_STP_IVR_OUTAGE_MDNS_PID_FILE
LOGDIR=$LOAD_STP_IVR_OUTAGE_MDNS_LOG_DIR
PIGSCRIPT=$LOAD_STP_IVR_OUTAGE_MDNS_PIG
SRCDIR=$LOAD_STP_IVR_OUTAGE_MDNS_HDFS_SOURCE_DIR
FILEPATTERN=$LOAD_STP_IVR_OUTAGE_MDNS_FILEPATTERN
WORKDIR=$LOAD_STP_IVR_OUTAGE_MDNS_HDFS_WORK_DIR
ARCDIR=$LOAD_STP_IVR_OUTAGE_MDNS_HDFS_ARCHIVE
SRCDELIM=$LOAD_STP_IVR_OUTAGE_MDNS_DELIM
MDNSTBL=$LOAD_STP_IVR_OUTAGE_MDNS_HIVE_TABLE
HIVEDB=$LOAD_STP_IVR_OUTAGE_MDNS_TARGET_HIVEDB
HIVEFILE=$LOAD_STP_IVR_OUTAGE_MDNS_HIVE_FILE
PIGGYBANK=$PIGGYBANK_JAR
OUTAGES_INTERM_PHASE=$LOAD_STP_IVR_OUTAGE_MDNS_PHASE
OUTAGES_INTERM_PHASE_DETAIL=$LOAD_STP_IVR_OUTAGE_MDNS_PHASE_DETAILS
OUTAGE_EVENTS_PHASE=$LOAD_STP_IVR_OUTAGE_MDNS_EVENTS_PHASE
OUTAGE_EVENTS_PHASE_DETAIL=$LOAD_STP_IVR_OUTAGE_MDNS_EVENTS_PHASE_DETAILS
mediationrunflag=0

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
                                scriptlogger $LOGFILE $PROCESS $$  "*** $(basename $0) Already running:\n $(ps -fp $pid)"
                        fi
                        ## Duplicate instance, so we have to exit.
                        exit 1
                fi
        fi
}

PrepareEnv()
{
        # Make the necessary directories.
        mkdir -p $LOGDIR
	mkdir -p $STP_LOAD_NETWORK_OUTAGE_STATS_LOG
        mkdir_log=$LOGDIR/$PROCESS.mkdir_.log

        hdfs dfs -test -d $ARCDIR >>/dev/null 2>&1

        if [[ $? -ne 0 ]]
        then
                hdfs dfs -mkdir -p $ARCDIR >> $mkdir_log 2>&1

                if [[ $? -ne 0 ]]
                then
                        scriptlogger $LOGFILE $PROCESS $$ "Failed to create $ARCDIR"
                        cat $mkdir_log >> $LOGFILE
                        rm -f $mkdir_log
                        exit 1
                else
                        scriptlogger $LOGFILE $PROCESS $$ "Created $ARCDIR"
                fi
                rm -f $mkdir_log
        fi

        hdfs dfs -test -d $WORKDIR >>/dev/null 2>&1

        if [[ $? -ne 0 ]]
        then
                hdfs dfs -mkdir -p $WORKDIR >> $mkdir_log 2>&1

                if [[ $? -ne 0 ]]
                then
                        scriptlogger $LOGFILE $PROCESS $$ "Failed to create $WORKDIR"
                        cat $mkdir_log >> $LOGFILE
                        rm -f $mkdir_log
                        exit 1
                else
                        scriptlogger $LOGFILE $PROCESS $$ "Created $WORKDIR"
                fi
            rm -f $mkdir_log
        fi
}
WritePIDFile()
{
    ## Write the PID to PID file.
    echo $$ > $PIDFILE
}
statsGenerator()
{
        scriptlogger $LOGFILE $PROCESS $$ "------ IVR Outage mdns Loader stats Generation ------"
        stats=""
        for outageid in $(cat $STPBASE/logs/ivr_mdns_outage_id_list|sort|uniq)
        do
                if [[ $stats == "" ]]
                then
                        stats="$outageid|$OUTAGES_INTERM_PHASE|$OUTAGES_INTERM_PHASE_DETAIL|$outages_interm_start_time|$outages_interm_end_time"
                else
                        stats+="\n$outageid|$OUTAGES_INTERM_PHASE|$OUTAGES_INTERM_PHASE_DETAIL|$outages_interm_start_time|$outages_interm_end_time"
                fi
                if [[ $LOAD_STP_IVR_OUTAGE_MDNS_MULTIPLE_PHASES == "Y" ]]
                then
                        stats+="\n$outageid|$OUTAGE_EVENTS_PHASE|$OUTAGE_EVENTS_PHASE_DETAIL|$outage_events_start_time|$outage_events_end_time"
                fi
        done
        scriptlogger $LOGFILE $PROCESS $$ "Writting stats to file"
        echo -e $stats > $STP_LOAD_NETWORK_OUTAGE_STATS_LOG/$PROCESS.$outage_events_end_time.stats
        if [[ $? -ne 0 ]]
        then
                scriptlogger $LOGFILE $PROCESS $$ "Failed to write stats"
        else
                scriptlogger $LOGFILE $PROCESS $$ "Successfully written stats to file"
        fi
}

CoreLogic()
{

        scriptlogger $LOGFILE $PROCESS $$ "Beginning the processing for IVR outage MDNs"

        # Set the main flag to false or zero and formulate the partition
        loaded=0
        move_to_archive=0
        partitiondate=`date +%Y%m%d%H%M`
        failed_on_input_file_move=0

        # Check if we have files in the input dir
        # If we do, move them to work dir
        hdfs dfs -ls -u $SRCDIR/$FILEPATTERN* >/dev/null 2>&1
        if [[ $? -ne 0 ]]
        then
                scriptlogger $LOGFILE $PROCESS $$ "No files of pattern $FILEPATTERN found in the input $SRCDIR"
        else
                mv_log=$LOGDIR/$PROCESS.filemove.log
                hdfs dfs -mv $SRCDIR/$FILEPATTERN* $WORKDIR >>$mv_log 2>&1

                if [[ $? -ne 0 ]]
                then
                        scriptlogger $LOGFILE $PROCESS $$ "Failed to move files from $SRCDIR to $WORKDIR"
                        cat $mv_log >> $LOGFILE
                        failed_on_input_file_move=1
                fi
                rm -f $mv_log
        fi

        # Do not proceed to processing if the file move failed.
        if [[ $failed_on_input_file_move -eq 1 ]]
        then
                scriptlogger $LOGFILE $PROCESS $$ "The files have failed to move and this needs investigating. Will not proceed to the processing."
                rm -f $PIDFILE
                return
        fi

        # Check if you have any files in work to call the pig script.
        hdfs dfs -ls -u $WORKDIR/$FILEPATTERN* >/dev/null 2>&1
        if [[ $? -ne 0 ]]
        then
                scriptlogger $LOGFILE $PROCESS $$ "No files found of previous run or this run in $WORKDIR. Will not proceed to the processing."
                return
        else
                scriptlogger $LOGFILE $PROCESS $$ "Found files from previous run."
                scriptlogger $LOGFILE $PROCESS $$ "Starting IVR outage MDNs  $PIGSCRIPT"

                pig_log=$LOGDIR/$PROCESS.pig.log

                # pig -x mapreduce -f ivr_outage_mdns.pig -useHCatalog -param stp_ivr_outage_mdn_dir=/user/sarvaat/TEST/ivr_outage.psv -param stp_db=sarvaat -param stp_ivr_outage_mdn_tbl=ivr_outage_stg
                /usr/bin/pig -Dexectype=tez -Dpig.additional.jars=$PIGGYBANK -useHCatalog -f $PIGSCRIPT -param stp_ivr_outage_mdn_dir=$WORKDIR/$FILEPATTERN -param stp_db=$HIVEDB -param stp_ivr_outage_mdn_tbl=$MDNSTBL -param partitiondate=$partitiondate -l $PIGLOG >>$pig_log 2>&1

                # If failed, append the log
                # else set the flag to true
                if [[ $? -ne 0 ]]
                then
                        scriptlogger $LOGFILE $PROCESS $$ "Failed to load IVR outage MDNs data into the hive table, see the below log for more details."
                else
                        scriptlogger $LOGFILE $PROCESS $$ "Finished loading $TARGET_SCHEMA.$MDNSTBL"
			outages_interm_end_time=$(date "+%Y%m%d%H%M%S")
                        loaded=1
                fi
                 cat $pig_log >> $LOGFILE
                rm -f $pig_log

                # If everything is fine, only then this flag would be true.
                # Move the files from work to archive.
                if [[ $loaded -eq 1 ]]
                then
                       scriptlogger $LOGFILE $PROCESS $$ "Finished loading everything, now going to move files to archive."
                       hdfs dfs -text $WORKDIR/$FILEPATTERN | cut -d'|' -f1 | grep -v OUTAGE_ID|sort|uniq > $STPBASE/logs/ivr_mdns_outage_id_list
                        mv_log=$LOGDIR/$PROCESS.filemove.log
                        hdfs dfs -mv $WORKDIR/$FILEPATTERN $ARCDIR/$DATE >>$mv_log 2>&1

                        if [[ $? -ne 0 ]]
                        then
                                scriptlogger $LOGFILE $PROCESS $$ "Failed to move files from $WORKDIR to $ARCDIR"
                                cat $mv_log >> $LOGFILE
                        else
                                move_to_archive=1
                                scriptlogger $LOGFILE $PROCESS $$ "Successfully moved files from $WORKDIR to $ARCDIR"
                        fi
                fi
                if [[ move_to_archive -eq 1 ]]
                then
                        CallEvents
                fi
                rm -f $mv_log
                rm -f $PIDFILE
        fi
}

CallEvents()
{
	outage_events_start_time=$(date "+%Y%m%d%H%M%S")
	scriptlogger $LOGFILE $PROCESS $$ "Loading Events for IVR outage MDN Start"
	last_part=$(hdfs dfs -ls /apps/hive/warehouse/$HIVEDB.db/stp_ivr_outage_mdns |cut -d'=' -f2 |sort -n|awk 'END{print $NF}') >>$LOGFILE 2>&1

	#get last runtime for IVR outage event
	hivevar=$(/usr/bin/hive -e "SELECT DATE_FORMAT(MAX(LAST_EXECUTION_TIME),'YYYY-MM-dd_HH:mm:ss.SSS') FROM $HIVEDB.STP_EVENT_LOAD_STATUS WHERE JOB_NAME='STP_CUSTOMER_EVENT_HISTORY_IVR_MDN';") >>$LOGFILE 2>&1

	if [[ -z "$hivevar" ]]
	then
		scriptlogger $LOGFILE $PROCESS $$ "Error while getting the last execution time for IVR outage."
		scriptlogger $LOGFILE $PROCESS $$ "Check Hive table $HIVEDB.STP_EVENT_LOAD_STATUS"
		exit 1
	else
		/usr/bin/hive -hiveconf "DATABASE_NAME"="$HIVEDB" -hiveconf "LASTRUNTIME"="$hivevar" -hiveconf "LASTPARTITION"="$last_part" -f $HIVEFILE >>$LOGFILE 2>&1
		rc=$?
		if [[ $rc -ne 0 ]]
		then
			scriptlogger $LOGFILE $PROCESS $$ "ERROR while loading Events for IVR outages."
			rm -f $PIDFILE >>$LOGFILE 2>&1
			exit 1
		else
			scriptlogger $LOGFILE $PROCESS $$ "Events table loaded successfully for IVR Outages."
		fi
	fi
	outage_list=$(/usr/bin/hive -e "SELECT DISTINCT(outage_id) FROM $STPSCHEMA.stp_ivr_outage_mdns WHERE partitiondate=$last_part")
        scriptlogger $LOGFILE $PROCESS $$ "Loading Events for IVR outage MDN End"
	outage_events_end_time=$(date "+%Y%m%d%H%M%S")
	statsGenerator
	#calling hql to insert outage events forr MDN [Starts]#
	loadOutageMdn
	#calling hql to insert outage events forr MDN [Ends]#
}

loadOutageMdn()
{
scriptlogger $LOGFILE $PROCESS $$ "----- Process START -----"
scriptlogger $LOGFILE $PROCESS $$ "----- Getting Outage Ids for which mediation job needs to run -----"
outages=$(/usr/bin/hive -hiveconf DATABASE_NAME=$STPSCHEMA -f $STPBASE/sql/stp_mediation_open_outages_hourly.hql)
rc=$?
if [[ $rc -ne 0 ]]
then
  scriptlogger $LOGFILE $PROCESS $$ "ERROR while getting the Outages, please check the log."
  rm -f $PIDFILE >>$LOGFILE 2>&1
  exit 1
fi

if [[ $outages != "" ]]
then
for outage in $outages
do
running_timestamp=`date +%Y%m%d%H%M%S`
scriptlogger $LOGFILE $PROCESS $$ "-----Currently processing for outageid=$outage with running_timestamp=$running_timestamp"
SUCCESS_THRESHOLD=$STP_MDN_SUCCESS_CONNECT_THRESHOLD

scriptlogger $LOGFILE $PROCESS $$ "-----Calling $LOAD_STP_OUTAGE_MEDIATION_START_HIVE_FILE to derive the outage start time..."
/usr/bin/hive -hiveconf "DATABASE_NAME"="$HIVEDB" -hiveconf "OUTAGEID"="$outage" -f $LOAD_STP_OUTAGE_MEDIATION_START_HIVE_FILE >>$LOGFILE 2>&1
rc=$?
if [[ $rc -ne 0 ]]
then
  scriptlogger $LOGFILE $PROCESS $$ "ERROR while getting the Outage start time, please check the log."
  rm -f $PIDFILE >>$LOGFILE 2>&1
  exit 1
fi

last_partition=$(/usr/bin/hive -e "select max(outage_start_date_hour) FROM $HIVEDB.stp_outage_mediation_log where outage_id=$outage group by outage_id") >>$LOGFILE 2>&1

scriptlogger $LOGFILE $PROCESS $$ "Determined next outage_start_date_hour, last_partition=$last_partition"

current_time_latency=`date "+%Y%m%d%H" -d "-$STP_LOAD_OUTAGE_MEDIATION_NSR_LATENCY_MINS min"`
scriptlogger $LOGFILE $PROCESS $$ "Check on future outages by comparing current_time_latency=$current_time_latency and last_partition=$last_partition"

if [[ $last_partition -le $current_time_latency ]]
then
  scriptlogger $LOGFILE $PROCESS $$ "Current outage is eligible for processing, so proceeding..."
  scriptlogger $LOGFILE $PROCESS $$ "----- Calling mediation process to load events for outageid=$outage and nsr_date=$last_partition ."

  /usr/bin/hive -hiveconf "DATABASE_NAME"="$HIVEDB" -hiveconf "SUCCESS_THRLD"="$SUCCESS_THRESHOLD" \
  -hiveconf "NSRPARTITION"="$last_partition" \
  -hiveconf "OUTAGEID"="$outage" \
  -hiveconf "PROCESS_LOADTIME"="$running_timestamp" \
  -hiveconf "CURRENT_LOADTIME"="$current_time" \
  -f $STP_LOAD_OUTAGE_MEDIATION_MDN_HQL >>$LOGFILE 2>&1

  rc=$?
  if [[ $rc -ne 0 ]]
  then
    scriptlogger $LOGFILE $PROCESS $$ "ERROR while running the mediation process, please check the log."
    rm -f $PIDFILE >>$LOGFILE 2>&1
    exit 1
    else
      ((mediationrunflag++))
#      $mediationrunflag=$mediationrunflag+1
      scriptlogger $LOGFILE $PROCESS $$ "Events table loaded successfully for outageid=$outage ."
      end_time=$(date "+%Y%m%d%H%M%S")
      scriptlogger $LOGFILE $PROCESS $$ "Generating Outage loader stats for outage id $outage"
      stats="$outage|$PHASE|$PHASE_DETAILS|$running_timestamp|$end_time"
      scriptlogger $LOGFILE $PROCESS $$ "Writting stats to file"
      echo $stats > $STP_LOAD_NETWORK_OUTAGE_STATS_LOG/$PROCESS.$end_time.stats
      if [[ $? -ne 0 ]]
      then
      	scriptlogger $LOGFILE $PROCESS $$ "Failed to write stats"
       else
       	 scriptlogger $LOGFILE $PROCESS $$ "Successfully written stats to file"
       fi
  fi
else
  scriptlogger $LOGFILE $PROCESS $$ "Skipping the outageid=$outage as the interval is in future."
fi
done 

if [[ $mediationrunflag -ge 1 ]]
then
scriptlogger $LOGFILE $PROCESS $$ "----- Calling VMB hourly Ingestion for latest load_time=$current_time ."
$STP_OUTAGE_MEDIATION_VMB_INGESTION_SCRIPT $current_time
if [[ $rc -ne 0 ]]
then
  scriptlogger $LOGFILE $PROCESS $$ "ERROR while running the VMB Mediation hourly Process,Please check log."
  rm -f $PIDFILE >>$LOGFILE 2>&1
  exit 1 
else
   scriptlogger $LOGFILE $PROCESS $$ "Successfully Ingested data into VMB."
fi
else
scriptlogger $LOGFILE $PROCESS $$ "----- Skipping VMB Ingestion as no outages has been processed..."
fi
else
 scriptlogger $LOGFILE $PROCESS $$ "----- No Open Outages for mediation job to run -----"
 scriptlogger $LOGFILE $PROCESS $$ "----- Exiting the process. -----"
rm -f $PIDFILE >>$LOGFILE 2>&1
scriptlogger $LOGFILE $PROCESS $$ "Done" 
exit $rc
fi

}

Main()
{
        PrepareEnv
        scriptlogger $LOGFILE $PROCESS $$  "----- Process START -----"
        InstanceCheck
        WritePIDFile
        CoreLogic
        scriptlogger $LOGFILE $PROCESS $$ "----- Process END -----"
}

Main

return 0

