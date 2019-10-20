#!/usr/bin/ksh
##############################################################################################################################################################
##############################################################################################################################################################
##### Author    : Akhilesh Varma
#####
##### Purpose   : Used to load the RTT VMAS kpi scores data to STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY and  STP_RTT_VMAS_KPI_SCORES_RAW_HOURLY Tables, SAME AREA&DEVICE_HOURLY TABLES and DRUID INGESTION
#####
#####  Usage    : ./stp_rtt_load_vmas_scores_hourly.sh
##############################################################################################################################################################
##############################################################################################################################################################
#set -x
if [[ -z $STPBASE ]]
then
  echo "STPBASE not set, cannot proceed further."
 exit 1
fi
. $STPBASE/config/load_stp_config.cfg

### Global Variables for this script

PROCESS=$(basename ${0%.*})
LOGDIR=$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_LOG_DIR
#LOGFILE=$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_LOG_FILE
#PIDFILE=$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_PID_FILE
PIGSCRIPT=$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_PIG
SRCDIR=$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_HDFS_SOURCE_DIR
TMPDIR=$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_HDFS_TEMP_DIR
FILEPATTERN=$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_FILEPATTERN
WORKDIR=$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_HDFS_WORK_DIR
ARCDIR=$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_HDFS_ARCHIVE
SRCDELIM=$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_SRC_DELIM
OUTDELIM=$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_OUTPUT_DELIM
PIGGYBANK=$PIGGYBANK_JAR
DRUID_INGESTION_HOURLY=$STP_RTT_VMAS_SUMMARY_SUBSCRIBER_AREA_DRUID_INGESTION

DATE=$(date "+%Y%m%d")
curr_time=$(date "+%Y%m%d%H%M%S")
###############################################################################
##########Function:Usage
###############################################################################
Usage()
{
  echo "Usage:$0 $1 Ex:$0(or)"
  echo "(or)"
  echo "$0 <Re-run date>[yyyyMMdd] <Re-run Hour> [hr]"
}

###############################################################################
###############################################################################
##### Function: PrepareEnv - create necessary directories
PrepareEnv()
{
  ## Make the necessary directories.

  hdfs dfs -mkdir -p $WORKDIR 2>/dev/null
#  hdfs dfs -mkdir -p $ARCDIR/$DATE 2>/dev/null
  hdfs dfs -mkdir -p $TMPDIR 2>/dev/null
  mkdir -p $LOGDIR 2>/dev/null
  mkdir -p $STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_PERMIT_DIR 2>/dev/null 
}
###############################################################################
###############################################################################
##### Function: InstanceCheck - to verify that only instance of the processis running
InstanceCheck()
{
  ## Checking that there is no other instance of this script running
  score_date=$1
  score_hour=$2
  if [[ -a $PIDFILE ]]
  then
    ## Now get the pid from the PID file and see if the PID is active, and
    ## relevant to this process.
    pid=$(cat $PIDFILE 2>>$LOGFILE)
    ps -o args= -p $pid | grep $PROCESS |grep $score_date|grep $score_hour> /dev/null 2>&1
    if [[ $? == 0 ]]
    then
      if [[ -t 0 ]]
      then
        echo "*** $(basename $0) is already running ***"
        ps -fp $pid
      else
        scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "*** $(basename $0) Already running:\n $(ps -fp $pid)"
      fi
      ## Duplicate instance. Process stops.
      exit 1
    fi
  fi
}

###############################################################################
###############################################################################
##### Function: WritePIDFile
WritePIDFile()
{
  ## Write the PID to PID file.
  echo $$ > $PIDFILE
}
###############################################################################
###############################################################################
##### Function: Druid_Restart
Druid_Restart ()
{
	if [[ -f $STP_RTT_VMAS_SCORE_HOURLY_TMP_DIR/stp_rtt_vmas_summary_subscriber_area_hourly_druid_failure.$score_date$score_hour.ctl ]]
	then
		reprocess_score_dt=$score_date
		reprocess_score_hr=$score_hour
		$DRUID_INGESTION_HOURLY $reprocess_score_dt $reprocess_score_hr
		if [[ $? -ne 0 ]]; then
			scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" "Error occurred while ingesting vmas summary subscriber area hourly into druid"
			scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Command used: $DRUID_INGESTION_HOURLY $reprocess_score_dt $reprocess_score_hr"
			touch $STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_PERMIT_DIR/stp_rtt_vmas_summary_subscriber_area_hourly_druid_failure.$score_date$score_hour.ctl
			rm -f $PIDFILE >>$LOGFILE 2>&1
			exit 1
		else
			scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "VMAS Summary data successfully ingested into Druid for score date = $reprocess_score_hr and score hour = $reprocess_score_hr"
			rm $STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_PERMIT_DIR/stp_rtt_vmas_summary_subscriber_area_hourly_druid_failure.$score_date$score_hour.ctl
		fi
    fi
}
###############################################################################
###############################################################################
# Drop If parition Exists for RTT VMAS RAW_HOURLY and AGG_HOURLY

DropIfExistsRAWHDFSPartition()
{
        date=$1
        hr=$2
        hdfs dfs -test -d "$STP_RTT_VMAS_KPI_SCORES_RAW_HOURLY_HDFS_OP_PATH/score_dt=$date/score_hr=$hr" >> $LOGFILE 2>&1
        if [[ $? -ne 0 ]]
        then
        scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "No prior partition found "$STP_RTT_VMAS_KPI_SCORES_RAW_HOURLY_HDFS_OP_PATH/score_dt=$date/score_hr=$hr" Good to go."
        return 0
        else
                scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Running RTT VMAS hourly kpi scores for $date and $hr"
                scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Truncating $date$hr previous run data at $STP_RTT_VMAS_KPI_SCORES_RAW_HOURLY_HDFS_OP_PATH/score_dt=$date/score_hr=$hr"
                hdfs dfs -rm -r -skipTrash "$STP_RTT_VMAS_KPI_SCORES_RAW_HOURLY_HDFS_OP_PATH/score_dt=$date/score_hr=$hr" >>$LOGFILE
                /usr/bin/hive -e "ALTER TABLE $STPSCHEMA.$STP_RTT_VMAS_KPI_SCORES_RAW_HOURLY_HIVE_TABLE DROP PARTITION(score_dt='$date',score_hr='$hr')" >>$LOGFILE
                if [[ $? -ne 0 ]]
                then
                                scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" "Failed to clean directory $STP_RTT_VMAS_KPI_SCORES_RAW_HOURLY_HDFS_OP_PATH/score_dt=$date/score_hr=$hr"
                                return 1
                else
                                scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Successfully removed HDFS partition $STP_RTT_VMAS_KPI_SCORES_RAW_HOURLY_HDFS_OP_PATH/score_dt=$date/score_hr=$hr. Good to go."
                                return 0

                fi
        fi
}

DropIfExistsAGGHDFSPartition()
{
        date=$1
        hr=$2
        #echo "$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_HDFS_OP_PATH/score_dt=$date/score_hr=$hr"
        hdfs dfs -test -d "$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_HDFS_OP_PATH/score_dt=$date/score_hr=$hr" >> $LOGFILE 2>&1
        if [[ $? -ne 0 ]]
        then
                scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "No prior partition found "$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_HDFS_OP_PATH/score_dt=$date/score_hr=$hr" Good to go."
        return 0
        else
                scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Running RTT VMAS hourly kpi scores subscriber agg  for $date and $hr"
                scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Truncating $date$hr previous run data at $STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_HDFS_OP_PATH/score_dt=$date/score_hr=$hr"
                hdfs dfs -rm -r -skipTrash "$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_HDFS_OP_PATH/score_dt=$date/score_hr=$hr" >>$LOGFILE
                /usr/bin/hive -e "ALTER TABLE $STPSCHEMA.$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_HIVE_TABLE DROP PARTITION(score_dt='$date',score_hr='$hr')" >>$LOGFILE
                if [[ $? -ne 0 ]]
                then
                                scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" "Failed to clean directory $STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_HDFS_OP_PATH/score_dt=$date/score_hr=$hr"
                                return 1
                else
                                scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Successfully removed HDFS partition $STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_HDFS_OP_PATH/score_dt=$date/score_hr=$hr. Good to go."
                                return 0
                fi
        fi

}


###############################################################################
###############################################################################
## CheckCompressMove

CheckCompressAndMove()
{
        completeFile=$1
        scriptLogger $LOGFILE $PROCESS $$ "[INFO]"  "file is $completeFile"
        base_name=$(basename $completeFile)
        last_extension=${completeFile:${#1}-3:3}
        if [[ $last_extension == ".gz" ]]
        then
                scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "File name has .gz extension..hence moving to $TMPDIR"
                hdfs dfs -mv $completeFile $TMPDIR >>$LOGFILE
                if [[ $? -ne 0 ]]
                then
                        scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" "Failed to move file from $file to $TMPDIR..Please check $TMPDIR"
                        return 1
                fi
                replace_ext=""
                filename_wo_gz=${base_name//$last_extension/$replace_text}
                scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "filename_wo_gz is $filename_wo_gz"
                scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Uncompressing the File and placing in $WORKDIR"
                hadoop fs -cat $TMPDIR/$base_name | gzip -d | hdfs dfs -put - $WORKDIR/$filename_wo_gz  >>$LOGFILE
                if [[ $? -ne 0 ]]
                then
                        scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" "Failed to uncompress ..Please check $TMPDIR"
                        return 1
                fi
        else
                scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "File do not have.gz extension..Hence moving directly to $WORKDIR"
                hdfs dfs -mv $file_name_withpath $WORKDIR >>$LOGFILE
                if [[ $? -ne 0 ]]
                then
                        scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" "Failed to move..Please check $WORKDIR"
                        return 1
                fi

        fi

}


###############################################################################
###############################################################################
## Main processing

CoreLogic()
{
	score_date=$1
        score_hour=$2
	input_file=""
        scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Checking the $SRCDIR for RTT VUE files"
        if [[ $score_date != ""  &&  $score_hour != "" ]]
        then
                scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Beginning the process for $score_date$score_hour instance"
                input_file=$(hdfs dfs -ls $SRCDIR/$FILEPATTERN |grep $score_date$score_hour |awk '{print $8}') >>$LOGFILE
		if [[ $input_file != "" ]]
		then
			scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Moving the File $file from SRC to $WORKDIR"
			CheckCompressAndMove $input_file
			if [[ $? -ne 0 ]]
			then
				scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" "Failed to move file from $file to $WORKDIR..Please check $WORKDIR"
				scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Hence skipping this Iteration.."
				return 1	
			fi
		else
			scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "No Data found at $SRCDIR for $score_date$score_hr"
			input_file=$(hdfs dfs -ls $WORKDIR/${FILEPATTERN%.gz} |grep $score_date$score_hour |awk '{print $8}') >>$LOGFILE
			if [[ $input_file == "" ]]
			then
				scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "No Data found for previous run  at $WORKDIR for $score_date$score_hr"
				return 0
			fi
		fi
		DropIfExistsRAWHDFSPartition $score_date $score_hour
		if [[ $? -eq 1 ]]
		then
			return 1	
		fi
		DropIfExistsAGGHDFSPartition $score_date $score_hour
		if [[ $? -eq 1 ]]
		then
			return 1	
		fi
		#Invoking pig script
		scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "/usr/bin/pig -Dexectype=tez -Dpig.additional.jars=$PIGGYBANK -useHCatalog  -f $PIGSCRIPT -param inputpath=$WORKDIR/*$score_date$score_hour* -param src_delim=$SRCDELIM -param out_delim=$OUTDELIM -param stp_rtt_vmas_kpi_scores_subscriber_agg_hourly_hdfs_path=$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_HDFS_OP_PATH -param stp_rtt_vmas_kpi_scores_raw_hourly_hdfs_path=$STP_RTT_VMAS_KPI_SCORES_RAW_HOURLY_HDFS_OP_PATH -param score_dt=$score_date -param score_hr=$score_hour >>$LOGFILE 2>&1"

               /usr/bin/pig -Dexectype=tez -Dpig.additional.jars=$PIGGYBANK -useHCatalog  -f $PIGSCRIPT -param inputpath=$WORKDIR/*$score_date$score_hour* -param src_delim=$SRCDELIM -param out_delim=$OUTDELIM -param stp_rtt_vmas_kpi_scores_subscriber_agg_hourly_hdfs_path=$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_HDFS_OP_PATH -param stp_rtt_vmas_kpi_scores_raw_hourly_hdfs_path=$STP_RTT_VMAS_KPI_SCORES_RAW_HOURLY_HDFS_OP_PATH -param score_dt=$score_date -param score_hr=$score_hour >>$LOGFILE 2>&1
               if [[ $? -eq 0 ]]
               then
			scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Successfully loaded RTT VMAS hourly KPI scores raw and subscriber_id level agg data."
			scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Moving input file to archive folder"
			hdfs dfs -mkdir -p $ARCDIR/$score_date >>$LOGFILE
			hdfs dfs -test -e $ARCDIR/$score_date/*$score_date$score_hour*
			if [[ $? -eq 0 ]]
			then
				ipfile=$(basename ${input_file%.gz})
				hdfs dfs -mv $WORKDIR/*$score_date$score_hour* $ARCDIR/$score_date/$ipfile"_"$curr_time >>$LOGFILE
			else
				hdfs dfs -mv $WORKDIR/*$score_date$score_hour* $ARCDIR/$score_date >>$LOGFILE                                
				if [[ $? -ne 0 ]]
				then
					scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" "Failed to move files from $WORKDIR to $ARCDIR/$DATE"
					return 1
				else
					scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Successfully moved files from $WORKDIR to $ARCDIR/$DATE"
				fi
			fi
			scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Removing files from temp directory"
			hdfs dfs -test -e $TMPDIR/*$score_date$score_hour*
			if [[ $? -eq 0 ]]
			then
				hadoop fs -rm -skipTrash $TMPDIR/*$score_date$score_hour* >>$LOGFILE
				if [[ $? -ne 0 ]]
				then
					scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" "Failed to remove files from $TMPDIR"
				else
					scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Successfully Removed files from $TMPDIR"
				fi
			else
				scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "No files found in $TMPDIR..Nothing to do.."
			fi
			scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Process start for same area same device hourly for $score_date $score_hour"					
			$STP_RTT_LOAD_SUBSCRIBER_VMAS_SAME_AREA_DEVICE_WRAPPER_HOURLY $score_date $score_hour

			if [[ $? -ne 0 ]]
			then
				scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" "Error in loading Same area device scores for $score_date $score_hour"
				scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Please check log: $LOAD_STP_RTT_SAME_AREA_DEVICE_HOURLY_LOG_DIR/stp_rtt_load_same_area_device_hourly.$score_date$score_hour.$Date.log"
			else
				scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Same area device wrapper has completed successfully for $score_date $score_hour"
			fi
			if [[ $STP_RTT_LOAD_SUBSCRIBER_SCORES_HOURLY_SUMMARY_DRUID_TOGGLE -eq 1 ]]
			then
				scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Ingesting STP RTT vmas  summary data into Druid for $score_date $score_hour"

				$DRUID_INGESTION_HOURLY $score_date $score_hour
				if [[ $? -ne 0 ]]; 
				then
					scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" "Error occurred while ingesting scores data into druid for $score_date $score_hour"
					touch $STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_PERMIT_DIR/stp_rtt_vmas_summary_subscriber_area_hourly_druid_failure.$score_date$score_hour.ctl
					return 1
				else
					scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "VMAS subscriber area data successfully ingested into Druid for $score_date $score_hour"
				fi

			fi
		else
			scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" "Failed to load RTT VMAS hourly KPI scores raw and subscriber_id level agg data."
		fi
	else
		scriptLogger $LOGFILE $PROCESS $$ "[ERROR]" "Please pass required score_date and score_hour arguments....exit"
		return 1
        fi
        # Check if you have any files in work to call the pig script.
 }

###############################################################################
###############################################################################
##### Function: Main
Main()
{
	if [[ $# -eq 2 ]]
	then
		if echo $1 | egrep -q '^[0-9]{4}[0-9]{2}[0-9]{2}$' && echo $2 | egrep -q '^[0-9]{2}$'
		then
        		PrepareEnv
			LOGFILE=$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_HOURLY_LOG_DIR/$PROCESS.$1$2.$Date.log
			PIDFILE=$STP_RTT_VMAS_KPI_SCORES_SUBSCRIBER_AGG_PERMIT_DIR/$PROCESS.$1$2.pid
			scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "----- Process START -----"
			scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "Checking for Druid ingestion failures"
        		InstanceCheck $1 $2
			WritePIDFile
        		Druid_Restart $1 $2
			CoreLogic $1 $2
			rm -f $PIDFILE >>$LOGFILE 2>&1
        		scriptLogger $LOGFILE $PROCESS $$ "[INFO]" "----- Process END -----"
		else
			Usage
		fi
	else
		Usage
	fi
	return 0
}

################################################################################
################################################################################
################################################################################
################################################################################
Main $1 $2
