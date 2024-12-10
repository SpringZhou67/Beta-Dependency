#!/bin/bash
STARTDATE_TEST="20230322"
ENDDATE_TEST="20230820"
SUBSAMPLE_TEST="test"

# Set TAQHOME directory
TAQHOME="/home/wustl/spring_zhou/taq"

# Create necessary directories if they do not exist
mkdir -p $TAQHOME/ge_logs
mkdir -p $TAQHOME/ge_errs
mkdir -p $TAQHOME/sas_logs

# Function to queue jobs based on parameters
queue_job() {
    local YYYYMMDD=$1
    local SUBSAMPLE=$2

    LOGFILE="ProgramMinuteLevelReturn.$YYYYMMDD.log"
    JOBSCRIPT="dailytaqjob.sh"

    if [ -f "$TAQHOME/ge_logs/$LOGFILE" ]; then
        echo "Skipping Job (already processed) at $(date): $YYYYMMDD $SUBSAMPLE"
    else
        echo "Queuing Job at $(date): $YYYYMMDD $SUBSAMPLE"
        qsub -v DATEPREFIX=$YYYYMMDD,SUBSAMPLE=$SUBSAMPLE "$JOBSCRIPT"
    fi
}

# Queue jobs for TEST mode
current_date="$STARTDATE_TEST"
while [[ $(date -d "$current_date" +%Y%m%d) -le $(date -d "$ENDDATE_TEST" +%Y%m%d) ]]; do
    YYYYMMDD=$(date -d "$current_date" +%Y%m%d)

    queue_job "$YYYYMMDD" "$SUBSAMPLE_TEST"

    current_date=$(date -d "$current_date + 1 day" "+%Y%m%d")
done
