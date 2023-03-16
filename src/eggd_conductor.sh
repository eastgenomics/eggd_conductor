#!/bin/bash
# eggd_conductor


set -exo pipefail

_set_environment () {
    : '''
    Set appropriate environment variables for being able to start jobs
    in other projects.

    Sets tokens to env variables from eggd_conductor config for later
    use
    '''
    dx download -f "$EGGD_CONDUCTOR_CONFIG" -o conductor.cfg

    # save original env variables to use later
    export PROJECT_NAME=$(dx describe --json $DX_PROJECT_CONTEXT_ID | jq -r '.name')
    export PROJECT_ID=$DX_PROJECT_CONTEXT_ID
    export PARENT_JOB_ID=$DX_JOB_ID

    # clear all set env variables to allow logging in and access to other projects
    unset DX_WORKSPACE_ID
    dx cd $DX_PROJECT_CONTEXT_ID:

    source /home/dnanexus/.dnanexus_config/unsetenv
    dx clearenv

    # set env variables from config file, contains auth token for login etc.
    # use of &> /dev/null  and removing set -x suppresses printing tokens
    # to DNAnexus logs which would not be ideal
    printf "sourcing config file and calling dx login"
    set +x
    printf "\n" >> conductor.cfg  # add new line char so read parses last line
    while IFS= read -r line; do export "${line?}"; done < conductor.cfg
    dx login --noprojects --token "$AUTH_TOKEN"
    set -x
}

_exit () {
    : '''
    Exit with code 1 and print given error message

    Arguments
        str : message string to print
    '''
    local message=$1

    printf "$message"
    printf "Exiting now."
    exit 1
}

_slack_notify () {
    : '''
    Send message to a given Slack channel

    Arguments
       str : message to send
       str : Slack channel to send to
    '''
    local message="$1"
    local channel=$2

    curl -d "text=${message}" \
         -d "channel=${channel}" \
         -H "Authorization: Bearer ${SLACK_TOKEN}" \
         -X POST https://slack.com/api/chat.postMessage

    printf "Message sent to $channel: $message"
}

_parse_sentinel_file () {
    : '''
    Parses given sentinel file from dx-streaming-upload to find samplesheet
    to extract sample ids from

    Globals
        SAMPLESHEET : file ID of samplesheet parsed from sentinel
            file / upload tars, set as global to be picked up in
            run_workflows.py if INPUT-SAMPLESHEET is set

    Arguments
        None
    '''
    # get json of details to parse required info from
    sentinel_details=$(dx describe --json "$upload_sentinel_record")
    sentinel_id=$(jq -r '.id' <<< "$sentinel_details")
    sentinel_path=$(jq -r '.details.dnanexus_path' <<< "$sentinel_details")
    sentinel_samplesheet=$(jq -r '.details.samplesheet_file_id' <<< "$sentinel_details")
    if [ -z "$RUN_ID" ]; then
        RUN_ID=$(jq -r '.details.run_id' <<< "$sentinel_details")
    fi

    tags=$(jq -r '.tags | .[]' <<< "$sentinel_details")
    if [[ "$tags" =~ "suppress-automation" ]]; then
        # sentinel file has been tagged to not run automated analysis
        # send Slack alert and exit without error
        local message=":warning: eggd_conductor: Sentinel file for run *${RUN_ID}* "
        message+="tagged with \`suppress-automation\` and will not be processed.%0A"
        message+="To run analysis, remove the tag and relaunch this job.%0A"
        message+="platform.dnanexus.com/projects/${PROJECT_ID/project-/}"
        message+="/monitor/job/${PARENT_JOB_ID/job-/}"

        _slack_notify "$message" "$SLACK_ALERT_CHANNEL"
        tag="Automated analysis not run due to sentinel file being tagged 'suppress-automation'"
        dx tag "$PARENT_JOB_ID" "$tag"
        mark-success
        exit 0
    fi

    # set file ID of sentinel record to env to pick up in run_workflows.py
    export upload_sentinel_record_ID="$sentinel_id"

    if [ "$SAMPLESHEET" ]; then
        # samplesheet specified as input arg
        echo "Using samplesheet specified as input"
        dx download -f "$SAMPLESHEET" -o SampleSheet.csv
    elif [ "$sentinel_samplesheet" != 'null' ]; then
        # samplesheet found during upload and associated to sentinel file
        echo "Using samplesheet associated with sentinel record"
        dx download -f "$sentinel_samplesheet" -o SampleSheet.csv
        SAMPLESHEET="$sentinel_samplesheet"
    else
        # sample sheet missing from sentinel file, most likely due to not being
        # named correctly, download the first tar, unpack and try to find it
        printf '\nCould not find samplesheet from sentinel file.\n'
        printf '\nFinding first run tar file to try get sample sheet from...\n'

        # first tar always named _000.tar.gz, return id of it to download
        local first_tar_id=$(dx find data --path "$sentinel_path" --brief --name "*_000.tar.gz")
        dx download -f "$first_tar_id" -o first_tar.tar.gz

        # unpack tar and find samplesheet
        mkdir ./first_tar_dir
        tar -xzf first_tar.tar.gz -C ./first_tar_dir
        local_samplesheet=$(find ./first_tar_dir -regextype posix-extended  -iregex '.*sample[-_ ]?sheet.csv$')

        if [ -z "$local_samplesheet" ]; then
            # sample sheet missing from root and first tar
            message=":warning: *Error - eggd_conductor*%0A%0A"
            message+="No samplesheet could be found for run *${RUN_ID}* from the app input, "
            message+="associated to the sentinel record or in the run data. Please rerun this "
            message+="job and provide a samplesheet with \`-iSAMPLESHEET\`.%0A%0A"
            message+="eggd_conductor job: ${conductor_job_url}"
            _slack_notify "$message" "$SLACK_ALERT_CHANNEL"
            dx-jobutil-report-error "No samplesheet found for analysis."
        else
            # found samplesheet in run data, upload back to project in same dir as sentinel file
            # to be able to get file ID for passing as input for INPUT-SAMPLESHEET
            SAMPLESHEET=$(dx upload "$SAMPLESHEET" --path "$sentinel_path" --brief)
            dx tag $SAMPLESHEET "samplesheet uploaded from eggd_conductor job: ${PARENT_JOB_ID}"

            # move samplesheet to parse sample names from in run_workflows.py
            mv "$local_samplesheet" /home/dnanexus/SampleSheet.csv
        fi
    fi
    export SAMPLESHEET_ID=$SAMPLESHEET  # SAMPLESHEET_ID picked up to parse as INPUT-SAMPLESHEET
}

_parse_fastqs () {
    : '''
    Parses given fastq files to keep only the file ID from the dict
    structure provided in the app

    Globals
    FASTQ_IDS : str of comma separated fastq file IDs
    '''
    FASTQ_IDS=""

    for fq in "${FASTQS[@]}"; do
        if [[ $fq =~ file-[A-Za-z0-9]* ]]
        then
            local file_id="${BASH_REMATCH[0]}"
            FASTQ_IDS+="$file_id,"
        else
            local message_str="Given fastq does not seem to be a valid file id: $fq\n"
            _slack_notify "$message_str" "$SLACK_ALERT_CHANNEL"
            _exit "$message_str"
        fi
    done

    # trim trailing comma
    FASTQ_IDS="${FASTQ_IDS::-1}"

    printf "\nFound fastq file ids: %s \n" "$FASTQS"
}

_testing_clean_up () {
    : '''
    If testing set to true a log file named testing_job_id.log will
    be generated with all job ids in that have been launched, these
    will all be terminated and any jobs that managed to complete
    before all were launched will have outputs deleted
    '''
    job_ids=$(cat testing_job_id.log)

    dx terminate $job_ids

    for job in $job_ids; do
        # find any output files and delete
        output=$(dx describe --json "$job" | jq -r '.output')
        if [ -z "$output" ]; then
            # some output present, gather all and delete
            all_outputs=$(dx describe --json "$job" | jq -r '.output | flatten | .[] | .["$dnanexus_link"] | select( . !=null )')
            if [ -z "$all_outputs" ]; then
                xargs -P8 -n1 <<< $all_outputs dx rm
            fi
        fi
    done
}


main () {

    mark-section "setting up"
    _set_environment

    python3 -m pip install -q --no-index --no-deps  packages/*

    # link to current running job
    conductor_job_url="platform.dnanexus.com/projects/${PROJECT_ID/project-/}"
    conductor_job_url+="/monitor/job/${PARENT_JOB_ID/job-/}"
    export conductor_job_url

    if [ -z "${upload_sentinel_record+x}" ] && [ -z "${FASTQS+x}" ]; then
        # requires either sentinel file or fastqs
        _exit "No sentinel file or list of fastqs provided."
    fi

    if [ "$SAMPLESHEET" ]; then
        # user specified samplesheet to use
        dx download -f "$SAMPLESHEET" -o SampleSheet.csv
        export SAMPLESHEET_ID=$SAMPLESHEET
    fi

    if [ "$RUN_INFO_XML" ]; then
        # user specified RunInfo.xml specified to use
        dx download -f "$RUN_INFO_XML" -o RunInfo.xml
    fi

    if [[ "$upload_sentinel_record" ]]; then
        printf "\nParsing sentinel file"
        _parse_sentinel_file
    else
        # app run manually without sentinel file
        # should have array of fastqs and sample sheet or sample names
        mark-section "running manually from provided FastQ files"
        _parse_fastqs
    fi

    # send a message to logs so we know something is starting
    message=":gear: eggd_conductor: Automated analysis beginning to process *${RUN_ID}*%0A"
    message+="${conductor_job_url}"
    _slack_notify "$message" "$SLACK_LOG_CHANNEL"


    mark-section "Building input arguments"

    optional_args=""
    if [ "$ASSAY_CONFIG" ]; then
        # assay config specified, download and use it
        dx download "$ASSAY_CONFIG" -o assay_config.json
        optional_args+="--assay_config assay_config.json "

        # add file ID of config as output field to easily audit what configs used for analyses
        ASSAY_CONFIG_ID=$(grep -oE 'file-[A-Za-z0-9]+' <<< "$ASSAY_CONFIG")
        dx-jobutil-add-output assay_config_file_id "$ASSAY_CONFIG_ID" --class=file
    fi
    if [ "$upload_sentinel_record" ]; then optional_args+="--sentinel_file ${sentinel_id} "; fi
    if [ -f "SampleSheet.csv" ]; then optional_args+="--samplesheet SampleSheet.csv "; fi
    if [ -f "RunInfo.xml" ]; then optional_args+="--run_info_xml RunInfo.xml "; fi
    if [ "$FASTQS" ]; then optional_args+="--fastqs $FASTQ_IDS "; fi
    if [ "$SAMPLE_NAMES" ]; then optional_args+="--samples ${SAMPLE_NAMES} "; fi
    if [ "$DX_PROJECT" ]; then optional_args+="--dx_project_id $DX_PROJECT "; fi
    if [ "$RUN_ID" ]; then optional_args+="--run_id $RUN_ID "; fi
    if [ "$DEMULTIPLEX_JOB_ID" ]; then optional_args+="--demultiplex_job_id $DEMULTIPLEX_JOB_ID "; fi
    if [ "$DEMULTIPLEX_OUT" ]; then optional_args+="--demultiplex_output ${DEMULTIPLEX_OUT} "; fi
    if [ "$DEVELOPMENT" == 'true' ]; then optional_args+="--development "; fi
    if [ "$TESTING" == 'true' ]; then optional_args+="--testing "; fi
    if [ "$TESTING_SAMPLE_LIMIT" ]; then optional_args+="--testing_sample_limit ${TESTING_SAMPLE_LIMIT} "; fi

    echo $optional_args

    mark-section "starting analyses"

    {
        python3 run_workflows/run_workflows.py $optional_args
    } || {
        # failed to launch all jobs -> handle clean up and sending error notification

        # if in testing mode terminate everything and clear output, else
        # terminate whatever is in 'job_id.log' if present as these will be
        # an incomplete set of jobs for a given app / workflow
        if [ -s testing_job_id.log ]; then
            _testing_clean_up
        elif [ -s job_id.log ]; then
            # non empty log => jobs to terminate
            echo "Terminating jobs"
            jobs=$(cat job_id.log)
            dx terminate "$jobs"
        fi

        if [ -f slack_fail_sent.log ]; then
            # something went wrong and Slack alert sent in Python script =>
            # just exit
            exit 1
        fi

        # build message to send to alert channel and exit
        message=":warning: *Error - eggd_conductor*%0A%0AJobs failed to launch - uncaught "
        message+="exception occurred!%0A%0Aeggd_conductor job: "
        message+="platform.dnanexus.com/projects/${PROJECT_ID/project-/}"
        message+="/monitor/job/${PARENT_JOB_ID/job-/}"
        if [ -s analysis_project.log ]; then
            # analysis project was created, add to alert
            read -r project_id _ _ < analysis_project.log
            message+="%0AAnalysis project: "
            message+="platform.dnanexus.com/projects/${project_id/project-/}/monitor/"
        fi

        # parse out the Python traceback to add to the message
        traceback=$(awk '/^Traceback/,/+/' dx_stderr | head -n -1)
        if [ "$traceback" ]; then
            message+="%0A%0ATraceback: \`\`\`${traceback}\`\`\`"
        fi

        _slack_notify "$message" "$SLACK_ALERT_CHANNEL"

        exit 1
    }

    if [ "$TESTING" == true ]; then
        _testing_clean_up
    fi

    read -r project_id assay version < analysis_project.log
    total_jobs=$(cat total_jobs.log)

    project_name=$(dx describe --json $project_id | jq -r '.name')

    analysis_project_url="platform.dnanexus.com/projects/${project_id/project-/}/monitor/"

    message=":white_check_mark: eggd_conductor: ${total_jobs} jobs successfully launched for "
    message+="*${RUN_ID}*%0AConfig used: *${assay}* (v${version})%0A"
    message+="Analysis project: *${project_name}*%0A${analysis_project_url}"

    _slack_notify "$message" "$SLACK_LOG_CHANNEL"

    # tag conductor job with downstream project used for analysis
    dx tag "$PARENT_JOB_ID" "$analysis_project_url"

    # set all job IDs as output
    job_ids=$(cat all_job_ids.log)
    job_ids="${job_ids%?}"  # trim off trailing comma
    dx-jobutil-add-output job_ids "$job_ids" --class=string

    mark-success
}
