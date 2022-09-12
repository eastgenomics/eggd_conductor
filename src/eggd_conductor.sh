#!/bin/bash
# eggd_conductor
# Jethro Rainford
# 20210831

# This can either be run automatically by dx-streaming-upload on completing
# upload of a sequencing run, or manually by providing a set of fastqs
# to analyse and a sample sheet / list of sample names

set -exo pipefail

_set_environment () {
    : '''
    Set appropriate environment variables for being able to start jobs
    in other projects.

    Sets tokens to env variables from eggd_conductor config for later
    use, parses out slack API token to slack_token.py
    '''
    dx download "$EGGD_CONDUCTOR_CONFIG" -o conductor.cfg
    mkdir hermes

    # save original env variables to use later
    PROJECT_NAME=$(dx describe --json $DX_PROJECT_CONTEXT_ID | jq -r '.name')
    PROJECT_ID=$DX_PROJECT_CONTEXT_ID
    PARENT_JOB_ID=$DX_JOB_ID

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
    source conductor.cfg &> /dev/null
    dx login --noprojects --token $AUTH_TOKEN
    echo "hermes_token=\"${SLACK_TOKEN}\"" > hermes/slack_token.py
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
    Send message to either egg-logs or egg-alerts slack channel using Hermes slack bot

    Arguments
       str : message to send
       str : Slack channel to send to
    '''

    local message="eggd_conductor - ${1}"
    local channel=$2

    python3 hermes/hermes.py -v msg "$message" "$channel"
    printf "Message sent to $channel: $message"
}

_parse_sentinel_file () {
    : '''
    Parses given sentinel file from dx-streaming-upload to find samplesheet
    to extract sample ids from

    Globals
        SAMPLESHEET : samplesheet parsed from sentinel file / upload tars

    Arguments
        None
    '''

    # get json of details to parse required info from
    sentinel_details=$(dx describe --json "$SENTINEL_FILE")
    sentinel_path=$(jq -r '.details.dnanexus_path' <<< "$sentinel_details")
    samplesheet=$(jq -r '.details.samplesheet_file_id' <<< "$sentinel_details")
    if [ -z "$RUN_ID" ]; then
        RUN_ID=$(jq -r '.details.run_id' <<< "$sentinel_details")
    fi

    if [ "$samplesheet" != 'null' ]; then
        # samplesheet found during upload and associated to sentinel file
        dx download "$samplesheet" -o SampleSheet.csv
        SAMPLESHEET="SampleSheet.csv"
    else
        # sample sheet missing from sentinel file, most likely due to not being
        # named correctly, download the first tar, unpack and try to find it
        printf 'Could not find samplesheet from sentinel file.\n'
        printf 'Finding first run tar file to get sample sheet from.\n'

        # first tar always named _000.tar.gz, return id of it to download
        local first_tar_id=$(dx find data --path "$sentinel_path" --brief --name "*_000.tar.gz")
        dx download "$first_tar_id" -o first_tar.tar.gz

        # unpack tar and find samplesheet
        mkdir ./first_tar_dir
        tar -xzf first_tar.tar.gz -C ./first_tar_dir
        SAMPLESHEET=$(find ./first_tar_dir -regextype posix-extended  -iregex '.*sample[-_ ]?sheet.csv$')

        if [ -z "$SAMPLESHEET" ];
        then
            # sample sheet missing from root and first tar
            message="Sample sheet missing from runs dir and first tar, exiting now."
            dx-jobutil-report-error "$message"
            _slack_notify "$message" "$SLACK_ALERT_CHANNEL"
            exit 1
        fi
    fi
}


main () {

    mark-section "setting up"
    _set_environment

    # our own sample sheet validator and slack bot
    tar xf validate_sample_sheet_v*.tar.gz
    tar xf hermes_v*.tar.gz -C hermes --strip-components 1

    python3 -m pip install --no-index --no-deps  packages/*

    # send a message to logs so we know something is starting
    message="Automated analysis beginning in ${PROJECT_NAME} ($PROJECT_ID) "
    message+="to process ${RUN_ID}\n\neggd_conductor job: "
    message+="platform.dnanexus.com/projects/${PROJECT_ID/project-/}"
    message+="/monitor/job/${PARENT_JOB_ID/job-/}"
    _slack_notify "$message" "$SLACK_LOG_CHANNEL"


    if [ -z "${SENTINEL_FILE+x}" ] && [ -z "${FASTQS+x}" ]; then
        # requires either sentinel file or fastqs
        _exit "No sentinel file or list of fastqs provided."
    fi

    exit 1


    if [[ "$SENTINEL_FILE" ]]; then
        if [[ -z "$SAMPLESHEET"  ]]; then
            # no samplesheet specified, try get from sentinel file
            mark-section "getting samplesheet from sentinel file"
            _parse_sentinel_file
        else
            echo "Using sentinel file ${SENTINEL_FILE} and samplesheet ${SAMPLESHEET}"
        fi
    else
        # app run manually without sentinel file
        # should have array of fastqs and sample sheet or sample names
        mark-section "running manually from provided FastQ files"
        _parse_fastqs
    fi

    mark-section "Building input arguments"

    optional_args=""
    if [ "$ASSAY_CONFIG" ]; then optional_args+="--assay_config ASSAY_CONFIG "; fi
    if [ "$SENTINEL_FILE" ]; then optional_args+="--sentinel_file ${SENTINEL_FILE} "; fi
    if [ "$SAMPLESHEET" ]; then optional_args+="--samplesheet ${SAMPLESHEET} "; fi
    if [ "$FASTQS" ]; then optional_args+="--fastqs $FASTQ_IDS "; fi
    if [ "$SAMPLE_NAMES" ]; then optional_args+="--samples ${SAMPLE_NAMES} "; fi
    if [ "$DX_PROJECT" ]; then optional_args+="--dx_project_id $DX_PROJECT "; fi
    if [ "$RUN_ID" ]; then optional_args+="--run_id $RUN_ID "; fi
    if [ "$BCL2FASTQ_JOB_ID" ]; then optional_args+="--bcl2fastq_id $BCL2FASTQ_JOB_ID "; fi
    if [ "$BCL2FASTQ_OUT" ]; then optional_args+="--bcl2fastq_output ${BCL2FASTQ_OUT} "; fi
    if [ "$DEVELOPMENT" ]; then optional_args+="--development "; fi

    mark-section "starting analyses"
    python3 run_workflows.py "$optional_args"

    mark-success
}
