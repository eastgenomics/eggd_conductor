#!/bin/bash
# eggd_conductor
# Jethro Rainford
# 20210831

# This can either be run automatically by dx-streaming-upload on completing
# upload of a sequencing run, or manually by providing a set of fastqs
# to analyse and a sample sheet / list of sample names

set -exo pipefail

_set_environment () {
    # Set appropriate environment variables for being able to start jobs in other projects
    # Sets tokens to env variables from eggd_conductor config for later use, parses out slack API
    # token to slack_token.py

    # if not given, get highest tagged version of config files from given directories
    if [ -z "$EGGD_CONDUCTOR_CONFIG" ]
    then
        EGGD_CONDUCTOR_CONFIG=$(_select_highest_version "$EGGD_CONDUCTOR_CFG_PATH")
    fi

    if [ -z "$CUSTOM_CONFIG" ]
    then
        local high_level_config
        high_level_config=$(_select_highest_version "$HIGH_LEVEL_CONFIG_PATH")
        dx download "$high_level_config" -o high_level_config.tsv
    fi

    dx download "$eggd_conductor_config" -o conductor.cfg
    mkdir packages hermes

    # save original env variables to use later
    PROJECT_NAME=$(dx describe --json $DX_PROJECT_CONTEXT_ID | jq -r '.name')
    PROJECT_ID=$DX_PROJECT_CONTEXT_ID
    PARENT_JOB_ID=$DX_JOB_ID

    # clear all set env variables to allow logging in and access to other projects
    unset DX_WORKSPACE_ID
    dx cd $DX_PROJECT_CONTEXT_ID:

    source /home/dnanexus/.dnanexus_config/unsetenv
    dx clearenv

    # set env variables from config file, contains auth token for login
    # use of &> /dev/null  and removing set -x suppresses printing auth token
    # to logs which would not be ideal
    printf "sourcing config file and calling dx login"
    set +x
    source conductor.cfg &> /dev/null
    dx login --noprojects --token $AUTH_TOKEN
    echo "hermes_token=\"${SLACK_TOKEN}\"" > hermes/slack_token.py
    set -x
}

_select_highest_version () {
    # Given a directory of configs with version properties, returns the file id of the highest version
    # Args: $1 - project:/path/to/configs
    local path=$1

    # get the version and store the file id if it is higher than the previous config AND it has an
    # associated CAPA property => signed off for use
    local properties
    local capa
    local next_version
    local current_version=0
    local current_file_id

    for config in $(dx find data --brief --path "$path"); do
        properties=$(dx describe --json "$config" | jq -r '.properties')
        capa=$(jq -r '.CAPA' <<< "$properties")
        next_version=$(jq -r '.version' <<< "$properties")

        if awk "BEGIN {exit !($next_version > $current_version)}" && [ ${capa+x} ]
        then
            # version is higher and has CAPA associated => update locals
            current_version="$next_version"
            current_file_id="$config"
        fi
    done

    if [ -z "$current_file_id" ]
    then
        local message="No version and CAPA tagged config files found in ${path}. Exiting now."
        _slack_notify "$message" egg-alerts
        _exit "$message"
    else
        # file id set, echo to capture from stdout by function call
        echo "$current_file_id"
    fi
}

_exit () {
    # exit with code 1 and print given error message
    # Args $1: message string to print
    local message=$1

    printf "$message"
    printf "Exiting now."
    exit 1
}

_slack_notify () {
    # Send message to either egg-logs or egg-alerts slack channel using Hermes slack bot
    # Args:
    #    - $1: message to send
    #    - $2: channel to send to
    local message="eggd_conductor - ${1}"
    local channel=$2

    python3 hermes/hermes.py -v msg "$message" "$channel"
    printf "Message sent to $channel: $message"
}

_parse_sentinel_file () {
    # Parses given sentinel file from dx-streaming-upload to find samplesheet
    # to extract sample ids from

    # get json of details to parse required info from
    local sentinel_details=$(dx describe --json "$SENTINEL_FILE")
    local sentinel_path=$(jq -r '.details.dnanexus_path' <<< "$sentinel_details")
    [ -z "$RUN_ID" ] && RUN_ID=$(jq -r '.details.run_id' <<< "$sentinel_details")
    SENTINEL_FILE_ID=$(jq -r '.id' <<< "$sentinel_details")

    if [ ! "$SAMPLE_SHEET_ID" ]; then
        # sample sheet has not been specified on running, try get from sentinel details
        SAMPLE_SHEET_ID=$(dx find data --path "$sentinel_path" --name 'SampleSheet.csv' --brief)
    fi

    if [ "$SAMPLE_SHEET_ID" ]
    then
        # found sample sheet or passed -> download
        printf "Found samplesheet from given sentinel file (%s), downloading" "$sample_sheet_id"
        dx download "$SAMPLE_SHEET_ID" -o SampleSheet.csv
        sample_sheet='SampleSheet.csv'
    else
        # sample sheet missing from runs dir, most likely due to not being name SampleSheet.csv
        # find the first tar, download, unpack and try to find it
        printf 'Could not find samplesheet from sentinel file.\n'
        printf 'Finding first run tar file to get sample sheet from.\n'

        # first tar always named _000.tar.gz, return id of it to download
        local first_tar_id=$(dx find data --path "$sentinel_path" --brief --name "*_000.tar.gz")
        dx download "$first_tar_id" -o first_tar.tar.gz

        # unpack tar and find samplesheet
        mkdir ./first_tar_dir
        tar -xzf first_tar.tar.gz -C ./first_tar_dir
        sample_sheet=$(find ./first_tar_dir -regextype posix-extended  -iregex '.*sample[-_ ]?sheet.csv$')

        if [ -z "$sample_sheet" ];
        then
            # sample sheet missing from root and first tar
            echo "Sample sheet missing from runs dir and first tar, exiting now."
            dx-jobutil-report-error "Sample sheet missing from both runs dir and first tar file"
            exit 1
        fi

        # parse list of samples from samplesheet
        sample_list=$(tail -n +22 "$sample_sheet" | cut -d',' -f 1)
    fi
}

_parse_fastqs () {
    # Called if not starting from a sentinel file and using an array of fastq
    # files as input, checks if all provided files are valid file ids and builds a string
    # to pass to python script

    if [ ! "$SAMPLE_NAMES" ] && [ ! "$sample_sheet" ]
    # needs sample sheet or sample list passing
    then
        printf 'No sample sheet or list of samples defined, one of these must be provided. Exiting now.'
        dx-jobutil-report-error 'No sample sheet or list of samples defined, one of these must be provided.'
        exit 1
    fi

    # build list of file ids for given fastqs to pass to workflow script
    FASTQ_IDS=""

    for fq in "${FASTQS[@]}"; do
        if [[ $fq =~ file-[A-Za-z0-9]* ]]
        then
            local file_id="${BASH_REMATCH[0]}"
            FASTQ_IDS+="$file_id, "
        else
            local message_str="Given fastq does not seem to be a valid file id: $fq\n"
            _exit "$message_str"
        fi
    done
    printf "\nFound fastq file ids: %s \n" "$FASTQS"

    if [[ $SAMPLE_NAMES ]] && [[ $sample_sheet ]]
    # samplesheet and sample list given, use list
    then
        printf 'both sample sheet and list of samples provided, using sample list'
        # ensure sample names are string seperated by a space
        sample_list=$(echo "$SAMPLE_NAMES" | tr -d '[:space:]' | sed 's/,/ /g')
        printf "Samples specified for analysis: %s" "$sample_list"
    fi

    if [[ $sample_sheet ]] && [[ ! $SAMPLE_NAMES ]]
    # just sheet given => download
    then
        dx download "$sample_sheet"
        # get list of sample names from sample sheet if not using sample name list
        sample_list=$(tail -n +22 "$sample_sheet" | cut -d',' -f 1)
    fi
}

_match_samples_to_assays () {
    # Build associative array (i.e. key value pairs) of assay EGG codes to sample names
    # This will allow us to run the workflows with the given config on appropriate sample sets
    declare -A sample_to_assay

    # for each sample, parse the eggd code, get associated assay from config if not specified
    for name in $sample_list;
    do
        local sample_assay_type
        if [ -z "$ASSAY_TYPE" ]; then
            # assay type not specified, infer from eggd code and high level config

            # get eggd code from name using regex, if not found will exit
            if [[ $name =~ EGG[0-9]{1,2} ]]
            then
                local sample_eggd_code="${BASH_REMATCH[0]}"
            else
                local message_str="Sample name invalid: could not parse EGG code from $name"
                dx-jobutil-report-error "$message_str"
                _slack_notify "$message_str" egg-alerts
                _exit "$message_str"
            fi

            # get associated assay from config
            sample_assay_type=$(grep "$sample_eggd_code" high_level_config.tsv | awk '{print $2}')

            if [ -z "$sample_assay_type" ]
            then
                # appropriate assay not found from sample egg code
                echo "Assay for sample $name not found in config using the following eggd code: $sample_eggd_code"
                echo "Exiting now."
                dx-jobutil-report-error "Assay for sample $name not found in config using the following eggd code: $sample_eggd_code"
                exit 1
            fi
        else
            sample_assay_type="$ASSAY_TYPE"
        fi
        # add sample name to associated assay code in array, comma separated to parse later
        sample_to_assay[$sample_assay_type]+="$name,"
    done
}

_run_bcl2fastq () {
    # Call bcl2fastq app to perform demultiplexing on given sentinel file run

    local bcl2fastq_out  # output directory for bcl2fastq app

    if [ -z "$BCL2FASTQ_OUT" ]; then
        # bcl2fastq output path not set, default to putting it in the parent run dir of the
        # sentinel record
        bcl2fastq_out=$(dx describe --json "$SENTINEL_FILE" | jq -r '.details.dnanexus_path')
        bcl2fastq_out=${bcl2fastq_out%runs}
    else
        bcl2fastq_out="$BCL2FASTQ_OUT"
    fi

    # check no fastqs are already present in the output directory for bcl2fastq, exit if any
    # present to prevent making a mess with bcl2fastq output
    local fastqs=$(dx find data --json --brief --name "*.fastq*" --path $bcl2fastq_out)

    if [ ! "$fastqs" == "[]" ]; then
        dx-jobutil-report-error "Selected bcl2fastq output directory already contains fastq files"

        local message_str="Selected output directory already contains fastq files: $bcl2fastq_out.\n
        This is likely because demultiplexing has already been run and output to this directory.\n
        Exiting now to prevent poluting output directory with bcl2fastq output.
        "

        _slack_notify "$message_str" egg-alerts
        _exit "$message_str"
    fi

    local optional_args
    optional_args+="--destination ${bcl2fastq_out}"

    echo "Starting bcl2fastq app with output at: $bcl2fastq_out"
    echo "Holding app until demultiplexing complete to trigger downstream workflow(s)..."

    {
        dx run --brief --detach --wait -y ${optional_args} --auth-token "$API_KEY" \
            "$BCL2FASTQ_APP_ID" -iupload_sentinel_record="$SENTINEL_FILE_ID"

        if [[ ! "$bcl2fastq_out" =~ "$PROJECT_ID" ]]
        then
            # demultiplexing run in a different project => tag parent job with link to project analysis
            local project_id=$(awk -F[\(\)] '{print $2}' <<< "$bcl2fastq_out" | sed 's/project-//')
            local url="platform.dnanexus.com/projects/${project_id}/monitor/"
            dx tag "$PARENT_JOB_ID" "$url"
        fi
    } || {
        # demultiplexing failed, send alert and exit
        local message_str="bcl2fastq job failed in project ${bcl2fastq_out%%:*}"
        _slack_notify "$message_str" egg-logs
        _exit "$message_str"
    }
}

_get_low_level_configs () {
    # Use assay codes from sample names to download appropriate low level config files
    mkdir low_level_configs

    # build associative array assay EGG codes to the downloaded config file
    declare -A assay_to_config
    local config_file_id
    local config_name

    for k in "${!sample_to_assay[@]}"
    do
        if [ ! "$CUSTOM_CONFIG" ]
        then
            # no custom config => get file id for low level assay config file & download
            config_file_id=$(grep "$k" high_level_config.tsv | awk '{print $NF}')
        else
            config_file_id="$CUSTOM_CONFIG"
        fi

        config_name=$(dx describe --json "$config_file_id" | jq -r '.name')
        assay_to_config[$k]+="$config_name"

        if [ ! -f "low_level_configs/${config_name}" ]; then
            # low level config file not yet downloaded => download
            dx download "$config_file_id" -o "low_level_configs/${config_name}"
        fi
    done
}

_validate_samplesheet () {
    # Run samplesheet validator to check for issues in samplesheet that will break
    # bcl2fastq or bad sample naming that will affect downstream workflows

    # get all regex patterns from low level config files to check
    # sample names against from config(s) if given
    regex_patterns=""
    for file in low_level_configs/*.json; do
        if jq -r '.sample_name_regex' "$file"; then
            # parse regex patterns into one string and add to string of total patterns
            local file_regexes=$(jq -rc '.sample_name_regex[]' "$file" | tr '\n' ' ')
            if [ "$file_regexes" != " " ]; then regex_patterns+="$file_regexes"; fi
        fi
    done

    # run samplesheet validator
    local stdout
    if [[ $regex_patterns ]]; then
        stdout=$(python3 validate_sample_sheet-1.0.0/validate/validate.py \
        --samplesheet "$sample_sheet" --name_patterns $regex_patterns) &> /dev/null
    else
        stdout=$(python3 validate_sample_sheet-1.0.0/validate/validate.py \
        --samplesheet "$sample_sheet") &> /dev/null
    fi

    stdout=$(echo "$stdout" | tr '\n' ' ') &> /dev/null  # remove messy line breaks

    if [[ ! "$stdout" =~ "SUCCESS" ]]; then
        # check for errors found in samplesheet => exit
        dx-jobutil-report-error "Errors found in sample sheet"
        local message_str="Errors found in sample sheet, please check logs for details."
        _slack_notify "$message_str" egg-alerts
        _exit "$message_str"
    fi
}

_trigger_workflow () {
    # Trigger workflow for given set of samples with appropriate low level config file

    printf "Calling workflow for assay $i on samples:\n ${sample_to_assay[$k]}"

    # set optional arguments to workflow script by app args
    local optional_args
    if [ "$DX_PROJECT" ]; then optional_args+="--dx_project_id $DX_PROJECT "; fi
    if [ "$BCL2FASTQ_JOB_ID" ]; then optional_args+="--bcl2fastq_id $BCL2FASTQ_JOB_ID "; fi
    if [ "$FASTQ_IDS" ]; then optional_args+="--fastqs $FASTQ_IDS"; fi
    if [ "$RUN_ID" ]; then optional_args+="--run_id $RUN_ID "; fi
    if [ "$DEVELOPMENT" ]; then optional_args+="--development "; fi

    {
        python3 run_workflows.py --config_file "low_level_configs/${assay_to_config[$k]}" \
        --samples "${sample_to_assay[$k]}" --assay_code "$k" $optional_args
    } || {
        # failed in starting up all workflows, send message to alerts
        local message_str="Automation failed calling workflows, please check the logs for details."

        # non-empty file => previous jobs launched, add to message and terminate all previous jobs
        if [ -s job_id.log ]; then
            local launched_job_ids=$(cat job_id.log)
            message_str+="\nCancelling previous analysis job(s): ${launched_job_ids}"
            dx terminate "${launched_job_ids}"
        fi

        _slack_notify "$message_str" egg-alerts
        _exit "$message_str"
    }
}

main () {

    mark-section "setting up"
    _set_environment

    if [ -z "${SENTINEL_FILE+x}" ] && [ -z "${FASTQS+x}" ]; then
        # requires either sentinel file or fastqs
        _exit "No sentinel file or list of fastqs provided."
    fi

    # our own sample sheet validator and slack bot
    tar xf validate_sample_sheet_v*.tar.gz
    tar xf hermes_v*.tar.gz -C hermes --strip-components 1
    tar xf python_packages.tar.gz -C packages  # required python packages for run_workflows.py

    python3 -m pip install --no-index --no-deps  packages/*

    # send an alert to logs so we know something is starting
    _slack_notify "Automated analysis beginning in ${PROJECT_NAME} ($PROJECT_ID)" egg-logs

    if [[ "$SENTINEL_FILE" ]]; then
        # sentinel file passed when run automatically via dx-streaming-upload
        mark-section "parsing sentinel file"
        _parse_sentinel_file
    else
        # applet run manually without sentinel file
        # should have array of fastqs and sample sheet or sample names
        mark-section "running manually from provided FastQ files"
        _parse_fastqs
    fi

    # now we need to know what samples we have to trigger the appropriate workflows
    # use the config and sample sheet names to infer which to use if a specific assay not specified
    mark-section "building assay-sample array"

    # build an array of assay codes -> comma separated sample names to pass to running workflows, i.e.
    # FH: X000001_EGG3,X000002_EGG3...
    # TSOE: X000003_EGG1,X000004_EGG1...
    _match_samples_to_assays

    # nice message for the logs to sanity check things
    printf "\nSample to assays:\n"
    for i in "${!sample_to_assay[@]}"; do printf "assay: $i - samples: ${sample_to_assay[$i]}"; done

    # get low level config files for appropriate assays
    _get_low_level_configs

    # perform samplesheet validation unless set to False
    if [ "$VALIDATE_SAMPLESHEET" = true ]; then
        mark-section "performing samplesheet validation"
        _validate_samplesheet
    fi

    if [ "$SENTINEL_FILE" ] && [ -z "$BCL2FASTQ_JOB_ID" ]
    then
        # no previous bcl2fastq job given to use the output from
        # starting bcl2fastq and holding app until it completes
        mark-section "demultiplexing with bcl2fastq"
        BCL2FASTQ_JOB_ID=$(_run_bcl2fastq)
    fi

    # trigger workflows using config for each set of samples for an assay
    # if calling all apps/workflows fails _trigger_workflow will make a call to _slack_notify then
    # _exit to stop
    mark-section "triggering workflows"
    for k in "${!sample_to_assay[@]}"
    do
        _trigger_workflow

        local analysis_project=$(cat run_workflows_output_project.log)
        local message="Workflows triggered for samples successfully in ${analysis_project}"
        _slack_notify "${message}" egg-alerts

        # run_workflows.py writes the analysis project name and id used to log file
        # add tag with URL to parent conductor job to link it to the downstream analysis
        local analysis_project_id=$(awk -F[\(\)] '{print $2}' <<< "$analysis_project" | sed 's/project-//')
        local URL="platform.dnanexus.com/projects/${analysis_project_id}/monitor/"
        dx tag "$PARENT_JOB_ID" "$URL"

    done

    mark-success
}

main
