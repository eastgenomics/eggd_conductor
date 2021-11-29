#!/bin/bash
# eggd_conductor

# This can either be run automatically by dx-streaming-upload on completing
# upload of a sequencing run, or manually by providing a set of fastqs 
# to analyse and a sample sheet / list of sample names

set -exo pipefail

main() {

    dx download "$eggd_conductor_config" -o conductor.cfg

    # clear all set env variables to allow logging in and access to other projects
    unset DX_WORKSPACE_ID
    dx cd $DX_PROJECT_CONTEXT_ID:

    source /home/dnanexus/.dnanexus_config/unsetenv
    dx clearenv

    # set env variables from config file, contains auth token for login
    # use of &> /dev/null  and removing set -x suppresses printing auth token to logs
    printf "Sourcing config file and calling dx login"
    set +x
    source conductor.cfg &> /dev/null
    dx login --noprojects --token $AUTH_TOKEN
    set -x

    # our own sample sheet validaecho-* pytz-* python_dateutil-* numpy-* pandas-*
    tar xf validate_sample_sheet_v1.0.0.tar.gz
    tar xf packages/python_packages.tar.gz

    pip3 install -q --user six-* pytz-* python_dateutil-* numpy-* pandas-*

    if [[ "$sentinel_file" ]]; then
        # sentinel file passed when run automatically via dx-streaming-upload
        mark-section "Parsing sentinel file"

        # get json of details to parse required info from
        sentinel_details=$(dx describe --json "$sentinel_file")
        run_id=$(jq -r '.details.run_id' <<< "$sentinel_details")
        run_dir=$(jq -r '.folder' <<< "$sentinel_details")
        sentinel_path=$(jq -r '.details.dnanexus_path' <<< "$sentinel_details")
        sentinel_file_id=$(jq -r '.id' <<< "$sentinel_details")

        if [ ! "$sample_sheet_id" ]; then
            # check if sample sheet has been specified on running
            sample_sheet_id=$(dx find data --path "$sentinel_path" --name 'SampleSheet.csv' --brief)
        fi

        if [ "$sample_sheet_id" ]
        then
            # found sample sheet or passed -> download
            printf "Found samplesheet from given sentinel file (%s), downloading" "$sample_sheet_id"
            dx download "$sample_sheet_id"
            sample_sheet='SampleSheet.csv'
        else
            # sample sheet missing from runs dir, most likely due to not being name SampleSheet.csv
            # find the first tar, download, unpack and try to find it
            printf 'Could not find samplesheet from sentinel file.\n'
            printf 'Finding first run tar file to get sample sheet from.\n'

            # first tar always named _000.tar.gz, return id of it to download
            first_tar_id=$(dx find data --path "$sentinel_path" --brief --name "*_000.tar.gz")
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
    else
        # applet run manually without sentinel file
        # should have array of fastqs and sample sheet or sample names
        mark-section "Running manually from provided FastQ files"

        if "${fastqs[@]}";
        then
            # build list of file ids for given fastqs to pass to workflow script
            fastq_ids=""

            for fq in "${fastqs[@]}"; do
                if [[ $fq =~ file-[A-Za-z0-9]* ]]
                then
                    file_id="${BASH_REMATCH[0]}"
                    fastq_ids+="$file_id, "
                else
                    printf "Given fastq does not seem to be a valid file id: %s \n" "$fq"
                    printf "Exiting now."
                    exit 1
                fi
            done
            printf "\nFound fastq file ids: %s \n" "$fastq_ids"
        fi

        if [ ! "$samples_names" ] && [ ! "$sample_sheet" ]
        # needs sample sheet or sample list passing
        then
            printf 'No sample sheet or list of samples defined, one of these must be provided. Exiting now.'
            dx-jobutil-report-error 'No sample sheet or list of samples defined, one of these must be provided.'
            exit 1
        fi

        if [[ $samples_names ]] && [[ $sample_sheet ]]
        # samplesheet and sample list given, use list
        then
            printf 'both sample sheet and list of samples provided, using sample list'
            # ensure sample names are string seperated by a space
            sample_list=$(echo "$samples_names" | tr -d '[:space:]' | sed 's/,/ /g')
            printf "Samples specified for analysis: %s" "$sample_list"
        fi

        if [[ $sample_sheet ]] && [[ ! $samples_names ]]
        # just sheet given => download
        then
            dx download "$sample_sheet"
            # get list of sample names from sample sheet if not using sample name list
            sample_list=$(tail -n +22 "$sample_sheet" | cut -d',' -f 1)
        fi
    fi

    # now we need to know what samples we have, to trigger the appropriate workflows
    # use the config and sample sheet names to infer which to use if a specific assay not passed

    # get high level config file if not using custom config
    mark-section "Building assay-sample array"

    if [ -z "$custom_config" ]
    then
        printf "\nDownloading high level config file: %s" "$high_level_config"
        dx download "$high_level_config" -o "high_level_config.tsv"
    fi

    # build associative array (i.e. key value pairs) of assay EGG codes to sample names
    # this will allow us to run the workflows with the given config on appropriate sample sets
    declare -A sample_to_assay

    # for each sample, parse the eggd code, get associated assay from config if not specified
    for name in $sample_list;
    do
        if [ -z "$assay_type" ]; then
            # assay type not specified, infer from eggd code and high level config

            # get eggd code from name using regex, if not found will exit
            if [[ $name =~ EGG[0-9]{1,2} ]]
            then 
                sample_eggd_code="${BASH_REMATCH[0]}"
            else
                printf "Could not parse EGG code from name: %s \n" "$name"
                printf 'Exiting now.'
                dx-jobutil-report-error "Could not parse EGG code from sample: $name"
                exit 1
            fi

            # get associated assay from config
            assay_type=$(grep "$sample_eggd_code" high_level_config.tsv | awk '{print $2}')

            if [ -z "$assay_type" ]
            then
                # appropriate assay not found from sample egg code
                echo "Assay for sample $name not found in config using the following eggd code: $sample_eggd_code"
                echo "Exiting now."
                dx-jobutil-report-error "Assay for sample $name not found in config using the following eggd code: $sample_eggd_code"
                exit 1
            fi
        fi
        # add sample name to associated assay code in array, comma separated to parse later
        sample_to_assay[$assay_type]+="$name,"
    done

    # we now have an array of assay codes to comma separated sample names i.e.
    # FH: X000001_EGG3,X000002_EGG3...
    # TSOE: X000003_EGG1,X000004_EGG1...

    printf "\nsample to assays:\n"

    for i in "${!sample_to_assay[@]}"
    do
        printf "key  : $i"
        printf "value: ${sample_to_assay[$i]}"
    done

    # get low level config files for appropriate assays
    mkdir low_level_configs

    # build associative array assay EGG codes to the downloaded config file
    declare -A assay_to_config

    for k in "${!sample_to_assay[@]}"
    do
        echo "Downloading low level config file(s)"
        if [ ! "$custom_config" ]
        then
            # no custom config => get file id for low level assay config file & download
            config_file_id=$(grep "$k" high_level_config.tsv | awk '{print $NF}')
            config_name=$(dx describe --json "$config_file_id" | jq -r '.name')
            dx download "$config_file_id" -o "low_level_configs/${config_name}"
        else
            config_name=$(dx describe --json "$config_file_id" | jq -r '.name')
            dx download "$config_file_id" -o "low_level_configs/${config_name}"
        fi
        assay_to_config[$k]+="$config_name"
    done

    # perform samplesheet validation unless set to False
    if [ "$validate_samplesheet" = true ]; then
        mark-section "Performing samplesheet validation"
        # get all regex patterns from low level config files to check
        # sample names against from config(s) if given
        regex_patterns=""
        for file in low_level_configs/*.json; do
            if jq -r '.sample_name_regex' "$file"; then
                # parse regex patterns into one string and add to string of total patterns
                file_regexes=$(jq -rc '.sample_name_regex[]' "$file" | tr '\n' ' ')
                if [ "$file_regexes" != " " ]; then regex_patterns+="$file_regexes"; fi
            fi
        done

        # run samplesheet validator
        if [[ $regex_patterns ]]; then
            stdout=$(python3 validate_sample_sheet-1.0.0/validate/validate.py \
            --samplesheet "$sample_sheet" --name_patterns $regex_patterns) &> /dev/null
        else
            stdout=$(python3 validate_sample_sheet-1.0.0/validate/validate.py \
            --samplesheet "$sample_sheet") &> /dev/null
        fi


        if [[ "$stdout" =~ "SUCCESS" ]]; then
            # check for errors found in samplesheet => exit
            printf 'Errors found in sample sheet, please check logs for details.'
            printf 'Exiting now.'
            printf '$stdout'
            dx-jobutil-report-error "Errors found in sample sheet"
            exit 1
        fi
    fi


    exit 1

    if [ "$sentinel_file" ] && [ -z "$bcl2fastq_job_id" ]
    then
        # starting bcl2fastq and holding app until it completes
        mark-section "Demultiplexing with bcl2fastq"
        if [ -z "$bcl2fastq_out" ]; then 
            # bcl2fastq output path not set, default to putting it in the parent run dir of the
            # sentinel record
            bcl2fastq_out=$(dx describe --json "$sentinel_file" | jq -r '.details.dnanexus_path')
            bcl2fastq_out=${bcl2fastq_out%runs}
        fi

        # check no fastqs are already present in the output directory for bcl2fastq, exit if any
        # present to prevent making a mess with bcl2fastq output
        fastqs=$(dx find data --json --brief --name "*.fastq*" --path $bcl2fastq_out)
        if [ ! "$fastqs" == "[]" ]; then
            printf "Selected output directory already contains fastq files: %s" "$bcl2fastq_out"
            printf "This is likely because demultiplexing has already been run and output to this directory."
            printf "Exiting now to prevent poluting output directory with bcl2fastq output."

            dx-jobutil-report-error "Selected bcl2fastq output directory already contains fastq files"
            exit 1
        fi

        optional_args=""
        optional_args+="--destination ${bcl2fastq_out}"

        echo "Starting bcl2fastq app with output at: $bcl2fastq_out"
        echo "Holding app until demultiplexing complete to trigger downstream workflow(s)..."

        bcl2fastq_job_id=$(dx run --brief --detach --wait -y ${optional_args} --auth-token $API_KEY \
            "$BCL2FASTQ_APP_ID" -iupload_sentinel_record="$sentinel_file_id")
    fi

    # trigger workflows using config for each set of samples for an assay
    for k in "${!sample_to_assay[@]}"
    do
        mark-section "Triggering workflows"
        printf "Calling workflow for assay $i on samples:\n ${sample_to_assay[$k]}"

        # set optional arguments to workflow script by app args
        optional_args=""
        if [ "$dx_project" ]; then optional_args+="--dx_project_id $dx_project "; fi
        if [ "$bcl2fastq_job_id" ]; then optional_args+="--bcl2fastq_id $bcl2fastq_job_id "; fi
        if [ "$fastq_ids" ]; then optional_args+="--fastqs $fastq_ids"; fi
        if [ "$run_id" ]; then optional_args+="--run_id $run_id "; fi
        if [ "$development" ]; then optional_args+="--development "; fi

        python3 run_workflows.py --config_file "low_level_configs/${assay_to_config[$k]}" \
        --samples "${sample_to_assay[$k]}" --assay_code "$k" $optional_args
    done

    echo "Workflows triggered for samples"
    mark-success
}
