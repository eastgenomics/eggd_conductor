#!/bin/bash
# eggd_conductor

# This can either be run automatically by dx-streaming-upload on completing
# upload of a sequencing run, or manually by providing a set of fastqs 
# to analyse and a sample sheet / list of sample names

set -exo pipefail

main() {

    # our own sample sheet validator is bundled with the app
    # https://github.com/eastgenomics/validate_sample_sheet
    tar xf validate_sample_sheet_v1.0.0.tar.gz
    tar xf packages/python_packages.tar.gz

    pip3 install -q --user six-* pytz-* python_dateutil-* numpy-* pandas-*

    if [[ "$sentinel_file" ]]; then
        # sentinel file passed when run automatically via dx-streaming-upload

        # get json of details to parse required info from
        sentinel_details=$(dx describe --json "$sentinel_file")
        run_id=$(jq -r '.details.run_id' <<< "$sentinel_details")
        run_dir=$(jq -r '.folder' <<< "$sentinel_details")
        sentinel_path=$(jq -r '.details.dnanexus_path' <<< "$sentinel_details")
        sentinel_file_id=$(jq -r '.id' <<< "$sentinel_details")
        
        if [ ! "$sample_sheet_id" ]; then
            # check if sample sheet has been specified on running
            sample_sheet_id=$(dx find data --path "$run_dir" --name 'SampleSheet.csv' --brief)
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
                exit 1
            fi

            # parse list of samples from samplesheet
            sample_list=$(tail -n +22 "$sample_sheet" | cut -d',' -f 1)
        fi
    else
        # applet run manually without sentinel file
        # should have array of fastqs and sample sheet or sample names
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
                exit 1
            fi

            # get associated assay from config
            assay_type=$(grep "$sample_eggd_code" high_level_config.tsv | awk '{print $2}')

            if [ -z "$assay_type" ]
            then
                # appropriate assay not found from sample egg code
                echo "Assay for sample $name not found in config using the following eggd code: $sample_eggd_code"
                echo "Exiting now."
                exit 1
            fi
        fi
        # add sample name to associated assay code in array, comma separated to parse later
        sample_to_assay[$assay_type]+="$name,"
    done

    printf "\nsample to assays:\n"

    for i in "${!sample_to_assay[@]}"
    do
        echo "key  : $i"
        echo "value: ${sample_to_assay[$i]}"
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
        # get all regex patterns from low level config files to check
        # sample names against from config(s) if given
        regex_patterns=""
        for file in low_level_configs/*.json; do
            if jq -r '.sample_name_regex' "$file"; then
                # the returned string here has new line characters and double quotes, when passed
                # to the python script this needs formatting as space separated strings with
                # single quotes. This is a pretty big mess to do it but it works so...
                file_regexes=$(jq -c '.sample_name_regex[]' "$file" | tr '\n' ' ' | sed s"/\"/'/g" | sed -e 's/[[:space:]]*$//')
                
                # remove first and last quote as bash handily adds its own around the string, thus
                # it ends up with a double at the beginning and end
                # file_regexes="${file_regexes:1:${#file_regexes}-2}"
                if [ "$file_regexes" != " " ]; then regex_patterns+="$file_regexes"; fi
            fi
        done

        # run samplesheet validator
        if [[ $regex_patterns ]]; then
            stdout=$(python3 validate_sample_sheet-1.0.0/validate/validate.py \
            --samplesheet "$sample_sheet" --name_patterns "$regex_patterns")
        else
            stdout=$(python3 validate_sample_sheet-1.0.0/validate/validate.py \
            --samplesheet "$sample_sheet")
        fi

        printf '%s' "$stdout"

        if [[ "$stdout" != "*SUCCESS*" ]]; then
            # errors found in samplesheet => exit
            printf 'Errors found in sample sheet, please check logs for details.\nExiting now.'
            exit 1
        fi
    fi

    echo "READY TO RUN BCL2FASTQ"
    echo "sample list: $sample_list"
    echo "assay code given: $assay_type"
    echo "sample to assays:"

    for i in "${!sample_to_assay[@]}"
    do
        echo "key  : $i"
        echo "value: ${sample_to_assay[$i]}"
    done

    # we now have an array of assay codes to comma separated sample names i.e.
    # FH: X000001_EGG3, X000002_EGG3...
    # TSOE: X000003_EGG1, X000004_EGG1...

    # access all array keys with ${!sample_to_assay[@]}
    # access all array value with ${sample_to_assay[@]}
    # access specific key value with ${sample_to_assay[$key]}

    if [ "$sentinel_file" ]
    then
        # starting bcl2fastq and holding app until it completes
        echo "Starting bcl2fastq app"
        echo "Holding app until demultiplexing complete to trigger downstream workflow(s)..."

        optional_args=""
        if [ -z "$bcl2fastq_out" ]; then 
            # bcl2fastq output path not set, default to putting it in the parent run dir of the
            # sentinel record
            bcl2fastq_out=$(dx describe --json "$sentinel_file" | jq -r '.details.dnanexus_path')
            bcl2fastq_out=${bcl2fastq_out%runs}
        fi

        echo "bcl2fastq output path: $bcl2fastq_out"

        optional_args+="--destination $bcl2fastq_out"

        bcl2fastq_job_id=$(dx run --brief --wait -y "${optional_args}" \
            applet-G4F8kk0433GxjKp9J9g3Fzq9 -iupload_sentinel_record="$sentinel_file_id")
    fi

    exit 1

    # trigger workflows using config for each set of samples for an assay
    for k in "${!sample_to_assay[@]}"
    do
        echo "Calling workflow for assay $i on samples_names ${sample_to_assay[$k]}"

        # set optional arguments to workflow script by app args
        optional_args=""
        if [ "$dx_project" ]; then optional_args+="--dx_project_id $dx_project "; fi
        if [ "$bcl2fastq_job_id" ]; then optional_args+="--bcl2fastq_id $bcl2fastq_job_id"; fi
        if [ "$fastq_ids" ]; then optional_args+="--fastqs $fastq_ids"; fi
        if [ "$run_id" ]; then optional_args+="--run_id $run_id "; fi
        if [ "$development" ]; then optional_args+="--development "; fi

        python3 run_workflows.py --config_file "${assay_to_config[$k]}" \
        --samples "${sample_to_assay[$k]}" --assay_code "$k" "$optional_args"
    done

    echo "Workflows triggered for samples"
}
