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

    if [[ "$sentinel_file" ]]; then
        # sentinel file passed when run automatically via dx-streaming-upload

        # get json of details to parse required info from
        sentinel_details=$(dx describe --details --json "$sentinel_file")
        run_id=$(echo $sentintel_details | jq -r '.details.run_id')
        run_dir=$(echo $sentinel_details | jq -r '.folder')
        tar_file_ids=$(echo $sentinel_details | jq -r '.details.tar_file_ids')
        
        if [ ! $sample_sheet_id ]; then
            # check if sample sheet has been specified on running
            sample_sheet_id=$(dx find data --path "$run_dir" --name 'SampleSheet.csv' --brief)
        fi

        if [ $sample_sheet_id ]
        then
            # found sample sheet or passed -> download
            printf 'Found sample sheet from given sentinel file, downloading'
            dx download $sample_sheet_id
            sample_sheet='SampleSheet.csv'
        else
            # sample sheet missing from runs dir, most likely due to not being name SampleSheet.csv
            # find the first tar, download, unpack and try to find it
            printf 'Could not find samplesheet from sentinel file.\n'
            printf 'Finding first run tar file to get sample sheet from.\n'

            # tar file ids sadly aren't in order, loop over them,
            # call dx describe and find first
            echo $tar_file_ids | jq -c '.[]' | while read i; do
                file_id=$(echo $i | sed 's/"//g')  # remove quotes from file id
                tar_name=$(dx describe --json $file_id | jq -r '.name')
                if [[ "$tar_name" == *"_000.tar.gz" ]]
                then
                    # first tar always will be _000
                    printf 'Found first tar: $tar_name'
                    printf 'Downloading tar file'
                    dx download $file_id -o first_tar.tar.gz
                    break
                else
                    printf 'Not first tar file, continuing...\n'
                fi
            done

            # unpack tar and find samplesheet
            mkdir ./first_tar_dir
            tar -xzf first_tar.tar.gz -C ./first_tar_dir
            sample_sheet=$(find ./first_tar_dir -regextype posix-extended  -iregex '.*sample[-_ ]?sheet.csv$')

            if [ -z $sample_sheet ];
            then
                # sample sheet missing from root and first tar
                echo "Sample sheet missing from runs dir and first tar, exiting now."
                exit 1
            fi
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
                    printf "Given fastq does not seem to be a valid file id: $fq\n"
                    printf "Exiting now."
                    exit 1
                fi
            done
            printf "\nFound fastq file ids: $fastq_ids\n"
        fi

        if [ ! $samples ] & [ ! $sample_sheet ]
        # needs sample sheet or sample list passing
        then
            printf 'No sample sheet or list of samples defined, one of these must be provided. Exiting now.'
            exit 1
        fi

        if [[ $samples ]] & [[ $sample_sheet ]]
        # samplesheet and sample list given, use list
        then
            printf 'both sample sheet and list of samples provided, using sample list'
            # ensure sample names are string seperated by a space
            sample_list=$(echo $samples | tr -d '[:space:]' | sed 's/,/ /g')
            printf 'Samples specified for analysis: $sample_list'
        fi

        if [[ $sample_sheet ]] & [[ ! $samples ]]
        # just sheet given => download
        then
            dx download $sample_sheet
            # get list of sample names from sample sheet if not using sample name list
            sample_list=$(tail -n +22 "$sample_sheet" | cut -d',' -f 1)
        fi
    fi

    # now we need to know what samples we have, to trigger the appropriate workflows
    # use the config and sample sheet names to infer which to use if a specific assay not passed
    
    # get high level config file if assay type not specified
    if [ -z $assay_type ]
    then
        printf 'assay type not specified, inferring from high level config and sample names'
        # dx download $high_level_config

        # hard coding test config from dev project for now
        dx download 'file-G5FYPp8469Fqvz8vP5VVVvQV' -o high_level_config.tsv
    fi

    # build associative array (i.e. key value pairs) of samples to assay EGG codes
    declare -A sample_to_assay

    # for each sample, parse the eggd code, get associated assay from config if not specified
    for name in $sample_list;
    do
        if [ -z $assay_type ]; then
            # assay type not specified, infer from eggd code and high level config
    
            # get eggd code from name using regex, if not found will exit
            if [[ $name =~ EGG[0-9]{1,2} ]]
            then 
                sample_eggd_code="${BASH_REMATCH[0]}"
            else
                printf 'Could not parse EGG code from name: $name \n'
                printf 'Exiting now.'
                exit 1
            fi

            # get associated assay from config
            assay_type=$(grep $sample_eggd_code high_level_config.tsv | awk '{print $2}')

            if [ -z $assay_type ]
            then
                # appropriate assay not found from sample egg code
                echo 'Assay for sample $name not found in config using the following eggd code: $sample_eggd_code'
                echo 'Exiting now.'
                exit 1
            fi
        fi
        # add sample name to associated assay code in array, comma separated to parse later
        sample_to_assay[$assay_type]+="$name,"
    done

    # get low level config files for appropriate assays
    mkdir low_level_configs

    for i in "${sample_to_assay[@]}"
    do
        echo "Downloading low level config file(s)"
        if [ ! $custom_config ]
        then
            # no custom config => get file id for low level assay config file & download
            config_file_id=$(grep $i high_level_config.tsv | awk '{print $NF}')
            config_name=$(dx describe --json $config_file_id | jq -r '.name')
            dx download $config_file_id -o "low_level_configs/${config_name}"
        else
            config_name=$(dx describe --json $config_file | jq -r '.name')
            dx download $config_file -o "low_level_configs/${config_name}"
        fi
    done

    # perform samplesheet validation unless set to False
    if [ $validate_samplesheet ]; then
        # get all regex patterns from low level config files to check
        # sample names against from config(s) if given
        regex_patterns=""
        for file in low_level_configs/*.json; do
            if jq -r '.sample_regex' $file;
                file_regexes=$(jq -r '.sample_regex[]')
                regex_patterns+=" $file_regexes"
            fi
        done

        # run samplesheet validator
        if [[ $regex_patterns ]]; then
            stdout=$(python validate_sample_sheet-1.0.0/validate/validate.py \
            --samplesheet $samplesheet --name_patterns "$regex_patterns")
        else
            stdout=$(python validate_sample_sheet-1.0.0/validate/validate.py \
            --samplesheet $samplesheet)
        fi

        printf "$stdout"

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

    if [ $upload_sentinel_record ]
    then
        # starting bcl2fastq and holding app until it completes
        echo "Starting bcl2fastq app"
        echo "Holding app until demultiplexing complete to trigger downstream workflow(s)"

        bcl2fastq_job_id=$(dx run --brief --wait --yes {bcl2fastq-applet-id} -iupload_sentinel_record $upload_sentinel_record)
        # dx describe --json $job_id | jq -r '.output.output'  # file ids of all outputs
    fi

    # trigger workflows using config for each set of samples for an assay
    for i in "${sample_to_assay[@]}"
    do
        echo "Calling workflow for assay $i on samples ${sample_to_assay[$i]}"

        # set optional arguments to workflow script by app args
        optional_args=""
        if [ $dx_project ]; then optional_args+="--dx_project_id $dx_project "; fi
        if [ $bcl2fastq_job_id ]; then optional_args+="--bcl2fastq_id $bcl2fastq_job_id"; fi
        if [ $fastq_ids ]; then optional_args+="--fastqs $fastq_ids"; fi
        if [ $run_id ]; then optional_args+="--run_id $run_id "; fi

        echo 'python3 run_workflows.py --config_file $config_name --samples "${sample_to_assay[$i]}" --assay_code "$i" "$optional_args"'
    done

    echo "Workflows triggered for samples"
}
