#!/bin/bash
# eggd_conductor

# This can either be run automatically by dx-streaming-upload on completing
# upload of a sequencing run, or manually by providing a set of fastqs 
# to analyse and a sample sheet / list of sample names

set -exo pipefail

main() {

    if [[ "$sentinel_file" ]]; then
        # sentinel file passed when run automatically via dx-streaming-upload

        # get json of details to parse required info from
        sentinel_details=$(dx describe --details --json "$sentinel_file")
        run_id=$(echo $sentintel_details | jq -r '.details.run_id')
        run_dir=$(echo $sentinel_details | jq -r '.folder')
        tar_file_ids=$(echo $sentinel_details | jq -r '.details.tar_file_ids')
        sample_sheet_id=$(dx find data --path "$run_dir" --name 'SampleSheet.csv' --brief)

        if [ $sample_sheet_id ];
        then
            # sample sheet named correctly, download
            printf 'Found sample sheet from given sentinel file, downloading'
            dx download $sample_sheet_id
            sample_sheet='SampleSheet.csv'
        else
            # sample sheet missing from runs dir, most likely due to being named incorrectly
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
        fi
    fi

    # if it has made it this far we have a sample sheet (or string of names), and
    # either a sentinel record or list of fastq files

    # we now need to do some sample sheet validation first, then check what types of samples
    # are present to determine the workflow(s) to run

    if [[ $sample_sheet ]] & [[ ! $samples ]]
    then
        printf 'validating sample sheet'
        #### SAMPLE SHEET VALIDATION HERE ####
    fi

    # now we need to know what samples we have, to trigger the appropriate workflows
    # use the config and sample sheet names to infer which to use if a specific assay not passed
    
    # get high level config file if type not specified
    if [ -z $assay_type ]
    then
        printf 'assay type not specified, inferring from high level config and sample names'
        # dx download $high_level_config

        # hard coding test config for now
        dx download 'file-G5FVZBQ469FQF1zF88Z0yyyF' -o high_level_config.tsv
    fi

    # get list of sample names from sample sheet if not using sample name list
    if [[ $sample_sheet ]] & [[ ! $samples ]]
    then
        sample_list=$(tail -n +22 "$sample_sheet" | cut -d',' -f 1)
    fi

    # build associative array (i.e. key value pairs) of samples to assays
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

    echo "READY TO RUN BCL2FASTQ"
    echo "sample list: $sample_list"
    echo "assay code given: $assay_type"
    echo "sample to assays:"

    for i in "${!sample_to_assay[@]}"
    do
        echo "key  : $i"
        echo "value: ${sample_to_assay[$i]}"
    done

    exit 1

    # we now have an array of assay codes to comma separated sample names i.e.
    # FH: X000001_EGG3, X000002_EGG3...
    # TSOE: X000003_EGG1, X000004_EGG1...

    # access all array keys with ${!sample_to_assay[@]}
    # access all array value with ${sample_to_assay[@]}
    # access specific key value with ${sample_to_assay[$key]}

    # starting bcl2fastq and holding app until it completes
    echo "Starting bcl2fastq app"
    echo "Holding app until demultiplexing complete to trigger downstream workflow(s)"

    if [ $upload_sentinel_record ]
    then
        # running using sentinel record => from dx-streaming-upload
        bcl2fastq_job_id=$(dx run --brief --wait --yes {bcl2fastq-applet-id} -iupload_sentinel_record $upload_sentinel_record)
        # dx describe --json $job_id | jq -r '.output.output'  # file ids of all outputs
    fi

    # trigger workflows using config for each set of samples for an assay
    for i in "${sample_to_assay[@]}"
    do
        echo "Calling workflow for assay $i on samples ${sample_to_assay[$i]}"

        if [ ! $custom_config ]
        then
            # no custom config => get file id for config file & download
            config_file_id=$(grep $i $high_level_config | awk '{print $NF}')
            config_name=$(dx describe --json $config_file_id | jq -r '.name')
            dx download $config_file_id -o $config_name
        else
            config_name=$(dx describe --json $config_file | jq -r '.name')
            dx download $config_file -o $config_name
        fi

        optional_args=''
        if [ $dx_project ]; then optional_args+="--dx_project_id $dx_project "; fi
        if [ $bcl2fastq_job_id ]; then optional_args+="--bcl2fastq_id $bcl2fastq_job_id"; fi
        if [ $run_id ]; then optional_args+="--run_id $run_id "; fi

        python3 run_workflows.py --config_file $config_name --samples "${sample_to_assay[$i]}" --assay_code "$i" "$optional_args"
    done

    echo "Workflows triggered for samples"
}
