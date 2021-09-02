#!/bin/bash
# eggd_conductor

# this can either be run automatically by dx-streaming-upload on completing
# upload of a sequencing run, or manually by providing a directory of data 
# to analyse and (optionally) a sample sheet

set -exo pipefail

main() {

    if [ $upload_sentinel_record ]; then
        # sentinel file passed when run automatically via dx-streaming-upload

        # get json of details to parse required info from
        sentinel_details=$(dx describe --details --json "$upload_sentinel_record")
        run_dir=$(echo $sentinel_details | jq -r '.folder')
        tar_file_ids=$(echo $sentinel_details | jq -r '.details.tar_file_ids')
        sample_sheet_id=$(dx find data --path "$run_dir" --name 'SampleSheet.csv' --brief)

        if [ $sample_sheet_id ];
        then
            # sample sheet named correctly, download
            dx download $sample_sheet_id
            sample_sheet='SampleSheet.csv'
        else
            # sample sheet missing from runs dir, most likely due to being named incorrectly
            # grab the first tar, download, unpack and try to find it
            dx download $(echo $tar_file_ids | jq -r '.[0]')
            first_tar=$(find ./ -name "run.*.tar.gz")
            tar -xzf $first_tar
            sample_sheet=$(find ./ -regextype posix-extended  -iregex '.*sample[-_ ]?sheet.csv$')

            if [ -z $sample_sheet ];
            then
                # sample sheet missing from root and first tar
                echo "Sample sheet missing from runs dir and first tar, exiting now."
                exit 1
            fi
        fi
    else
        # applet run manually, should pass dir of data and (optionally) sample sheet
        if [ $sample_sheet ];
        then
            dx download $sample_sheet
        else
            # sample sheet not given, find in data dir
            # need to find with dx find data and also check in tars if tarred data given
            # TODO
    fi

    # if it has made it this far we have a sample sheet, and either a sentinel record or
    # directory of data if run manually

    # we now need to do some sample sheet validation first, then check what types of samples
    # are present to determine the workflow(s) to run


    #### SAMPLE SHEET VALIDATION HERE ####


    # now we need to know what samples we have, to trigger the appropriate workflows
    # use the config and sample sheet names to infer which to use if a specific assay not passed
    

    # get config file
    dx download $high_level_config

    # get list of sample names from sample sheet
    sample_list=$(tail -n +20 "$sample_sheet" | cut -d',' -f 1)

    # build associative array (i.e. key value pairs) of samples to assays
    declare -A sample_to_assay

    # for each sample, parse the eggd code, get associated assay from config if not specified
    for name in $sample_list;
    do
        if [ -z $assay_type ]; then
            # assay type not specified, infer from eggd code and high level config
    
            # get eggd code from end of name for now
            sample_eggd_code=$(echo $name | awk -F_ '{print $NF}')

            # get associated assay from config
            assay_type=$(grep $sample_eggd_code $high_level_config | awk '{print $2}')

            if [ -z $assay_type ];
            then
                # appropriate assay not found from sample egg code
                echo 'Assay for sample $name not found in config using the following eggd code: $sample_eggd_code'
                echo 'Exiting now.'
                exit 1
            fi
        fi

        # add sample name to associated assay code in array, comma separated to parse later
        if [ -v sample_to_assay[$assay_type] ];
        then
            # assay key already exists => append to existing sample list
            sample_to_assay["$assay_type"]="sample_to_assay[$assay_type], $name"
        else
            # new assay key
            sample_to_assay["$assay_type"]="$name"
        fi
    done


    # we now have an array of assay codes to comma separated sample names i.e.
    # FH: X000001_EGG3, X000002_EGG3...
    # TSOE: X000003_EGG1, X000004_EGG1...

    # access all array keys with ${!sample_to_assay[@]}
    # access all array value with ${sample_to_assay[@]}
    # access specific key value with ${sample_to_assay[$key]}

    # issue - need to split the output of bcl2fastq by sample to start appropriate workflow
        # solution A - hold this app until bcl2fastq finishes, then get file ids of the output
        #               and start appropriate workflows, not ideal but it only runs for ~ 1 1/2
        #               hours and small instance is cheap so ¯\_(ツ)_/¯


    #### start bcl2fastq app, either with sentinel file if run from dx-streaming-upload or
    #### a directory of tars (+ optionally a sample sheet)
    #### can also skip bcl2fastq if it is run with a dir of fastqs - TODO

    echo "Starting bcl2fastq app"
    echo "Holding app until demultiplexing complete to trigger downstream workflow(s)"

    if [ $upload_sentinel_record ];
    then
        # running using sentinel record => from dx-streaming-upload
        job_id=$(dx run --brief --wait --yes {bcl2fastq-applet-id} -iupload_sentinel_record $upload_sentinel_record)
    elif [ $data_dir ];
    then
        # running from dir of tars
        job_id=$(dx run --brief --wait --yes {bcl2fastq-applet-id} -irun_archive)
    fi

    # now we have run bcl2fastq and the output is an array of files
    # we need to match the fastqs output for the samples and start the workflow(s)

    dx describe --json $job_id | jq -r '.output.output'  # file ids of all outputs

    # now the fun begins of triggering each workflow type, not sure what to do for now...

    # what we need:
    # low level configs for each assay
    # check correct data for each sample available
    # check if it is already demultiplexed or need to run bcl2fastq
    # trigger each workflow(s) for each set of samples

    # Option A:
        # write a python script to parse the low level config for an assay, and call the
        # workflow / app from the config and other inputs (sample name / fastqs etc.)
        # needs to be able to handle all workflows and extra inputs etc.

    # trigger workflows using config for each set of samples for an assay
    for i in "${sample_to_assay[@]}"
    do
        echo "Calling workflow for assay $i on samples ${sample_to_assay[$i]}"

        # get file id for config file & download
        config_file_id=$(grep $i $high_level_config | awk '{print $NF}')
        config_name=$(dx describe --json $config_file_id | jq -r '.name')
        dx download $config_file_id

        python3 run_workflows.py --config_file $config_name --samples "${sample_to_assay[$i]}"
    done

}
