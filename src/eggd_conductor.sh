#!/bin/bash
# eggd_conductor

# this can either be run automatically by dx-streaming-upload on completing
# upload of a sequencing run, or manually by providing a directory of data 
# to analyse and (optionally) a sample sheet

set -exo pipefail

main() {

    if [ $upload_sentinel_record ]; then
        # sentinel file specified when run via dx-streaming-upload

        # do things
        # - get sample sheet by file id from sentinel file

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
    # use the config and sample sheet names

    #### DO SOME MAGIC WITH A CONFIG FILE TO SET CORRECT ASSAYS

    if [ -z $assay_type ];
    then
        # assay type not specified, use sample names and config file
        sample_list=$(tail -n +20 "$sample_sheet" | cut -d',' -f 1)

        # build array of samples to assays
        declare -A sample_to_assay

        # for each sample, parse the eggd code, get associated assay from config and build array of assay -> sample names
        for name in $sample_list;
        do
            sample_eggd_code=$(echo $name | awk -F_ '{print $NF}')  # get eggd code from end of name for now
            sample_assay=$(grep $sample_eggd_code $config_file | awk '{print $NF}')  # get associated assay from config

            if [ -z $sample_assay ];
            then
                # appropriate assay not found from sample egg code
                echo 'Assay for sample $name not found in config using the following eggd code: $sample_eggd_code'
                echo 'Exiting now.'
                exit 1
            fi

            # add sample name to associated assay code in array, comma separated to parse later
            if [ -v sample_to_assay[$sample_assay] ];
            then
                # assay key already exists => append to existing sample list
                sample_to_assay["$sample_assay"]="sample_to_assay[$sample_assay], $name"
            else
                # new assay key
                sample_to_assay["$sample_assay"]="$name"
            fi
        done
    fi

    # we now have an array of assay codes to sample names i.e.
    # FH: X000001_EGG3, X000002_EGG3...
    # TSOE: X000001_EGG1, X000002_EGG1...

    # access all array keys with ${!sample_to_assay[@]}
    # access all array value with ${sample_to_assay[@]}
    # access specific key value with ${sample_to_assay[$key]}

    # now the fun begins of triggering each workflow type, not sure what to do for now...

    # what we need:
    # low level configs for each assay
    # check correct data for each sample available
    # check if it is already demultiplexed or need to run bcl2fastq
    
    

}
