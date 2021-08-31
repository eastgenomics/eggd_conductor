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
            # sample sheet not given, find in data fir
            # need to find with dx find data and also check in tars if tarred data given
            # TODO
    fi

    # if it has made it this far we have a sample sheet, and either a sentinel record or
    # directory of data if run manually

    # we now need to do some sample sheet validation first, then check what types of samples
    # are present to determine the workflow(s) to run

    #### SAMPLE SHEET VALIDATION HERE


    # now we need to know what samples we have, to trigger the appropriate workflows
    # use the config and sample sheet names



}
