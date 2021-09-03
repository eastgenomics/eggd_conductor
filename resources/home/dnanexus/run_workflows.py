"""
Script to call workflow(s) / apps from a given config

Jethro Rainford 210902
"""
import argparse
from contextlib import redirect_stdout
from datetime import datetime
from io import StringIO
import json
import os
import sys

import dxpy


def time_stamp() -> str:
    """
    Returns string of date & time
    """
    return datetime.now().strftime("%Y%m%d_%H%M")


def load_config(config_file) -> dict:
    """
    Read in given config json to dict
    """
    with open(config_file) as file:
        config = json.load(file)

    return config


def parse_args():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(
        description="Trigger workflows from given config file"
    )

    parser.add_argument(
        '--config_file', required=True
    )
    parser.add_argument(
        '--samples', required=True,
        help='list of sample names to run analysis on'
    )
    parser.add_argument(
        '--dx_project', required=False,
        help='DNAnexus project to use to run analysis in'
    )
    parser.add_argument(
        '--run_id',
        help='id of run parsed from sentinel file, used to create output project'
    )
    parser.add_argument(
        '--assay_code',
        help='assay code, used for naming outputs'
    )
    parser.add_argument(
        '--bcl2fastq_id',
        help='id of job from running bcl2fastq (if run)'
    )

    args = parser.parse_args()

    # turn comma separated sample str to python list
    args.samples = [x.replace(' ', '') for x in args.samples.split(',')]

    return args


def main():
    """
    Main function to run workflows
    """
    args = parse_args()
    # print(args)

    config = load_config(args.config_file)

    if args.bcl2fastq_id:
        # get details of job that ran to perform demultiplexing
        bcl2fastq_job = dxpy.bindings.dxjob.DXJob(
            dxid=args.bcl2fastq_job).describe()
        bcl2fastq_project = bcl2fastq_job['project']
        bcl2fastq_folder = bcl2fastq_job['folder']

        # find all fastqs from bcl2fastq job and return list of dicts with full
        # details, keep name and file ids as list of tuples
        fastq_details = list(dxpy.search.find_data_objects(
            name="*.fastq*", name_mode="glob", project=bcl2fastq_project,
            folder=bcl2fastq_folder, describe=True
        ))
        fastq_details = [(x['id'], x['describe']['name']) for x in fastq_details]
    else:
        # if we're here it means bcl2fastq wasn't run, so we have either a dir
        # of fastqs being passed, this is for tso500 or something else weird
        # this is going to need some thought and clever handling to know
        # what is being passed

        pass


    for executable, params in config['executables']:
        # for each workflow/app, check if its per sample or all samples and
        # run correspondingly
        if params['per_sample']:
            # run workflow / app on every sample
            print(f'Calling {params["name"]} per sample')

            # loop over given samples, find data and run workflows
            for sample in args.samples:
                sample_fastqs = [x for x in fastq_details if sample in x[1]]
        else:
            # passing all samples to workflow
            pass

    sys.exit()

    if not args.dx_project:
        # output project not specified, create new one from run id
        output_project = f'002_{args.run_id}_{args.assay_code}'

        # create new project and capture returned project id and store
        project_id = StringIO()
        with redirect_stdout(project_id):
            dxpy.bindings.dxproject.DXProject().new(
                name=output_project,
                summary=f'Analysis of run {args.run_id} with {args.assay_code}'
            )
        project_id = project_id.getvalue()

        print(f'Created new project for output: {output_project}')


    

if __name__ == "__main__":
    main()
