"""
Script to call workflow(s) / apps from a given config

Jethro Rainford 210902
"""
import argparse
from copy import deepcopy
from contextlib import redirect_stdout
from datetime import datetime
from io import StringIO
import json
import os
import sys

import dxpy
from dxpy.bindings.search import find_projects


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


def find_project(project_name):
    """
    Check if project already exists in DNAnexus
    """
    dx_projects = list(dxpy.bindings.search.find_projects(
        name=project_name
    ))

    if len(dx_projects) == 0:
        # found no project => create new one
        return None

    if len(dx_projects) > 1:
        # this shouldn't happen as projects shouldn't be named the same,
        # print warning and select first
        names = ', '.join([x['describe']['name'] for x in dx_projects])
        print('WARNING')
        print(f'More than one project found for name: {project_name}: {names}')
        print(f'Using project: {dx_projects[0]["describe"]["name"]}')

    return dx_projects[0]['id']



def create_project(args):
    """
    Create new project in DNAnexus
    """
    output_project = f'002_{args.run_id}_{args.assay_code}'

    project_id = find_project(output_project)

    if not project_id:
        # create new project and capture returned project id and store
        project_id = StringIO()
        with redirect_stdout(project_id):
            dxpy.bindings.dxproject.DXProject().new(
                name=output_project,
                summary=f'Analysis of run {args.run_id} with {args.assay_code}'
            )
        project_id = project_id.getvalue()

        print(f'Created new project for output: {output_project}')
    else:
        print(f'Using existing found project: {output_project} ({project_id})')

    args.dx_project_id = project_id

    return args


def call_dx_run(args, executable, input_dict):
    """
    Call workflow / app, returns id of submitted job
    """
    job_id = StringIO()
    with redirect_stdout(job_id):
        dxpy.bindings.dxworkflow.DXWorkflow(
            dxid=executable, project=args.dx_project
        ).run(workflow_input=input_dict)

    job_id = job_id.getvalue()

    return job_id


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
    run_time = time_stamp()

    if not args.dx_project:
        # output project not specified, create new one from run id
        args = create_project(args)

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
        fastq_details = []

        # test data
        fastq_details = [
            ('file-Fykqgj84X7kV5VQ33fXVvzFV', 'X211628_S19_L003_R1_001.fastq.gz'),
            ('file-Fykqgp04X7kZPXQ6JzYjV5kj', 'X211628_S19_L004_R1_001.fastq.gz'),
            ('file-Fykqgkj4X7kV5VQ33fXVvzFY', 'X211628_S19_L003_R2_001.fastq.gz'),
            ('file-Fykqgk84X7kkXzk73fV6jZ6j', 'X211628_S19_L004_R2_001.fastq.gz')
        ]

    for executable, params in config['executables'].items():
        # for each workflow/app, check if its per sample or all samples and
        # run correspondingly

        # create output folder for workflow, unique by datetime stamp
        workflow_out_folder = f'{params["name"]}-{run_time}'
        dxpy.bindings.dxproject.DXContainer(
            dxid=args.dx_project_id).new_folder(workflow_out_folder)

        if params['per_sample']:
            # run workflow / app on every sample
            print(f'Calling {params["name"]} per sample')

            # loop over given samples, find data, add to config and call workflow
            for sample in args.samples:
                # copy config to add sample info to for calling workflow
                # select input dict from config for current workflow / app
                sample_config = deepcopy(config)
                input_dict = sample_config['executables'][executable]['inputs']

                sample_fastqs = [x for x in fastq_details if sample in x[1]]

                # fastqs should always be named with R1/2_001
                r1_fastqs = [x for x in sample_fastqs if 'R1_001.fastq' in x[1]]
                r2_fastqs = [x for x in sample_fastqs if 'R2_001.fastq' in x[1]]

                print(f'Found {len(r1_fastqs)} R1 fastqs and {len(r2_fastqs)} R2 fastqs')

                for stage, inputs in input_dict.items():
                    # check each stage in input config for fastqs, format
                    # as required with R1 and R2 fastqs
                    if inputs == 'INPUT-R1':
                        r1_input = [{"$dnanexus_link": x[0]} for x in r1_fastqs]
                        input_dict[stage] = r1_input

                    if inputs == 'INPUT-R2':
                        r2_input = [{"$dnanexus_link": x[0]} for x in r2_fastqs]
                        input_dict[stage] = r2_input

                    if inputs == 'INPUT-R1-R2':
                        # stage requires all fastqs, build one list of dicts
                        r1_r2_input = []
                        r1_r2_input.extend([{"$dnanexus_link": x[0]} for x in r1_fastqs])
                        r1_r2_input.extend([{"$dnanexus_link": x[0]} for x in r2_fastqs])
                        input_dict[stage] = r1_r2_input


                # check if config has field(s) expecting the sample name as input
                keys = [k for k, v in input_dict.items() if v == 'INPUT-SAMPLE_NAME']
                if keys:
                    for key in keys:
                        input_dict[key] = sample


                # TODO: create the output folder dir structure here and pass to run() below
                # http://autodoc.dnanexus.com/bindings/python/current/dxpy_apps.html#dxpy.bindings.dxworkflow.DXWorkflow

                # call workflow for given sample wth configured input dict
                call_dx_run(args, executable, input_dict)

        else:
            # passing all samples to workflow
            pass




if __name__ == "__main__":
    main()
