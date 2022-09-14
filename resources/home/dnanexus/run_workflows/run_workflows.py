"""
Using a JSON config, calls all workflows / apps defined in config for
given samples.

Handles correctly interpreting and parsing inputs, defining output projects
and directory structures, and linking up outputs of jobs to inputs of
subsequent jobs.

See readme for full documentation of how to structure the config file and what
inputs are valid.


TODO
    - slack notifications
    - test mode
    - log jobs started in case of failing to start some and terminate all
    - tag conductor job with link to downstream project jobs and
        bcl2fastq job

"""
import argparse
from collections import defaultdict
import json
import sys

import dxpy as dx
import pandas as pd

from utils.dx_requests import DXExecute, DXManage
from utils.utils import Slack, time_stamp


TESTING = False  # controls if to terminate all jobs and clean up


def parse_sample_sheet(samplesheet) -> list:
    """
    Parses list of sample names from given samplesheet

    Parameters
    ----------
    samplesheet : file
        samplesheet to parse

    Returns
    -------
    list
        list of samplenames
    """
    sheet = pd.read_csv(samplesheet, header=None)
    column = sheet[0].tolist()
    sample_list = column[column.index('Sample_ID') + 1:]

    # sense check some samples found and samplesheet isn't malformed
    assert sample_list, Slack().send(
        f"Sample list could not be parsed from samplesheet: {samplesheet}\n\n"
    )

    return sample_list


def match_samples_to_assays(configs, all_samples) -> dict:
    """
    Match sample list against configs to identify correct config to use
    for each sample

    Parameters
    ----------
    configs : list
        list of config dicts for each assay
    all_samples : list
        list of samples parsed from samplesheet or specified with --samples

    Returns
    -------
    dict
        dict of assay codes: list of matching samples
    """
    # build a dict of assay codes from configs found to samples based off
    # matching assay_code in sample names
    all_config_assay_codes = [x.get('assay_code') for x in configs.values()]
    assay_to_samples = defaultdict(list)

    for code in all_config_assay_codes:
        for sample in all_samples:
            if code in sample:
                assay_to_samples[code].append(sample)

    # check all samples have an assay code in one of the configs
    samples_w_codes = [x for y in list(assay_to_samples.values()) for x in y]
    assert sorted(all_samples) == sorted(samples_w_codes), Slack().send(
        "could not identify assay code for all samples - "
        f"{set(all_samples) - set(samples_w_codes)}"
    )

    # check all samples are for the same assay, don't handle mixed runs for now
    assert len(assay_to_samples.keys() == 1), Slack().send(
        f"more than one assay found in given sample list: {assay_to_samples}"
    )

    print(f"Total samples per assay identified: {assay_to_samples}")

    return assay_to_samples


def load_config(config_file) -> dict:
    """
    Read in given config json to dict

    Parameters
    ----------
    config_file : str
        json config file

    Raises
    ------
    RuntimeError: raised when a non-json file passed as config

    Returns
    -------
    config : dict
        dictionary of loaded json file
    """
    if not config_file.endswith('.json'):
        # sense check a json passed
        raise RuntimeError('Error: invalid config passed - not a json file')

    with open(config_file) as file:
        config = json.load(file)

    return config


def load_test_data() -> list:
    """
    Read in file ids of fastqs and sample names from test_samples file to test
    calling workflows

    Returns
    -------
    fastq_details : list of tuples
        list with tuple per fastq containing (DNAnexus file id, filename)

    """
    with open(args.test_samples) as f:
        fastq_details = f.read().splitlines()

    fastq_details = [(x.split()[0], x.split()[1]) for x in fastq_details]

    return fastq_details


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments

    Returns
    -------
    args : Namespace
        Namespace of passed command line argument inputs
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--assay_config',
        help='assay specific config file that defines all executables to run'
    )
    parser.add_argument(
        '--sentinel_file',
        help='sentinel file uploaded by dx-streaming-upload'
    )
    parser.add_argument(
        '--samplesheet',
        help='samplesheet to parse sample IDs from'
    )
    parser.add_argument(
        '--samples', nargs='+',
        help='list of sample names to run analysis on'
    )
    parser.add_argument(
        '--dx_project_id', required=False,
        help=(
            'DNAnexus project ID to use to run analysis in, '
            'if not specified will create one based off run ID and assay name'
        )
    )
    parser.add_argument(
        '--run_id',
        help='ID of run, used to name output project.'
    )
    parser.add_argument(
        '--assay_name',
        help='assay name, used for naming outputs'
    )
    parser.add_argument(
        '--development', '-d', action='store_true',
        help='Created project will be prefixed with 003 instead of 002.'
    )
    parser.add_argument(
        '--testing', action='store_true',
        help=(
            'controls if to terminate and clean up jobs after launching '
            'for testing purposes'
        )
    )
    parser.add_argument(
        '--bcl2fastq_id',
        help='id of job from running bcl2fastq (if run)'
    )
    parser.add_argument(
        '--bcl2fastq_output',
        help=(
            'dx path to store output from demultiplexing, defaults to parent '
            'of sentinel file if not specified'
        )
    )
    parser.add_argument(
        '--fastqs',
        help='comma separated string of fastq file ids for starting analysis'
    )
    parser.add_argument(
        '--test_samples',
        help=(
            'for test use only. Pass in file with 1 sample per line '
            'specifing file-id of fastq and sample name'
        )
    )

    args = parser.parse_args()

    # turn comma separated str to python list
    if args.samples:
        args.samples = [
            x.replace(' ', '') for x in args.samples.split(',') if x
        ]
    if args.fastqs:
        args.fastqs = [x.replace(' ', '') for x in args.fastqs.split(',') if x]

    return args


def main():
    """
    Main function to run apps and workflows
    """
    args = parse_args()

    if args.testing:
        # if testing, log all jobs to one file to terminate and clean up
        TESTING = True
        open('testing_job_id.log', 'w').close()

    if not args.samples:
        # TODO : sample sheet validation?
        args.samples = parse_sample_sheet(args.samplesheet)

    if args.assay_config:
        # using user defined config file
        config = load_config(args.assay_config)
        assay_code = config.get('assay_code')
        sample_list = args.samples.copy()
    else:
        # get all json assay configs from path in conductor config
        configs = DXManage(args).get_json_configs()
        assay_to_samples = match_samples_to_assays(configs, args.samples)

        # select config to use by assay code from all samples
        # TODO : if/when there are mixed runs this should be looped over for
        # all of the below, will currently exit in match_samples_to_assays()
        # where more than one assay is present from the sample names
        assay_code = next(iter(assay_to_samples))
        config = configs[assay_code]
        sample_list = assay_to_samples[assay_code]


    if not args.dx_project_id:
        # output project not specified, create new one from run id
        args.dx_project_id = DXManage(args).get_or_create_dx_project(config)

    # write analysis project to file to pick up at end to send Slack message
    output_project = dx.bindings.dxproject.DXProject(
        dxid=args.dx_project_id).describe().get('name')
    with open('analysis_project.log', 'w') as fh:
        fh.write(f'{output_project} {args.dx_project_id}')

    # set context to project for running jobs
    dx.set_workspace_id()

    dx_execute = DXExecute(args)
    dx_manage = DXManage(args)

    run_time = time_stamp()

    fastq_details = []
    upload_tars = []

    # sense check per_sample defined for all workflows / apps in config before
    # starting as we want this explicitly defined for everything to ensure
    # it is launched correctly
    for executable, params in config['executables'].items():
        assert 'per_sample' in params.keys(), Slack().send(
            f"per_sample key missing from {executable} in config, check config"
            "and re-run"
        )

    if args.bcl2fastq_id:
        # previous bcl2fastq job specified to use fastqs from
        fastq_details = dx_manage.get_bcl2fastq_details(args.bcl2fastq_id)
    elif args.fastqs:
        # fastqs specified to start analysis from, call describe on
        # files to get name and build list of tuples of (file id, name)
        for fastq_id in args.fastqs:
            fastq_name = dx.api.file_describe(
                fastq_id, input_params={'fields': {'name': True}}
            )
            fastq_name = fastq_name['name']
            fastq_details.append((fastq_id, fastq_name))
    elif args.upload_tars:
        # passed a list of upload tar files from dx-streaming-upload to
        # use as start point for analysis
        upload_tars = [{"$dnanexus_link": x} for x in args.upload_tars]
    elif args.test_samples:
        # test files of fastq names : file ids given
        fastq_details = []
        if args.test_samples:
            fastq_details = load_test_data()
    elif config.get('demultiplex'):
        # not using previous demultiplex job, fastqs or test sample list and
        # demultiplex set to true in config => run bcl2fastq app
        job_id = dx_execute.demultiplex()
        dx_manage.get_bcl2fastq_details(job_id)
    else:
        # not demultiplexing or given fastqs, exit as we aren't handling
        # this for now
        print('No fastqs passed or demultiplexing specified. Exiting now')
        sys.exit()


    # dict to add all stage output names and file ids for every sample to,
    # used to pass correct file ids to subsequent worklow/app calls
    job_outputs_dict = {}

    # storing output folders used for each workflow/app, might be needed to
    # store data together / access specific dirs of data
    executable_out_dirs = {}

    for executable, params in config['executables'].items():
        # for each workflow/app, check if its per sample or all samples and
        # run correspondingly
        print(f'\nConfiguring {executable} to start jobs')

        # log file of all jobs run for current executable, used in case
        # of failing to launch all jobs to be able to terminate all analyses
        open('job_id.log', 'w').close()

        # create output folder for workflow, unique by datetime stamp
        out_folder = f'/output/{params["name"]}-{run_time}'
        out_folder = dx_manage.create_dx_folder(out_folder)
        executable_out_dirs[params['analysis']] = out_folder

        params['executable_name'] = dx.api.app_describe(executable).get('name')

        if params['per_sample'] is True:
            # run workflow / app on every sample
            print(f'\nCalling {params["name"]} per sample')

            # loop over samples and call app / workflow
            for idx, sample in enumerate(sample_list):
                print(
                    f'\nStarting analysis for {sample} - '
                    f'({idx}/{len(sample_list)})'
                )
                job_outputs_dict = dx_execute.call_per_sample(
                    executable,
                    params,
                    sample,
                    config,
                    out_folder,
                    job_outputs_dict,
                    executable_out_dirs,
                    fastq_details,
                    upload_tars
                )

        elif params['per_sample'] is False:
            # run workflow / app on all samples at once
            job_outputs_dict = dx_execute.call_per_run(
                executable,
                params,
                config,
                out_folder,
                job_outputs_dict,
                executable_out_dirs,
                fastq_details,
                upload_tars
            )
        else:
            # per_sample is not True or False, exit
            raise ValueError(
                f"per_sample declaration for {executable} is not True or "
                f"False ({params['per_sample']}). \n\nPlease check the config."
            )

    print("Completed calling jobs")


if __name__ == "__main__":
    main()
