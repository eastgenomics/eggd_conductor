"""
Using a JSON config, calls all workflows / apps defined in config for
given samples.

Handles correctly interpreting and parsing inputs, defining output projects
and directory structures, and linking up outputs of jobs to inputs of
subsequent jobs.

See readme for full documentation of how to structure the config file and what
inputs are valid.
"""
import argparse
from collections import defaultdict
from xml.etree import ElementTree as ET
import json
import sys

import dxpy as dx
import pandas as pd

from utils.dx_requests import PPRINT, DXExecute, DXManage
from utils.utils import Slack, time_stamp


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


def parse_run_info_xml(xml_file) -> str:
    """
    Parses RunID from RunInfo.xml file

    Parameters
    ----------
    xml_file : file
        RunInfo.xml file

    Returns
    -------
    str
        Run ID parsed from file
    """
    tree = ET.parse(xml_file)
    root = tree.getroot()
    run_attributes = [x.attrib for x in root.findall('Run')]
    run_id = ''

    if run_attributes:
        # should always be present
        run_id = run_attributes[0].get('Id')

    print(f'Parsed run ID {run_id} from RunInfo.xml')

    return run_id


def match_samples_to_assays(configs, all_samples, testing) -> dict:
    """
    Match sample list against configs to identify correct config to use
    for each sample

    Parameters
    ----------
    configs : dict
        dict of config dicts for each assay
    all_samples : list
        list of samples parsed from samplesheet or specified with --samples
    testing : bool
        if running in test mode, if not will perform checks on samples

    Returns
    -------
    dict
        dict of assay codes : list of matching samples

    Raises
    ------
    AssertionError
        Raised when not all samples have an assay config matched
    AssertionError
        Raised when more than one assay config found to use for given samples
    """
    # build a dict of assay codes from configs found to samples based off
    # matching assay_code in sample names
    all_config_assay_codes = [x.get('assay_code') for x in configs.values()]
    assay_to_samples = defaultdict(list)

    print(f'All assay codes: {all_config_assay_codes}')
    print(f'All samples: {all_samples}')

    for code in all_config_assay_codes:
        for sample in all_samples:
            if code in sample:
                assay_to_samples[code].append(sample)

    if not testing:
        # check all samples have an assay code in one of the configs
        samples_w_codes = [x for y in list(assay_to_samples.values()) for x in y]
        assert sorted(all_samples) == sorted(samples_w_codes), Slack().send(
            f"could not identify assay code for all samples!\n "
            f"Configs for assay codes found: "
            f"{', '.join(all_config_assay_codes)}.\n"
            f"Samples not matching any available config: "
            f"{set(all_samples) - set(samples_w_codes)}"
        )

        # check all samples are for the same assay, don't handle mixed runs for now
        assert len(assay_to_samples.keys()) == 1, Slack().send(
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


def load_test_data(test_samples) -> list:
    """
    Read in file ids of fastqs and sample names from test_samples file to test
    calling workflows

    Parameters
    ----------
    test_samples : str
        filename of test samples to read in

    Returns
    -------
    fastq_details : list of tuples
        list with tuple per fastq containing (DNAnexus file id, filename)

    """
    with open(test_samples) as f:
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
        '--run_info_xml',
        help='RunInfo.xml file, used to parse run ID from'
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
        '--testing_sample_limit',
        help='for use when testing only - no. samples to limit running analyses for'
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

    if args.run_info_xml:
        args.run_id = parse_run_info_xml(args.run_info_xml)

    if not args.samples:
        # TODO : sample sheet validation?
        args.samples = parse_sample_sheet(args.samplesheet)

    return args


def main():
    """
    Main entry point to run all apps and workflows
    """
    args = parse_args()

    if args.testing:
        # if testing, log all jobs to one file to terminate and clean up
        open('testing_job_id.log', 'w').close()

    if args.assay_config:
        # using user defined config file
        config = load_config(args.assay_config)
        assay_code = config.get('assay_code')
        sample_list = args.samples.copy()
    else:
        # get all json assay configs from path in conductor config
        configs = DXManage(args).get_json_configs()
        assay_to_samples = match_samples_to_assays(
            configs=configs,
            all_samples=args.samples,
            testing=args.testing
        )

        # select config to use by assay code from all samples
        # TODO : if/when there are mixed runs this should be looped over for
        # all of the below, will currently exit in match_samples_to_assays()
        # where more than one assay is present from the sample names
        assay_code = next(iter(assay_to_samples))
        config = configs[assay_code]
        sample_list = assay_to_samples[assay_code]

    if args.testing_sample_limit:
        sample_list = sample_list[:int(args.testing_sample_limit)]

    if not args.assay_name:
        args.assay_name = config.get('assay')

    if not args.dx_project_id:
        # output project not specified, create new one from run id
        args.dx_project_id = DXManage(args).get_or_create_dx_project(config)

    # write analysis project to file to pick up at end to send Slack message
    output_project = dx.bindings.dxproject.DXProject(
        dxid=args.dx_project_id).describe().get('name')
    with open('analysis_project.log', 'w') as fh:
        fh.write(f'{output_project} {args.dx_project_id}\n')

    # set context to project for running jobs
    dx.set_workspace_id(args.dx_project_id)

    run_time = time_stamp()

    # set parent output directory, each app will have sub dir in here
    # use argparse Namespace for laziness to pass to dx_manage functions
    parent_out_dir = f"/output/{args.assay_name}-{run_time}"
    args.parent_out_dir = parent_out_dir

    dx_execute = DXExecute(args)
    dx_manage = DXManage(args)

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
        fastq_details = []
        for fastq_id in args.fastqs:
            fastq_name = dx.api.file_describe(
                fastq_id, input_params={'fields': {'name': True}}
            )
            fastq_name = fastq_name['name']
            fastq_details.append((fastq_id, fastq_name))
    elif args.test_samples:
        # test files of fastq names : file ids given
        fastq_details = load_test_data(args.test_samples)
    elif config.get('demultiplex'):
        # not using previous demultiplex job, fastqs or test sample list and
        # demultiplex set to true in config => run bcl2fastq app
        job_id = dx_execute.demultiplex()
        fastq_details = dx_manage.get_bcl2fastq_details(job_id)
    else:
        # not demultiplexing or given fastqs, exit as we aren't handling
        # this for now
        print('No fastqs passed or demultiplexing specified. Exiting now')
        sys.exit()


    # build a dict mapping executable names to human readable names
    exe_names = dx_manage.get_executable_names(config['executables'].keys())
    print('Executable names identified:')
    PPRINT(exe_names)

    # dict to add all stage output names and file ids for every sample to,
    # used to pass correct file ids to subsequent worklow/app calls
    job_outputs_dict = {}

    # storing output folders used for each workflow/app, might be needed to
    # store data together / access specific dirs of data
    executable_out_dirs = {}

    total_jobs = 0  # counter to write to final Slack message

    for executable, params in config['executables'].items():
        # for each workflow/app, check if its per sample or all samples and
        # run correspondingly
        print(
            f'\n\nConfiguring {params.get("name")} ({executable}) to start jobs'
        )
        print(f"Params parsed from config before modifying:")
        PPRINT(params)

        # log file of all jobs run for current executable, used in case
        # of failing to launch all jobs to be able to terminate all analyses
        open('job_id.log', 'w').close()

        # save name to params to access later to name job
        params['executable_name'] = exe_names[executable]['name']

        if params['per_sample'] is True:
            # run workflow / app on every sample
            print(f'\nCalling {params["executable_name"]} per sample')

            # loop over samples and call app / workflow
            for idx, sample in enumerate(sample_list):
                print(
                    f'\n\nStarting analysis for {sample} - '
                    f'({idx}/{len(sample_list)})'
                )
                job_outputs_dict = dx_execute.call_per_sample(
                    executable,
                    exe_names=exe_names,
                    params=params,
                    sample=sample,
                    config=config,
                    out_folder=parent_out_dir,
                    job_outputs_dict=job_outputs_dict,
                    executable_out_dirs=executable_out_dirs,
                    fastq_details=fastq_details
                )
                total_jobs += 1

        elif params['per_sample'] is False:
            # run workflow / app on all samples at once
            job_outputs_dict = dx_execute.call_per_run(
                executable=executable,
                exe_names=exe_names,
                params=params,
                config=config,
                out_folder=parent_out_dir,
                job_outputs_dict=job_outputs_dict,
                executable_out_dirs=executable_out_dirs,
                fastq_details=fastq_details
            )
            total_jobs += 1
        else:
            # per_sample is not True or False, exit
            raise ValueError(
                f"per_sample declaration for {executable} is not True or "
                f"False ({params['per_sample']}). \n\nPlease check the config."
            )

        print(
            f'\n\nAll jobs for {params.get("name")} ({executable}) '
            f'launched successfully!\n\n'
        )

    with open('total_jobs.log', 'w') as fh:
        fh.write(str(total_jobs))

    print("Completed calling jobs")


if __name__ == "__main__":
    main()
