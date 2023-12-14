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
import os
from packaging.version import parse as parseVersion
import re
import subprocess

import dxpy as dx
import pandas as pd

from utils.dx_requests import PPRINT, DXExecute, DXManage
from utils.manage_dict import ManageDict
from utils.utils import Jira, Slack, log, select_instance_types, time_stamp


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

    Raises
    ------
    AssertionError
        Raised when no samples parsed from samplesheet
    """
    sheet = pd.read_csv(samplesheet, header=None, usecols=[0])
    column = sheet[0].tolist()
    sample_list = column[column.index('Sample_ID') + 1:]

    # sense check some samples found and samplesheet isn't malformed
    assert sample_list, Slack().send(
        f"Sample list could not be parsed from samplesheet: {samplesheet}"
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

    log.info(f'\nParsed run ID {run_id} from RunInfo.xml')

    return run_id


def match_samples_to_assays(configs, all_samples, testing, mismatch) -> dict:
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
    mismatch : int
        number of samples allowed to not match to a given assay code. If the
        total no. of samples not matching an assay code is <= the given
        allowed no. of mismatches, these will use the same assay config
        as all other samples on the run

    Returns
    -------
    dict
        dict of assay_code : list of matching samples, i.e.
            {LAB123 : ['sample1-LAB123', 'sample2-LAB123' ...]}

    Raises
    ------
    AssertionError
        Raised when not all samples have an assay config matched
    AssertionError
        Raised when more than one assay config found to use for given samples
    """
    # build a dict of assay codes from configs found to samples based off
    # matching assay_code in sample names
    log.info("\nMatching samples to assay configs")
    all_config_assay_codes = sorted([
        x.get('assay_code') for x in configs.values()])
    assay_to_samples = defaultdict(list)

    log.info(f'\nAll assay codes of config files: {all_config_assay_codes}')
    log.info(f'\nAll samples parsed from samplesheet: {all_samples}')

    # for each sample check each assay code if it matches, then select the
    # matching config with highest version
    for sample in all_samples:
        sample_to_assay_configs = {}
        for code in all_config_assay_codes:
            # find all config files that match this sample
            if re.search(code, sample, re.IGNORECASE):
                sample_to_assay_configs[code] = configs[code]['version']

        if sample_to_assay_configs:
            # found at least one config to match to sample, select
            # one with the highest version
            highest_ver_config = max(
                sample_to_assay_configs.values(), key=parseVersion)

            # select the config key with for the corresponding value found
            # to be the highest
            latest_config_key = list(sample_to_assay_configs)[
                list(sample_to_assay_configs.values()).index(highest_ver_config)]

            assay_to_samples[latest_config_key].append(sample)
        else:
            # no match found, just log this as an AssertionError will be raised
            # below for all samples that don't have a match
            log.error(f"No matching config file found for {sample} !\n")

    if not testing:
        # check all samples are for the same assay, don't handle mixed runs for now
        assert len(assay_to_samples.keys()) == 1, Slack().send(
            f"more than one assay found in given sample list: {assay_to_samples}"
        )

        # check all samples have an assay code in one of the configs
        samples_w_codes = [x for y in list(assay_to_samples.values()) for x in y]

        if mismatch:
            if (
                sorted(all_samples) != sorted(samples_w_codes)
            ) and (
                (len(all_samples) - len(samples_w_codes)) <= int(mismatch)
            ):
                # not all samples matched a code and the total not matching
                # is less than we allow => force the mismatch to use code
                sample_not_match = set(all_samples) - set(samples_w_codes)
                assay_code = next(iter(assay_to_samples))

                log.info(
                    f"Not all samples matched assay codes!\nSamples not "
                    f"matching: {sample_not_match}\nTotal samples not "
                    f"matching is less than mismatch limit allowed of "
                    f"{mismatch}, therefore analysis will continue using "
                    f"assay code of other samples ({assay_code})."
                )

                # add in sample(s) to the assay code match
                assay_to_samples[assay_code].extend(sample_not_match)
                samples_w_codes.extend(sample_not_match)

        samples_without_codes = '\n\t\t'.join([
            f'`{x}`' for x in sorted(set(all_samples) - set(samples_w_codes))
        ])

        assert sorted(all_samples) == sorted(samples_w_codes), Slack().send(
            f"Could not identify assay code for all samples!\n\n"
            f"Configs for assay codes found: "
            f"`{', '.join(all_config_assay_codes)}`\n\nSamples not matching "
            f"any available config:\n\t\t{samples_without_codes}"
        )
    else:
        # running in testing mode, check we found at least one sample to config
        # to actually run. We expect that not all samples may match since if
        # TESTING_SAMPLE_LIMIT is specified then only a subset of samples
        # will be in this dict
        assert assay_to_samples, Slack().send(
            "No samples matched to available config files for testing"
        )

    log.info(f"\nTotal samples per assay identified: {assay_to_samples}")

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
        '--samples',
        help='command seperated string of sample names to run analysis on'
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
        '--demultiplex_job_id',
        help='id of job from running demultiplexing (if run)'
    )
    parser.add_argument(
        '--demultiplex_output',
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
    parser.add_argument(
        '--mismatch_allowance', type=int,
        help=(
            "no. of samples allowed to not match to any assay code and use "
            "the assay code of other samples (default: 1, set in dxapp.json)"
        )
    )

    args = parser.parse_args()

    # turn comma separated str to python list
    if args.samples:
        args.samples = [
            x.replace(' ', '') for x in args.samples.split(',') if x
        ]
        log.info(
            f"\nsamples specified to run jobs for: \n\t{args.samples}\n"
        )
    if args.fastqs:
        args.fastqs = [x.replace(' ', '') for x in args.fastqs.split(',') if x]

    if args.run_info_xml:
        args.run_id = parse_run_info_xml(args.run_info_xml)

    if not args.samples:
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
        config_data = DXManage(args).get_json_configs()
        config_data = DXManage.filter_highest_config_version(config_data)

        assay_to_samples = match_samples_to_assays(
            configs=config_data,
            all_samples=args.samples,
            testing=args.testing,
            mismatch=args.mismatch_allowance
        )

        # select config to use by assay code from all samples
        # TODO : if/when there are mixed runs this should be looped over for
        # all of the below, will currently exit in match_samples_to_assays()
        # where more than one assay is present from the sample names
        assay_code = next(iter(assay_to_samples))
        config = config_data[assay_code]
        sample_list = assay_to_samples[assay_code]

        # add the file ID of assay config file used as job output, this
        # is to make it easier to audit what configs were used for analysis
        subprocess.run(
            "dx-jobutil-add-output assay_config_file_id "
            f"{config_data[assay_code]['file_id']} --class=string",
            shell=True, check=False
        )

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
        fh.write(
            f'{args.dx_project_id} {args.assay_name} {config.get("version")}\n'
        )

    args.dx_project_name = output_project

    print(
        f"\nUsing project {output_project} ({args.dx_project_id}) "
        "for launching analysis jobs in\n"
    )

    # set context to project for running jobs
    dx.set_workspace_id(args.dx_project_id)

    run_time = time_stamp()

    # set parent output directory, each app will have sub dir in here
    # use argparse Namespace for laziness to pass to dx_manage functions
    parent_out_dir = f"/output/{args.assay_name}-{run_time}"
    args.parent_out_dir = parent_out_dir

    # get upload tars from sentinel file, abuse argparse Namespace object
    # again to make it simpler to pass through to DXExecute / DXManage
    args.upload_tars = DXManage(args).get_upload_tars()

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

    # add comment to Jira ticket for run to link to this eggd_conductor job
    Jira().add_comment(
        run_id=args.run_id,
        comment="This run was processed automatically by eggd_conductor: ",
        url=f"http://{os.environ.get('conductor_job_url')}"
    )

    fastq_details = []

    if args.demultiplex_job_id:
        # previous demultiplexing job specified to use fastqs from
        fastq_details = dx_manage.get_demultiplex_job_details(args.demultiplex_job_id)
    elif args.fastqs:
        # fastqs specified to start analysis from, call describe on
        # files to get name and build list of tuples of (file id, name)
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
        # demultiplex set to true in config => run demultiplexing app

        # config and app ID for demultiplex is optional in assay config
        demultiplex_config = config.get('demultiplex_config', {})
        demultiplex_app_id = demultiplex_config.get('app_id', '')
        demultiplex_app_name = demultiplex_config.get('app_name', '')

        if not demultiplex_app_id and not demultiplex_app_name:
            # ID for demultiplex app not in assay config, use default from
            # app config
            demultiplex_app_id = os.environ.get('DEMULTIPLEX_APP_ID')

        job_id = dx_execute.demultiplex(
            app_id=demultiplex_app_id,
            app_name=demultiplex_app_name,
            config=demultiplex_config
        )
        fastq_details = dx_manage.get_demultiplex_job_details(job_id)
    elif ManageDict().search(
            identifier='INPUT-UPLOAD_TARS',
            input_dict=config,
            check_key=False,
            return_key=False
        ):
        # an app / workflow takes upload tars as an input => valid start point
        pass
    else:
        # not demultiplexing or given fastqs, exit as we aren't handling
        # this for now
        raise RuntimeError(
            Slack().send(
                'No fastqs passed or demultiplexing specified. Exiting now'
        ))

    # build a dict mapping executable names to human readable names
    exe_names = dx_manage.get_executable_names(config['executables'].keys())
    log.info('\nExecutable names identified:')
    log.info(PPRINT(exe_names))

    # build mapping of executables input fields => required types (i.e.
    # file, array:file, boolean), used to correctly build input dict
    input_classes = dx_manage.get_input_classes(config['executables'].keys())
    log.info('\nExecutable input classes found:')
    log.info(PPRINT(input_classes))

    # dict to add all stage output names and job ids for every sample to,
    # used to pass correct job ids to subsequent workflow / app calls
    job_outputs_dict = {}

    # storing output folders used for each workflow/app, might be needed to
    # store data together / access specific dirs of data
    executable_out_dirs = {}

    total_jobs = 0  # counter to write to final Slack message

    # log file of all jobs, used to set as app output for picking up
    # by separate monitoring script
    open('all_job_ids.log', 'w').close()

    for executable, params in config['executables'].items():
        # for each workflow/app, check if its per sample or all samples and
        # run correspondingly
        log.info(
            f'\n\nConfiguring {params.get("name")} ({executable}) to start jobs'
        )
        log.info("\nParams parsed from config before modifying:")
        log.info(PPRINT(params))

        # log file of all jobs run for current executable, used in case
        # of failing to launch all jobs to be able to terminate all analyses
        open('job_id.log', 'w').close()

        # save name to params to access later to name job
        params['executable_name'] = exe_names[executable]['name']

        # get instance types to use for executable from config for flowcell
        instance_types = select_instance_types(
            run_id=args.run_id,
            instance_types=params.get('instance_types'))

        if params['per_sample'] is True:
            # run workflow / app on every sample
            log.info(f'\nCalling {params["executable_name"]} per sample')

            # loop over samples and call app / workflow
            for idx, sample in enumerate(sample_list):
                log.info(
                    f'\n\nStarting analysis for {sample} - '
                    f'[{idx+1}/{len(sample_list)}]'
                )
                job_outputs_dict = dx_execute.call_per_sample(
                    executable,
                    exe_names=exe_names,
                    input_classes=input_classes,
                    params=params,
                    sample=sample,
                    config=config,
                    out_folder=parent_out_dir,
                    job_outputs_dict=job_outputs_dict,
                    executable_out_dirs=executable_out_dirs,
                    fastq_details=fastq_details,
                    instance_types=instance_types
                )
                total_jobs += 1

            if params.get('hold'):
                # specified to hold => wait for all sample jobs to complete
                # job_outputs_dict for per sample jobs structured as:
                # {'sample1': {'analysis_1': 'job-xxx'}...}
                job_ids = [x.get(params['analysis']) for x in job_outputs_dict.values()]
                log.info(
                    f'Holding conductor until {len(job_ids)} '
                    f'{params["executable_name"]} job(s) complete...'
                )
                for job in job_ids:
                    if job.startswith('job-'):
                        dx.DXJob(dxid=job).wait_on_done()
                    else:
                        dx.DXAnalysis(dxid=job).wait_on_done()

        elif params['per_sample'] is False:
            # run workflow / app on all samples at once
            job_outputs_dict = dx_execute.call_per_run(
                executable=executable,
                exe_names=exe_names,
                input_classes=input_classes,
                params=params,
                config=config,
                out_folder=parent_out_dir,
                job_outputs_dict=job_outputs_dict,
                executable_out_dirs=executable_out_dirs,
                fastq_details=fastq_details,
                instance_types=instance_types
            )
            total_jobs += 1

            if params.get('hold'):
                print(f"Holding conductor until {params['name']} completes...")
                executable_id = job_outputs_dict[params['analysis']]

                if executable_id.startswith('job-'):
                    dx.DXJob(dxid=executable_id).wait_on_done()
                else:
                    dx.DXAnalysis(dxid=executable_id).wait_on_done()

        else:
            # per_sample is not True or False, exit
            raise ValueError(
                f"per_sample declaration for {executable} is not True or "
                f"False ({params['per_sample']}). \n\nPlease check the config."
            )

        log.info(
            f'\n\nAll jobs for {params.get("name")} ({executable}) '
            f'launched successfully!\n\n'
        )

    with open('total_jobs.log', 'w') as fh:
        fh.write(str(total_jobs))

    log.info("\nCompleted calling jobs")

    # add comment to Jira ticket for run to link to analysis project
    Jira().add_comment(
        run_id=args.run_id,
        comment=(
            "All jobs sucessfully launched by eggd_conductor. "
            "\nAnalysis project: "
        ),
        url=(
            "http://platform.dnanexus.com/projects/"
            f"{args.dx_project_id.replace('project-', '')}/monitor/"
        )
    )


if __name__ == "__main__":
    main()
