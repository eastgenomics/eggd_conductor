"""
Functions related to querying and managing objects in DNAnexus, as well
as running jobs.
"""
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
import json
import os
from pprint import PrettyPrinter
import random
import re
from typing import Tuple

import dxpy as dx
from packaging.version import Version, parse

from utils.manage_dict import ManageDict
from utils.utils import (
    Slack,
    prettier_print,
    select_instance_types,
    time_stamp
)


def get_json_configs() -> dict:
    """
    Query path in DNAnexus for json config files for each assay, returning
    full data for all unarchived config files found

    ASSAY_CONFIG_PATH comes from the app config file sourced to the env.

    Returns
    -------
    list
        list of dicts of the json object for each config file found

    Raises
    ------
    AssertionError
        Raised when invalid project:path structure defined in app config
    AssertionError
        Raised when no config files found at the given path
    """
    config_path = os.environ.get('ASSAY_CONFIG_PATH', '')

    # check for valid project:path structure
    assert re.match(r'project-[\d\w]*:/.*', config_path), Slack().send(
        f'ASSAY_CONFIG_PATH from config appears invalid: {config_path}'
    )

    prettier_print(
        f"\nSearching following path for assay configs: {config_path}"
    )

    project, path = config_path.split(':')

    files = list(dx.find_data_objects(
        name="*.json",
        name_mode='glob',
        project=project,
        folder=path,
        describe=True
    ))

    # sense check we find config files
    assert files, Slack().send(
        f"No config files found in given path: {project}:{path}")

    files_ids = '\n\t'.join([
        f"{x['describe']['name']} ({x['id']} - "
        f"{x['describe']['archivalState']})" for x in files])
    prettier_print(f"\nAssay config files found:\n\t{files_ids}")

    all_configs = []
    for file in files:
        if file['describe']['archivalState'] == 'live':
            config_data = json.loads(
                dx.bindings.dxfile.DXFile(
                    project=file['project'], dxid=file['id']).read())

            # add file ID as field into the config file
            config_data['file_id'] = file['id']
            all_configs.append(config_data)
        else:
            prettier_print(
                "Config file not in live state - will not be used:"
                f"{file['describe']['name']} ({file['id']}"
            )

    return all_configs


def filter_highest_config_version(all_configs) -> dict:
    """
    Filters all configs found from get_json_configs() to retain highest
    version for each assay code to use for analysis.

    Assay codes are expected to be either a single code in the 'assay_code'
    field in the config file, or a '|' seperated string of multiple.

    This keeps the highest version of each assay code, factoring in where
    one assay code may be a subset of another with a higher version, and
    only keeping the latter, i.e.
    {'EGG2': 1.0.0, 'EGG2|LAB123': 1.1.0} -> {'EGG2|LAB123': 1.1.0}


    Parameters
    ----------
    all_configs : list
        list of dicts of the json object for each config file
        found, returned from get_json_configs()

    Returns
    -------
    dict
        mapping of assay_code to full config data for the highest
        version config file for each assay_code

    Raises
    ------
    AssertionError
        Raised when config file has missing assay_code or version field
    """
    # filter all config files to just get full config data for the
    # highest version of each full assay code
    prettier_print(
        "\nFiltering config files from DNAnexus for highest versions"
    )
    highest_version_config_data = {}

    for config in all_configs:
        current_config_code = config.get('assay_code')
        current_config_ver = config.get('version')

        # sense check config file has code and version fields
        assert current_config_code and current_config_ver, Slack().send(
            f"Config file missing assay_code and/or version field!"
            f"File ID: {config['file_id']}"
        )

        # get highest stored version of config file for current code
        # we have found so far
        highest_version = highest_version_config_data.get(
            current_config_code, {}).get('version', '0')

        if Version(current_config_ver) > Version(highest_version):
            # higher version than stored one for same code => replace
            highest_version_config_data[current_config_code] = config

    # build simple dict of assay_code : version
    all_assay_codes = {
        x['assay_code']: x['version']
        for x in highest_version_config_data.values()
    }

    # get unique list of single codes from all assay codes, split on '|'
    # i.e. ['EGG1', 'EGG2', 'EGG2|LAB123'] -> ['EGG1', 'EGG2', 'LAB123']
    uniq_codes = [
        x.split('|') for x in all_assay_codes.keys()]
    uniq_codes = list(set([
        code for split_codes in uniq_codes for code in split_codes]))

    prettier_print(
        "\nUnique assay codes parsed from all config "
        f"assay_code fields {uniq_codes}\n"
    )

    # final dict of config files to use as assay_code : config data
    configs_to_use = {}

    # for each single assay code, find the highest version config file
    # that code is present in (i.e. {'EGG2': 1.0.0, 'EGG2|LAB123': 1.1.0}
    # would result in EGG2 -> {'EGG2|LAB123': 1.1.0})
    for uniq_code in uniq_codes:
        matches = {}
        for full_code in all_assay_codes.keys():
            if uniq_code in full_code.split('|'):
                # this single assay code is in the full assay code
                # parsed from config, add match as 'assay_code': 'version'
                matches[full_code] = all_assay_codes[full_code]

        # check we don't have 2 matches with the same version as we
        # can't tell which to use, i.e. EGG2 : 1.0.0 & EGG2|LAB123 : 1.0.0
        assert sorted(list(matches.values())) == \
            sorted(list(set(matches.values()))), Slack().send(
            f"More than one version of config file found for a single "
            f"assay code!\n\t{matches}"
        )

        # for this unique code, select the full assay code with the highest
        # version this one was found in using packaging.version.parse, and
        # then select the full config file data for it
        full_code_to_use = max(matches, key=parse)
        configs_to_use[
            full_code_to_use] = highest_version_config_data[full_code_to_use]

    # add to log record of highest version of each config found
    usable_configs = '\n\t'.join(
        [f"{k} ({v['version']}): {v['file_id']}"
        for k, v in configs_to_use.items()]
    )

    prettier_print(
        "\nHighest versions of assay configs found to use:"
        f"\n\t{usable_configs}\n"
    )

    return configs_to_use


def find_dx_project(project_name) -> str:
    """
    Check if project already exists in DNAnexus with given name,
    returns project ID if present and None if not found.

    Parameters
    ----------
    project_name : str
        name of project to search DNAnexus for

    Returns
    -------
    dx_project : str
        dx ID of given project if found, else returns `None`

    Raises
    ------
    AssertionError
        Raised when more than one project found for given name
    """
    dx_projects = list(dx.bindings.search.find_projects(name=project_name))

    prettier_print('Found the following DNAnexus projects:')
    prettier_print(dx_projects)

    if not dx_projects:
        # found no project, return None and create one in
        # get_or_create_dx_project()
        return None

    assert len(dx_projects) == 1, Slack().send(
        "Found more than one project matching given "
        f"project name: {project_name}"
    )

    return dx_projects[0]['id']


def get_demultiplex_job_details(job_id) -> list:
    """
    Given job ID for demultiplexing, return a list of the fastq file IDs

    Parameters
    ----------
    job_id : str
        job ID of demultiplexing job

    Returns
    -------
    fastq_ids : list
        list of tuples with fastq file IDs and file name
    """
    prettier_print(f"\nGetting fastqs from given demultiplexing job: {job_id}")
    demultiplex_job = dx.bindings.dxjob.DXJob(dxid=job_id).describe()
    demultiplex_project = demultiplex_job['project']
    demultiplex_folder = demultiplex_job['folder']

    # find all fastqs from demultiplex job, return list of dicts with details
    fastq_details = list(dx.search.find_data_objects(
        name="*.fastq*", name_mode="glob", project=demultiplex_project,
        folder=demultiplex_folder, describe=True
    ))
    # build list of tuples with fastq name and file ids
    fastq_details = [
        (x['id'], x['describe']['name']) for x in fastq_details
    ]
    # filter out Undetermined fastqs
    fastq_details = [
        x for x in fastq_details if not x[1].startswith('Undetermined')
    ]

    prettier_print(f'\nFastqs parsed from demultiplexing job {job_id}')
    prettier_print(fastq_details)

    return fastq_details


def get_executable_names(executables) -> dict:
    """
    Build a dict of all executable IDs parsed from config to human
    readable names, used for naminmg outputs needing workflow/app names

    Parameters
    ----------
    executables : list
        list of executables to get names for (workflow-, app-, applet-)

    Returns
    -------
    dict
        mapping of executable -> human readable name, for workflows this
        will be workflow_id -> workflow_name + each stage_id -> stage_name

        {
            'workflow-1' : {
                'name' : 'my_workflow_v1.0.0',
                'stages' : {
                    'stage1' : 'first_app-v1.0.0'
                    'stage2' : 'second_app-v1.0.0'
                }
            },
            'app-1' : {
                'name': 'myapp-v1.0.0'
            }
        }
    """
    prettier_print(f'\nGetting names for all executables: {executables}')
    mapping = defaultdict(dict)

    # sense check everything is a valid dx executable
    assert all([
        x.startswith('workflow-')
        or x.startswith('app')
        or x.startswith('applet-')
        for x in executables
    ]), Slack().send(
        f'Executable(s) from the config not valid: {executables}'
    )

    for exe in executables:
        if exe.startswith('workflow-'):
            workflow_details = dx.api.workflow_describe(exe)
            workflow_name = workflow_details.get('name')
            workflow_name.replace('/', '-')
            mapping[exe]['name'] = workflow_name
            mapping[exe]['stages'] = defaultdict(dict)

            for stage in workflow_details.get('stages'):
                stage_id = stage.get('id')
                stage_name = stage.get('executable')
                if stage_name.startswith('applet-'):
                    # need an extra describe for applets
                    stage_name = dx.api.workflow_describe(
                        stage_name).get('name')

                if stage_name.startswith('app-'):
                    # apps are prefixed with app- which is ugly
                    stage_name = stage_name.replace('app-', '')

                # app names will be in format app-id/version
                stage_name = stage_name.replace('/', '-')
                mapping[exe]['stages'][stage_id] = stage_name

        elif exe.startswith('app-') or exe.startswith('applet-'):
            app_details = dx.api.workflow_describe(exe)
            app_name = app_details['name'].replace('/', '-')
            if app_name.startswith('app-'):
                app_name = app_name.replace('app-', '')
            mapping[exe] = {'name': app_name}

    return mapping


def get_input_classes(executables) -> dict:
    """
    Get classes of all inputs for each app / workflow stage, used
    when building out input dict to ensure correct type set

    Parameters
    ----------
    executables : list
        list of executables to get names for (workflow-, app-, applet-)

    Returns
    -------
    dict
        mapping of exectuable / stage to outputs with types
        {
            'applet-FvyXygj433GbKPPY0QY8ZKQG': {
                'adapters_txt': {
                    'class': 'file',
                    'optional': False
                },
                'contaminants_txt': {
                    'class': 'file',
                    'optional': False
                }
                'nogroup': {
                    'class': 'boolean',
                    'optional': False
                }
        },
            'workflow-GB12vxQ433GygFZK6pPF75q8': {
                'stage-G9Z2B7Q41bQg2Jy40zVqqGg4.female_threshold': {
                    'class': 'int',
                    'optional': True
                }
                'stage-G9Z2B7Q41bQg2Jy40zVqqGg4.male_threshold': {
                    'class': 'int',
                    'optional': True
                }
                'stage-G9Z2B7Q41bQg2Jy40zVqqGg4.somalier_input': {
                    'class': 'file',
                    'optional': False
                }
                'stage-G9Z2B8841bQY907z1ygq7K9x.file_prefix': {
                    'class': 'string',
                    'optional': True
                }
        },
        ......
    """
    mapping = defaultdict(dict)
    for exe in executables:
        describe = dx.describe(exe)
        for input in describe['inputSpec']:
            mapping[exe][input['name']] = defaultdict(dict)
            mapping[exe][input['name']]['class'] = input['class']
            mapping[exe][input['name']]['optional'] = input.get(
                'optional', False)

    return mapping


def get_job_output_details(job_id) -> Tuple[list, list]:
    """
    Get describe details for all output files from a job

    Parameters
    ----------
    job_id : str
        ID of job to get output files from

    Returns
    -------
    list
        list of describe dicts for each file found
    list
        list of dicts of job output field -> job output file IDs
    """
    print(f"Querying output files for {job_id}")
    # find files in given jobs out directory
    job_details = dx.DXJob(dxid=job_id).describe()
    job_output_ids = job_details.get('output')
    all_output_files = list(dx.find_data_objects(
        project=job_details.get('project'),
        folder=job_details.get('folder'),
        describe=True
    ))

    # ensure these files only came from our given job
    all_output_files = [
        x for x in all_output_files
        if x['describe']['createdBy']['job'] == job_id
    ]

    print(f"Found {len(all_output_files)} from {job_id}")

    return all_output_files, job_output_ids


def wait_on_done(analysis, analysis_name, all_job_ids) -> None:
    """
    Hold eggd_conductor until all job(s) for the given analysis step
    have completed

    Parameters
    ----------
    analysis : str
        analysis step to select job IDs to wait on
    analysis_name : str
        name of analysis step to wait on
    all_job_ids : dict
        mapping of analysis step -> job ID(s)
    """
    # job_outputs_dict for per run jobs structured as
    # {'analysis_1': 'job-xxx'} and per sample as
    # {'sample1': {'analysis_2': 'job-xxx'}...} => try and get both
    job_ids = [all_job_ids.get(analysis)]
    job_ids.extend([
        x.get(analysis) for x in all_job_ids.values()
        if isinstance(x, dict)
    ])

    # ensure we don't have any Nones
    job_ids = [x for x in job_ids if x]

    prettier_print(
        f'Holding conductor until {len(job_ids)} '
        f'{analysis_name} job(s) complete: {", ".join(job_ids)}'
    )

    for job in job_ids:
        if job.startswith('job-'):
            dx.DXJob(dxid=job).wait_on_done()
        else:
            dx.DXAnalysis(dxid=job).wait_on_done()

    print('All jobs to wait on completed')
