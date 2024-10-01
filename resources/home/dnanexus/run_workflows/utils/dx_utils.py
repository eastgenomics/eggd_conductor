"""
Functions related to querying and managing objects in DNAnexus, as well
as running jobs.
"""

import concurrent
import json
import os
import re
from typing import Tuple

import dxpy as dx
from packaging.version import Version, parse

from utils.utils import prettier_print
from utils.WebClasses import Slack


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
                matches[full_code] = parse(all_assay_codes[full_code])

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
        full_code_to_use = max(matches, key=matches.get)
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


def terminate_jobs(jobs) -> None:
    """
    Terminate all launched jobs in testing mode

    Parameters
    ----------
    jobs : list
        list of job / analysis IDs
    """
    def terminate_one(job) -> None:
        """dx call to terminate single job"""
        if job.startswith('job'):
            dx.DXJob(dxid=job).terminate()
        else:
            dx.DXAnalysis(dxid=job).terminate()

    prettier_print(f"Trying to terminate: {jobs}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        concurrent_jobs = {
            executor.submit(terminate_one, job_id):
            job_id for job_id in sorted(jobs, reverse=True)
        }

        for future in concurrent.futures.as_completed(concurrent_jobs):
            # access returned output as each is returned in any order
            try:
                future.result()
            except Exception as exc:
                # catch any errors that might get raised
                prettier_print(
                    "Error terminating job "
                    f"{concurrent_jobs[future]}: {exc}"
                )

    prettier_print("Terminated jobs.")


def dx_run(
    executable, job_name, input_dict, output_dict, prev_jobs,
    extra_args, instance_types, project_id
) -> str:
    """
    Call workflow / app with populated input and output dicts

    Returns id of submitted job

    Parameters
    ----------
    executable : str
        human readable name of executable (i.e. workflow / app / applet)
    job_name : str
        name to assign to job, will be combination of human readable name
        of exectuable and sample ID
    input_dict : dict
        dict of input parameters for calling workflow / app
    output_dict : dict
        dict of output directory paths for each app
    prev_jobs : list
        list of job ids to wait for completion before starting
    extra_args : dict
        mapping of any additional arguments to pass to underlying dx
        API call, parsed from extra_args field in config file
    instance_types : dict
        mapping of instances to use for apps
    project_id : str
        DNAnexus project id in which the job will be launched
    testing : bool
        Boolean indicating if the execution of conductor is in testing mode

    Returns
    -------
    job_id : str
        DNAnexus job id of newly started analysis

    Raises
    ------
    RuntimeError
        Raised when workflow-, app- or applet- not present in exe name
    """

    prettier_print(f"\nPopulated input dict for: {executable}")
    prettier_print(input_dict)

    if os.environ.get('TESTING') == 'true':
        # running in test mode => don't actually want to run jobs =>
        # make jobs dependent on conductor job finishing so no launched
        # jobs actually start running
        prev_jobs.append(os.environ.get('PARENT_JOB_ID'))

    if 'workflow-' in executable:
        # get common top level of each apps output destination
        # to set as output of workflow for consitency of viewing
        # in the browser
        parent_path = os.path.commonprefix(list(output_dict.values()))

        job_handle = dx.bindings.dxworkflow.DXWorkflow(
            dxid=executable,
            project=project_id
        ).run(
            workflow_input=input_dict,
            folder=parent_path,
            stage_folders=output_dict,
            rerun_stages=['*'],
            depends_on=prev_jobs,
            name=job_name,
            extra_args=extra_args,
            stage_instance_types=instance_types
        )

    elif 'app-' in executable:
        job_handle = dx.bindings.dxapp.DXApp(dxid=executable).run(
            app_input=input_dict,
            project=project_id,
            folder=output_dict.get(executable),
            ignore_reuse=True,
            depends_on=prev_jobs,
            name=job_name,
            extra_args=extra_args,
            instance_type=instance_types
        )

    elif 'applet-' in executable:
        job_handle = dx.bindings.dxapplet.DXApplet(dxid=executable).run(
            applet_input=input_dict,
            project=project_id,
            folder=output_dict.get(executable),
            ignore_reuse=True,
            depends_on=prev_jobs,
            name=job_name,
            extra_args=extra_args,
            instance_type=instance_types
        )

    else:
        # doesn't appear to be valid workflow or app
        raise RuntimeError(
            f'Given executable id is not valid: {executable}'
        )

    job_details = job_handle.describe()
    job_id = job_details.get('id')

    prettier_print(
        f'Started analysis in project {project_id}, '
        f'job: {job_id}'
    )

    with open('job_id.log', 'a') as fh:
        # log of current executable jobs
        fh.write(f'{job_id} ')

    with open('all_job_ids.log', 'a') as fh:
        # log of all launched job IDs
        fh.write(f'{job_id},')

    return job_id
