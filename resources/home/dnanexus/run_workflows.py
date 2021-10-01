"""
Using a config, calls all workflows / apps defined in config for given samples.
Handles correctly interpreting and parsing inputs, defining output projects
and directory structures, and linking up outputs of jobs to inputs of
subsequent jobs. See readme for full documentation of how to structure the
config file.

Jethro Rainford 210902
"""
import argparse
from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
import pprint
import re
import sys
from typing import Generator

import dxpy


PPRINT = pprint.PrettyPrinter(indent=4).pprint


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


def find_dx_project(project_name) -> str:
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

    dx_project = dx_projects[0]['id']

    return dx_project


def create_dx_project(args, config) -> argparse.ArgumentParser:
    """
    Create new project in DNAnexus
    """
    output_project = f'002_{args.run_id}_{args.assay_code}'

    project_id = find_dx_project(output_project)

    if not project_id:
        # create new project and capture returned project id and store
        project_id = dxpy.bindings.dxproject.DXProject().new(
            name=output_project,
            summary=f'Analysis of run {args.run_id} with {args.assay_code}'
        )
        print(
            f'Created new project for output: {output_project} ({project_id})'
        )
    else:
        print(f'Using existing found project: {output_project} ({project_id})')

    users = config.get('users')

    if users:
        # users specified in config to grant access to project
        for user, access_level in users.items():
            dxpy.bindings.dxproject.DXProject(dxid=project_id).invite(
                user, access_level, send_email=False
            )

    args.dx_project_id = project_id

    return args


def create_dx_folder(args, out_folder) -> str:
    """
    Create output folder in DNAnexus project
    """
    for i in range(1, 100):
        # sanity check, should only be 1 or 2 already existing at most
        dx_folder = f'{out_folder}-{i}'

        try:
            dxpy.api.project_list_folder(
                args.dx_project_id,
                input_params={"folder": dx_folder, "only": "folders"},
            )
        except dxpy.exceptions.ResourceNotFound:
            dxpy.api.project_new_folder(
                args.dx_project_id, input_params={'folder': dx_folder}
            )
            print(f'Created output folder: {dx_folder}')
            break
        else:
            # folder already exists => continue
            print(f'{dx_folder} already exists, incrementing suffix integer')
            continue

    return dx_folder


def call_dx_run(args, executable, input_dict, output_dict, prev_jobs) -> str:
    """
    Call workflow / app, returns id of submitted job
    """
    if 'workflow-' in executable:
        job_handle = dxpy.bindings.dxworkflow.DXWorkflow(
            dxid=executable, project=args.dx_project_id
        ).run(
            workflow_input=input_dict, stage_folders=output_dict,
            rerun_stages=['*'], depends_on=prev_jobs
        )
    elif 'app-' in executable:
        job_handle = dxpy.bindings.dxapp.DXApp(dxid=executable).run(
            app_input=input_dict, project=args.dx_project_id,
            folder=output_dict[executable], ignore_reuse=True,
            depends_on=prev_jobs
        )
    elif 'applet-' in executable:
        job_handle = dxpy.bindings.dxapplet.DXApplet(dxid=executable).run(
            applet_input=input_dict, project=args.dx_project_id,
            folder=output_dict[executable], ignore_reuse=True,
            depends_on=prev_jobs
        )
    else:
        # doesn't appear to be valid workflow or app
        raise Exception

    job_details = job_handle.describe()

    job_id = job_details.get('id')

    print(f'Started analysis in project {args.dx_project_id}, job: {job_id}')

    return job_id


def add_fastqs(input_dict, fastq_details, sample=None) -> dict:
    """
    If process_fastqs set to true, function is called to populate input dict
    with appropriate fastq file ids
    """
    sample_fastqs = []
    if sample:
        for fastq in fastq_details:
            # sample specified => running per sample, if not using all fastqs
            # find fastqs for given sample
            sample_regex = rf'{sample}_L00[0-9]_R[1,2]_001.fastq(.gz)?'
            match = re.search(sample_regex, fastq[1])

            if match:
                sample_fastqs.append(fastq)

        # ensure some fastqs found
        assert sample_fastqs, f'No fastqs found for {sample}'
    else:
        # use all fastqs
        sample_fastqs = fastq_details

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

    return input_dict


def add_other_inputs(
        input_dict, dx_project_id, executable_out_dirs, sample=None) -> dict:
    """
    Generalised function for adding other INPUT-s, currently handles parsing:
    workflow output directories, project id and project name.

    Extensible to add more in future, probably could be cleaner than a load of
    if statements but oh well
    """
    # first checking if any INPUT- in dict to fill, if not return
    other_inputs = list(find_job_inputs('INPUT-', input_dict, check_key=False))

    if not other_inputs:
        # no other inputs found to replace
        return input_dict

    print('found other inputs to fill')

    for job_input in other_inputs:
        if job_input == 'INPUT-SAMPLE-NAME':
            # add sample name
            replace_job_inputs(input_dict, job_input, sample)

        if job_input == 'INPUT-dx_project_id':
            # add project id
            replace_job_inputs(input_dict, job_input, dx_project_id)

        if job_input == 'INPUT-dx_project_name':
            # call describe on job id and add project name
            output = dxpy.api.project_describe(
                dx_project_id, input_params={'fields': {'name': True}})
            project_name = output.get('name')

            replace_job_inputs(input_dict, job_input, project_name)

        # match analysis_X (i.e. analysis_1, analysis_2...)
        out_folder_match = re.search(
            r'^INPUT-analysis_[0-9]{1,2}-out_dir$', job_input)

        if out_folder_match:
            # passing an out folder for given analysis
            # will be specified in format INPUT-analysis_1-out_dir, where
            # job input should be replaced with respective out dir
            analysis = job_input.strip('INPUT-').strip('-out_dir')
            analysis_out_dir = executable_out_dirs.get(analysis)

            if analysis_out_dir:
                # removing /output/ for now to fit to MultiQC
                analysis_out_dir = Path(analysis_out_dir).name
                replace_job_inputs(input_dict, job_input, analysis_out_dir)
            else:
                raise KeyError((
                    'Error trying to parse output directory to input dict.\n'
                    f'No output directory found for given input: {job_input}\n'
                    'Please check config to ensure job input is in the '
                    'format: INPUT-analysis_[0-9]-out_dir'
                ))

    return input_dict


def find_job_inputs(identifier, input_dict, check_key) -> Generator:
    """
    Recursive function to find all values with identifying prefix, these
    require replacing with appropriate job output file ids. Returns a generator
    with input fields to replace.
    """
    for key, value in input_dict.items():
        # set field to check for identifier to either key or value
        if check_key is True:
            check_field = key
        else:
            check_field = value

        if isinstance(value, dict):
            yield from find_job_inputs(identifier, value, check_key)
        if isinstance(value, list):
            # found list of dicts -> loop over them
            for list_val in value:
                yield from find_job_inputs(identifier, list_val, check_key)
        if isinstance(value, bool):
            # stop it breaking on booleans
            continue
        if identifier in check_field:
            # found input to replace
            yield value


def replace_job_inputs(input_dict, job_input, link_id) -> Generator:
    """
    Recursively traverse through nested dictionary and replace any matching
    job_input with given DNAnexus job/file/project id
    """
    for key, val in input_dict.items():
        if isinstance(val, dict):
            # found a dict, continue
            replace_job_inputs(val, job_input, link_id)
        if isinstance(val, list):
            # found list of dicts, check each dict
            for list_val in val:
                replace_job_inputs(list_val, job_input, link_id)
        if val == job_input:
            # replace analysis_ with correct job id
            input_dict[key] = link_id


def get_dependent_jobs(params, job_outputs_dict, sample=None):
    """
    If app / workflow depends on previous job(s) completing these will be
    passed with depends_on = [analysis_1, analysis_2...]. Get all job ids for
    given analysis to pass to dx run.
    """
    if sample:
        # running per sample, assume we only wait on the samples previous job
        job_outputs_dict = job_outputs_dict[sample]

    # check if job depends on previous jobs to hold till complete
    dependent_analysis = params.get("depends_on")
    dependent_jobs = []

    if dependent_analysis:
        for id in dependent_analysis:
            for job in find_job_inputs(id, job_outputs_dict, check_key=True):
                # find jobs for every analysis id
                if job:
                    dependent_jobs.append(job)

    print(f'Dependent jobs found: {dependent_jobs}')

    return dependent_jobs


def link_inputs_to_outputs(job_outputs_dict, input_dict, sample=None) -> dict:
    """
    Check input dict for 'analysis_', these will be for linking outputs of
    previous jobs and stored in the job_outputs_dict to input of next job
    """
    if sample:
        # ensure we only use outputs for given sample for per sample workflow
        try:
            job_outputs_dict = job_outputs_dict[sample]
        except KeyError:
            raise KeyError((
                f'{sample} not found in output dict, this is most likely from '
                'this being the first executable called and having '
                'a misconfigured input section in config (i.e. misspelt input)'
                ' that should have been parsed earlier. Check config and try '
                'again. Input dict given: '
                f'{input_dict}'
            ))

    # search input dict for job ids to add
    inputs = list(find_job_inputs('analysis_', input_dict, check_key=True))

    if not inputs:
        # no inputs found to replace
        return input_dict

    for job_input in inputs:
        # for each input, use the analysis id to get the job id containing
        # the required output from the job outputs dict
        match = re.search(r'^analysis_[0-9]{1,2}$', job_input)
        if not match:
            # doesn't seem to be a valid app or worklfow, we cry
            raise RuntimeError((
                f'{job_input} does not seem to be a valid analysis id, check '
                'config and try again'
            ))

        analysis_id = match.group(0)

        # job output has analysis-id: job-id
        # select job id for appropriate analysis id
        job_id = [v for k, v in job_outputs_dict.items() if analysis_id == k]

        # job out should always(?) only have one output with given name, exit
        # for now if more found
        if len(job_id) > 1:
            raise RuntimeError(
                f'More than one job found for {job_input}: {job_id}'
            )

        if not job_id:
            # this shouldn't happen as it will be caught with the regex but
            # double checking anyway
            raise ValueError((
                f"No job id found for given analysis id: {job_input}, please "
                "check that it has the same analysis as a previous job in the "
                "config"
            ))

        # replace analysis id with given job id in input dict
        replace_job_inputs(input_dict, job_input, job_id[0])

    return input_dict


def populate_output_dir_config(executable, output_dict, out_folder) -> dict:
    """
    Loops over stages in dict for output directory naming and adds worlflow /
    app name.
    # pass in app/workflow name to each apps output directory path
    # i.e. will be named /output/{out_folder}/{stage_name}/, where stage
    # name is the human readable name of each stage defined in the config
    """
    for stage, dir in output_dict.items():
        if "OUT-FOLDER" in dir:
            out_folder = out_folder.replace('/output/', '')
            dir = dir.replace("OUT-FOLDER", out_folder)
        if "APP-NAME" in dir:
            # use describe method to get actual name of app with version
            if 'workflow-' in executable:
                workflow_details = dxpy.api.workflow_describe(executable)
                stage_app_id = [
                    (x['id'], x['executable'])
                    for x in workflow_details['stages']
                    if x['id'] == stage
                ]
                if stage_app_id:
                    # get applet id for given stage id
                    stage_app_id = stage_app_id[0][1]
                    applet_details = dxpy.api.workflow_describe(stage_app_id)
                    app_name = applet_details['name']
                else:
                    # not found app ID for stage, going to print message
                    # and continue with using stage id
                    print('Error finding applet ID for naming output dir')
                    app_name = stage
            elif 'app-' or 'applet-' in executable:
                app_details = dxpy.api.workflow_describe(executable)
                app_name = app_details['name']

            # add app/workflow name to output dir name
            dir = dir.replace("APP-NAME", app_name)
            output_dict[stage] = dir

    return output_dict


def check_all_inputs(input_dict) -> None:
    """
    Check for any remaining INPUT-, should be none, if there is most likely
    either a typo in config or invalid input given => raise AssertionError
    """
    # checking if any INPUT- in dict still present
    inputs = find_job_inputs('INPUT-', input_dict, check_key=False)
    _empty = object()

    assert next(inputs, _empty) == _empty, (
        f"Error: unparsed INPUT- still in config, please check readme for "
        f"valid input parameters. Input dict: {input_dict}"
    )


def call_per_sample(
    args, executable, params, sample, config, out_folder,
        job_outputs_dict, executable_out_dirs, fastq_details) -> dict:
    """
    Populate input and output dicts for given workflow and sample, then call
    to dx to start job. Job id is returned and stored in output dict that maps
    the workflow to dx job id for given sample.
    """
    # select input and output dict from config for current workflow / app
    config_copy = deepcopy(config)
    input_dict = config_copy['executables'][executable]['inputs']
    output_dict = config_copy['executables'][executable]['output_dirs']

    # create output directory structure in config
    populate_output_dir_config(executable, output_dict, out_folder)

    # check if stage requires fastqs passing
    if params["process_fastqs"] is True:
        input_dict = add_fastqs(input_dict, fastq_details, sample)

    # find all jobs for previous analyses if next job depends on them finishing
    if params.get("depends_on"):
        dependent_jobs = get_dependent_jobs(
            params, job_outputs_dict, sample=sample)
    else:
        dependent_jobs = []

    if params.get("sample_name_delimeter"):
        # if delimeter specified to split sample name on, use it
        delim = params.get("sample_name_delimeter")

        if delim in sample:
            sample = sample.split(delim)[0]
        else:
            print((
                f'Specified delimeter ({delim}) is not in sample name '
                f'({sample}), ignoring and continuing.'
            ))

    # handle other inputs defined in config to add to inputs
    input_dict = add_other_inputs(
        input_dict, args.dx_project_id, executable_out_dirs, sample)

    # check any inputs dependent on previous job outputs to add
    input_dict = link_inputs_to_outputs(
        job_outputs_dict, input_dict, sample=sample
    )

    # check that all INPUT- have been parsed in config
    check_all_inputs(input_dict)

    # call dx run to start jobs
    print(f"Calling {executable} on sample {sample}")
    print(f'Input dict: {PPRINT(input_dict)}')

    job_id = call_dx_run(
        args, executable, input_dict, output_dict, dependent_jobs)

    if sample not in job_outputs_dict.keys():
        # create new dict to store sample outputs
        job_outputs_dict[sample] = {}

    # map analysis id to dx job id for sample
    job_outputs_dict[sample].update({params['analysis']: job_id})

    return job_outputs_dict


def call_per_run(
    args, executable, params, config, out_folder,
        job_outputs_dict, executable_out_dirs, fastq_details) -> dict:
    """
    Populates input and output dicts from config for given workflow, returns
    dx job id and stores in dict to map workflow -> dx job id.
    """
    # select input and output dict from config for current workflow / app
    input_dict = config['executables'][executable]['inputs']
    output_dict = config['executables'][executable]['output_dirs']

    # create output directory structure in config
    populate_output_dir_config(executable, output_dict, out_folder)

    if params["process_fastqs"] is True:
        input_dict = add_fastqs(input_dict, fastq_details)

    # handle other inputs defined in config to add to inputs
    input_dict = add_other_inputs(
        input_dict, args.dx_project_id, executable_out_dirs)

    # check any inputs dependent on previous job outputs to add
    input_dict = link_inputs_to_outputs(job_outputs_dict, input_dict)

    # find all jobs for previous analyses if next job depends on them finishing
    if params.get("depends_on"):
        dependent_jobs = get_dependent_jobs(params, job_outputs_dict)
    else:
        dependent_jobs = []

    # check that all INPUT- have been parsed in config
    check_all_inputs(input_dict)

    # passing all samples to workflow
    print(f'Calling {params["name"]} for all samples')
    job_id = call_dx_run(
        args, executable, input_dict, output_dict, dependent_jobs)

    PPRINT(input_dict)
    PPRINT(output_dict)

    # map workflow id to created dx job id
    job_outputs_dict[params['analysis']] = job_id

    return job_outputs_dict


def load_test_data(args) -> list:
    """
    Read in file ids of fastqs and sample names from test_samples file to test
    calling workflows
    """
    with open(args.test_samples) as f:
        fastq_details = f.read().splitlines()

    fastq_details = [(x.split()[0], x.split()[1]) for x in fastq_details]

    return fastq_details


def parse_args() -> argparse.ArgumentParser:
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
        '--dx_project_id', required=False,
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
    parser.add_argument(
        '--test_samples',
        help=(
            'for test use only. Pass in file with 1 sample per line '
            'specifing file-id of fastq and sample name'
        )
    )

    args = parser.parse_args()

    # turn comma separated sample str to python list
    args.samples = [x.replace(' ', '') for x in args.samples.split(',') if x]

    return args


def main():
    """
    Main function to run workflows
    """
    args = parse_args()

    config = load_config(args.config_file)
    run_time = time_stamp()

    if not args.dx_project_id:
        # output project not specified, create new one from run id
        args = create_dx_project(args, config)

    # set context to project for running jobs
    dxpy.set_workspace_id(args.dx_project_id)

    if args.bcl2fastq_id:
        # get details of job that ran to perform demultiplexing
        bcl2fastq_job = dxpy.bindings.dxjob.DXJob(
            dxid=args.bcl2fastq_id).describe()
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
        # bcl2fastq wasn't run => we have either a dir of fastqs being passed,
        # this is for tso500 or something else weird this is going to need some
        # thought and clever handling to know what is being passed
        fastq_details = []

        # test data - myeloid sample
        if args.test_samples:
            fastq_details = load_test_data(args)

    # check per_sample defined for all workflows / apps before starting
    for executable, params in config['executables'].items():
        assert 'per_sample' in params.keys(), (
            f"per_sample key missing from {executable} in config, check config"
            "and re run"
        )

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

        # create output folder for workflow, unique by datetime stamp
        out_folder = f'/output/{params["name"]}-{run_time}'
        out_folder = create_dx_folder(args, out_folder)
        executable_out_dirs[params['analysis']] = out_folder

        if params['per_sample'] is True:
            # run workflow / app on every sample
            print(f'\nCalling {params["name"]} per sample')

            # loop over given sample and call workflow
            for sample in args.samples:
                job_outputs_dict = call_per_sample(
                    args, executable, params, sample, config, out_folder,
                    job_outputs_dict, executable_out_dirs, fastq_details
                )

        elif params['per_sample'] is False:
            # run workflow / app on all samples at once
            # need to explicitly check if False vs not given, must always be
            # defined to ensure running correctly
            job_outputs_dict = call_per_run(
                args, executable, params, config, out_folder, job_outputs_dict,
                executable_out_dirs, fastq_details
            )
        else:
            # per_sample is not True or False, exit
            raise ValueError(
                f"per_sample declaration for {executable} is not True or "
                f"False ({params['per_sample']}). Please check the config"
            )

    print("Completed calling jobs")


if __name__ == "__main__":
    main()
