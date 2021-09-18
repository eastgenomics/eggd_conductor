"""
Script to call workflow(s) / apps from a given config

Jethro Rainford 210902
"""
import argparse
from copy import deepcopy
from datetime import datetime
import json
import pprint
import re
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


def create_dx_project(args) -> argparse.ArgumentParser:
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

    args.dx_project_id = project_id

    return args


def create_dx_folder(args, out_folder):
    """
    Create output folder in DNAnexus project
    """
    for i in range(1, 100):
        # sanity check, should only be 1 or 2 already existing at most
        out_folder = f'{out_folder}-{i}'
        try:
            dxpy.api.project_new_folder(
                args.dx_project_id,
                input_params={'folder': out_folder}
            )
            print(f'Created output folder: {out_folder}')
            break
        except dxpy.exceptions.ResourceNotFound:
            # work around for bug in dxpy, if parents True is specified
            # and folder already exists it will not correctly raise
            # exception, therefore first try without to force creating
            # parent /output/, then the normal try will correctly increment
            # the numbering
            print("/output/ not yet created, trying again")
            dxpy.api.project_new_folder(
                args.dx_project_id,
                input_params={'folder': out_folder, 'parents': True}
            )
            print(f'Created output folder: {out_folder}')
            break
        except dxpy.exceptions.InvalidState:
            # catch exception where folder already exists, increment number
            print(f'{out_folder} already exists, creating new folder')
            continue

    return out_folder


def call_dx_run(args, executable, input_dict, output_dirs_dict):
    """
    Call workflow / app, returns id of submitted job
    """
    print(input_dict)
    if 'workflow-' in executable:
        job_handle = dxpy.bindings.dxworkflow.DXWorkflow(
            dxid=executable, project=args.dx_project_id
        ).run(workflow_input=input_dict, stage_folders=output_dirs_dict)
    elif 'app-' in executable:
        job_handle = dxpy.bindings.dxapp.DXApp(dxid=executable).run(
            app_input=input_dict, project=args.dx_project_id,
            folder=output_dirs_dict[executable]
        )
    elif 'applet-' in executable:
        job_handle = dxpy.bindings.dxapplet.DXApplet(dxid=executable).run(
            applet_input=input_dict, project=args.dx_project_id,
            folder=output_dirs_dict[executable]
        )
    else:
        # doesn't appear to be valid workflow or app
        raise Exception

    job_details = job_handle.describe()
    job_id = job_details.get('id')

    print(f'Started analysis in project {args.dx_project_id}, job: {job_id}')

    return job_id


def add_fastqs(input_dict, fastq_details, sample=None):
    """
    If process_fastqs set to true, function is called to populate input dict
    with appropriate fastq file ids
    """
    if sample:
        # sample specified => running per sample, if not using all fastqs
        fastq_details = [x for x in fastq_details if sample in x[1]]

    # fastqs should always be named with R1/2_001
    r1_fastqs = [x for x in fastq_details if 'R1_001.fastq' in x[1]]
    r2_fastqs = [x for x in fastq_details if 'R2_001.fastq' in x[1]]

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


def add_sample_name(input_dict, sample):
    """
    Adds sample name to input dict
    """
    # check if config has field(s) expecting the sample name as input
    keys = [k for k, v in input_dict.items() if v == 'INPUT-SAMPLE_NAME']
    if keys:
        for key in keys:
            input_dict[key] = sample

    return input_dict


def find_job_inputs(input_dict, key_path=[]):
    """
    Recursive function to find all values with identifying prefix, these
    require replacing with appropriate job output file ids. Returns a generator
    with input fields to replace
    """
    input_prefix = 'INPUTS-'

    for key, value in input_dict.items():
        if isinstance(value, dict):
            key_path.append(key)
            yield from find_job_inputs(value, key_path)
        elif input_prefix in value:
            key_path.append(key)
            yield value

            key_path.clear()  # remove path for next key


def replace_job_inputs(input_dict, job_input, output_file):
    """
    Recursively traverse through nested dictionary and replace any matching
    INPUT- with given DNAnexus file id in correct format.

    job_input = INPUTS-{output name (i.e. vcf)}
    output_file = dx file id
    """
    for key, val in input_dict.items():
        if isinstance(val, dict):
            replace_job_inputs(val, job_input, output_file)
        if val == job_input:
            input_dict[key] = output_file


def populate_input_dict(job_outputs_dict, input_dict, sample=None):
    """
    Check input dict for remaining 'INPUTS-', any left *should* be
    outputs of previous jobs and stored in the job_outputs_dict and can
    be parsed in to link up outputs
    """
    # first checking if any INPUT- in dict to fill, if not return
    # stops error of trying to select sample from output dict if this is the
    # first job and won't have anything in the dict or any more inputs to fill
    inputs = find_job_inputs(input_dict)
    _empty = object()

    if next(inputs, _empty) == _empty:
        # generator returned empty object => no inputs found to fill
        return input_dict

    if sample:
        # ensure we only use outputs for given sample for per sample workflow
        try:
            job_outputs_dict = job_outputs_dict[sample]
        except KeyError:
            raise KeyError((
                f'{sample} not found in output dict, this is most likely from '
                'this being the first executable called and having a misconfigured '
                'input section in config (i.e. misspelt input) that should have '
                'been parsed earlier. Check config and try again. Input dict given: '
                f'{input_dict}'
            ))

    for job_input in find_job_inputs(input_dict):
        # find_job_inputs() returns generator, loop through for each input to
        # replace in the input dict

        # for each input, use the workflow/app id to get the job id containing
        # the required output
        match = re.search(r'(workflow|app|applet)-[A-Za-z0-9]*', job_input)
        if not match:
            # doesn't seem to be a valid app or worklfow, we cry
            raise RuntimeError(
                f'{job_input} does not seem to be a valid app or workflow'
            )

        executable_id = match.group(0)

        # job output has workflow-id: job-id, select job id for appropriate
        # workflow id
        job_id = [x for x in job_outputs_dict.keys() if executable_id in x]

        # job out should always(?) only have one output with given name, exit
        # for now if more found
        if len(job_id) > 1:
            raise RuntimeError(
                f'More than one job found for {job_input}: {job_id}'
            )

        if not job_id:
            raise ValueError((
                f"No file found for {job_input} in output files from previous "
                "workflow, please check config INPUTS- matches output names"
            ))

        # format for parsing into input_dict
        output_file = job_input.replace('INPUT-', '').replace(executable_id, '')
        output_file = f'{job_id}:{output_file}'

        # get file id for given field from dict to replace in input_dict

        replace_job_inputs(input_dict, job_input, output_file)

    return input_dict


def populate_output_dir_config(executable, output_dirs_dict, out_folder):
    """
    Loops over stages in dict for output directory naming and adds worlflow /
    app name.
    # pass in app/workflow name to each apps output directory path
    # i.e. will be named /output/{out_folder}/{stage_name}/, where stage
    # name is the human readable name of each stage defined in the config
    """
    for stage, dir in output_dirs_dict.items():
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
            output_dirs_dict[stage] = dir

    return output_dirs_dict


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

    args = parser.parse_args()

    # turn comma separated sample str to python list
    args.samples = [x.replace(' ', '') for x in args.samples.split(',')]

    return args


def main():
    """
    Main function to run workflows
    """
    args = parse_args()

    config = load_config(args.config_file)
    run_time = time_stamp()

    pp = pprint.PrettyPrinter(indent=4)

    if not args.dx_project_id:
        # output project not specified, create new one from run id
        args = create_dx_project(args)

    # set context for running jobs
    dxpy.set_workspace_id(args.dx_project_id)

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

        # test data - myeloid sample
        fastq_details = [
            ('file-G50BZJQ4BQBZz22v4jVkP5f0',
            '2107285-21232Z0085-PB-CLL-MYE-F-EGG2_S35_L001_R1_001.fastq.gz'),
            ('file-G50BbYj4BQBV0pQF4ZBbxXJF',
            '2107285-21232Z0085-PB-CLL-MYE-F-EGG2_S35_L002_R1_001.fastq.gz'),
            ('file-G50BZK04BQBbfB794bBZQ5qV',
            '2107285-21232Z0085-PB-CLL-MYE-F-EGG2_S35_L001_R2_001.fastq.gz'),
            ('file-G50BbZ84BQBvqx1xB8KBgBYJ',
            '2107285-21232Z0085-PB-CLL-MYE-F-EGG2_S35_L002_R2_001.fastq.gz'),
            ('file-G50BZJQ4BQBZz22v4jVkP5f0',
            '2107285-21232Z0085-PB-CLL-MYE-F-EGG2_S36_L001_R1_001.fastq.gz'),
            ('file-G50BbYj4BQBV0pQF4ZBbxXJF',
            '2107285-21232Z0085-PB-CLL-MYE-F-EGG2_S36_L002_R1_001.fastq.gz'),
            ('file-G50BZK04BQBbfB794bBZQ5qV',
            '2107285-21232Z0085-PB-CLL-MYE-F-EGG2_S36_L001_R2_001.fastq.gz'),
            ('file-G50BbZ84BQBvqx1xB8KBgBYJ',
            '2107285-21232Z0085-PB-CLL-MYE-F-EGG2_S36_L002_R2_001.fastq.gz')
        ]

    # check per_sample defined for all workflows / apps before starting
    for executable, params in config['executables'].items():
        assert 'per_sample' in params.keys(), (
            f"per_sample key missing from {executable} in config, check config"
            "and re run"
        )

    # dict to add all stage output names and file ids for every sample to,
    # used to pass correct file ids to subsequent worklow/app calls
    job_outputs_dict = {}

    for executable, params in config['executables'].items():
        # for each workflow/app, check if its per sample or all samples and
        # run correspondingly
        print(f'Configuring {executable} to start jobs')

        # create output folder for workflow, unique by datetime stamp
        out_folder = f'/output/{params["name"]}-{run_time}'
        out_folder = create_dx_folder(args, out_folder)

        if params['per_sample'] is True:
            # run workflow / app on every sample
            print(f'Calling {params["name"]} per sample')

            # loop over given sample and call workflow
            for sample in args.samples:
                # copy config to add sample info to for calling workflow
                # select input and output dict from config for current workflow / app
                sample_config = deepcopy(config)

                input_dict = sample_config['executables'][executable]['inputs']
                output_dirs_dict = sample_config['executables'][executable]['output_dirs']

                # create output directory structure in config
                populate_output_dir_config(executable, output_dirs_dict, out_folder)

                # check if stage requires fastqs passing
                if "process_fastqs" in params:
                    input_dict = add_fastqs(input_dict, fastq_details, sample)

                # add sample name where required
                input_dict = add_sample_name(input_dict, sample)

                # check for any more inputs to add
                input_dict = populate_input_dict(
                    job_outputs_dict, input_dict, sample=sample
                )

                # call dx run to start jobs
                print(f"Calling {executable} on sample {sample}")
                job_id = call_dx_run(args, executable, input_dict, output_dirs_dict)

                pp.pprint(input_dict)
                pp.pprint(output_dirs_dict)

                if sample not in job_outputs_dict.keys():
                    # create new dict to store sample outputs
                    job_outputs_dict[sample] = {}

                # map workflow id to dx job id for sample
                job_outputs_dict[sample].update({executable: job_id})

        elif params['per_sample'] is False:
            # need to explicitly check if False vs not given, must always be
            # defined to ensure running correctly

            # copy config to add sample info to for calling workflow
            # select input and output dict from config for current workflow / app
            sample_config = deepcopy(config)
            input_dict = sample_config['executables'][executable]['inputs']
            output_dirs_dict = sample_config['executables'][executable]['output_dirs']

            # create output directory structure in config
            populate_output_dir_config(executable, output_dirs_dict, out_folder)

            if "process_fastqs" in params:
                input_dict = add_fastqs(input_dict, fastq_details)

            # check for any more inputs to add
            input_dict = populate_input_dict(job_outputs_dict, input_dict)

            # passing all samples to workflow
            print(f'Calling {params["name"]} for all samples')
            job_id = call_dx_run(args, executable, input_dict, output_dirs_dict)

            pp.pprint(input_dict)
            pp.pprint(output_dirs_dict)

            # map workflow id to created dx job id
            job_outputs_dict[executable] = job_id

        else:
            # per_sample is not True or False, exit
            raise ValueError(
                f"per_sample declaration for {executable} is not True or "
                f"False ({params['per_sample']}). Please check the config"
            )

        input_dict.clear()
        output_dirs_dict.clear()

    print("Completed calling jobs")


if __name__ == "__main__":
    main()
