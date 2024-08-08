from copy import deepcopy
import dxpy as dx
import os

from dx_utils import get_job_output_details
from manage_dict import ManageDict
from utils import prettier_print


def call_per_sample(
    executable,
    exe_names,
    input_classes,
    params,
    sample,
    config,
    out_folder,
    job_outputs_dict,
    executable_out_dirs,
    fastq_details,
    instance_types,
    args
) -> dict:
    """
    Populate input and output dicts for given workflow and sample, then
    call to dx to start job. Job id is returned and stored in output dict
    that maps the workflow to dx job id for given sample.

    Parameters
    ----------
    executable : str
        human readable name of dx executable (workflow-, app- or applet-)
    exe_names : dict
        mapping of executable IDs to human readable names
    input_classes : dict
        mapping of executable inputs -> expected types
    params : dict
        dictionary of parameters specified in config for running analysis
    sample : str, default None
        optional, sample name used to limit searching for previous analyes
    config : dict
        low level assay config read from json file
    out_folder : str
        name of parent dir path
    job_outputs_dict : dict
        dictionary of previous job outputs
    executable_out_dirs : dict
        dict of analysis stage to its output dir path, used to pass output
        of an analysis to input of another (i.e.
        analysis_1 : /path/to/output)
    fastq_details : list of tuples
        list with tuple per fastq containing (DNAnexus file id, filename)
    instance_types : dict
        mapping of instances to use for apps

    Returns
    -------
    job_outputs_dict : dict
        dictionary of analysis stages to dx job ids created
    """

    # select input and output dict from config for current workflow / app
    config_copy = deepcopy(config)
    input_dict = config_copy['executables'][executable]['inputs']
    output_dict = config_copy['executables'][executable]['output_dirs']

    extra_args = params.get("extra_args", {})

    if params['executable_name'].startswith('TSO500_reports_workflow'):
        # handle specific inputs of eggd_TSO500 -> TSO500 workflow

        # get the job ID for previous eggd_tso500 job, this _should_ just
        # be analysis_1, but check anyway incase other apps added in future
        # per sample jobs would be stored in prev_jobs dict under sample key,
        # so we can just check for analysis_ for prior apps run once per run
        jobs = [
            job_outputs_dict[x]
            for x in job_outputs_dict
            if x.startswith('analysis_')
        ]
        jobs = {dx.describe(job_id).get('name'): job_id for job_id in jobs}
        tso500_id = [
            v for k, v in jobs.items() if k.startswith('eggd_tso500')
        ]

        assert len(tso500_id) == 1, (
            "Could not correctly find prior eggd_tso500 "
            f"job, jobs found: {jobs}"
        )

        tso500_id = tso500_id[0]

        # get details of the job to pull files from
        all_output_files, job_output_ids = get_job_output_details(tso500_id)

        # try add all eggd_tso500 app outputs to reports workflow input
        input_dict = ManageDict().populate_tso500_reports_workflow(
            input_dict=input_dict,
            sample=sample,
            all_output_files=all_output_files,
            job_output_ids=job_output_ids
        )

    # check if stage requires fastqs passing
    if params["process_fastqs"] is True:
        input_dict = ManageDict().add_fastqs(
            input_dict=input_dict,
            fastq_details=fastq_details,
            sample=sample
        )

    # find all jobs for previous analyses if next job depends on them
    if params.get("depends_on"):
        dependent_jobs = ManageDict().get_dependent_jobs(
            params=params,
            job_outputs_dict=job_outputs_dict,
            sample=sample
        )
    else:
        dependent_jobs = []

    sample_prefix = sample

    if params.get("sample_name_delimeter"):
        # if delimeter specified to split sample name on, use it
        delim = params.get("sample_name_delimeter")

        if delim in sample:
            sample_prefix = sample.split(delim)[0]
        else:
            prettier_print((
                f'Specified delimeter ({delim}) is not in sample name '
                f'({sample}), ignoring and continuing...'
            ))

    # handle other inputs defined in config to add to inputs
    # sample_prefix passed to pass to INPUT-SAMPLE_NAME
    input_dict = ManageDict().add_other_inputs(
        input_dict=input_dict,
        args=args,
        executable_out_dirs=executable_out_dirs,
        sample=sample,
        sample_prefix=sample_prefix
    )

    # check any inputs dependent on previous job outputs to add
    input_dict = ManageDict().link_inputs_to_outputs(
        job_outputs_dict=job_outputs_dict,
        input_dict=input_dict,
        analysis=params["analysis"],
        per_sample=True,
        sample=sample
    )

    # check input types correctly set in input dict
    input_dict = ManageDict().check_input_classes(
        input_dict=input_dict,
        input_classes=input_classes[executable]
    )

    # check that all INPUT- have been parsed in config
    ManageDict().check_all_inputs(input_dict)

    # set job name as executable name and sample name
    job_name = f"{params['executable_name']}-{sample}"

    # create output directory structure in config
    ManageDict().populate_output_dir_config(
        executable=executable,
        exe_names=exe_names,
        output_dict=output_dict,
        out_folder=out_folder
    )

    # call dx run to start jobs
    prettier_print(
        f"\nCalling {params['executable_name']} ({executable}) "
        f"on sample {sample}"
        )

    if input_dict.keys:
        prettier_print(f'\nInput dict: {input_dict}')

    job_id = call_dx_run(
        executable=executable,
        job_name=job_name,
        input_dict=input_dict,
        output_dict=output_dict,
        prev_jobs=dependent_jobs,
        extra_args=extra_args,
        instance_types=instance_types
    )

    if sample not in job_outputs_dict.keys():
        # create new dict to store sample outputs
        job_outputs_dict[sample] = {}

    # map analysis id to dx job id for sample
    job_outputs_dict[sample].update({params['analysis']: job_id})

    return job_outputs_dict


def call_per_run(
    executable,
    exe_names,
    input_classes,
    params,
    config,
    out_folder,
    job_outputs_dict,
    executable_out_dirs,
    fastq_details,
    instance_types,
    args,
    upload_tars=None
) -> dict:
    """
    Populates input and output dicts from config for given workflow,
    returns dx job id and stores in dict to map workflow -> dx job id.

    Parameters
    ----------
    executable : str
        human readable name of dx executable (workflow-, app- or applet-)
    exe_names : dict
        mapping of executable IDs to human readable names
    input_classes : dict
        mapping of executable inputs -> expected types
    params : dict
        dictionary of parameters specified in config for running analysis
    config : dict
        low level assay config read from json file
    out_folder : str
        name of parent dir path
    job_outputs_dict : dict
        dictionary of previous job outputs
    executable_out_dirs : dict
        dict of analysis stage to its output dir path, used to pass
        output of an analysis to input of another (i.e.
        analysis_1 : /path/to/output)
    fastq_details : list of tuples
        list with tuple per fastq containing (DNAnexus file id, filename)
    instance_types : dict
        mapping of instances to use for apps

    Returns
    -------
    job_outputs_dict : dict
        dictionary of analysis stages to dx job ids created
    """
    # select input and output dict from config for current workflow / app
    input_dict = config['executables'][executable]['inputs']
    output_dict = config['executables'][executable]['output_dirs']

    extra_args = params.get("extra_args", {})

    if params["process_fastqs"] is True:
        input_dict = ManageDict().add_fastqs(input_dict, fastq_details)

    # add upload tars as input if INPUT-UPLOAD_TARS present
    if upload_tars:
        input_dict = ManageDict().add_upload_tars(
            input_dict=input_dict,
            upload_tars=upload_tars
        )

    # handle other inputs defined in config to add to inputs
    input_dict = ManageDict().add_other_inputs(
        input_dict=input_dict,
        args=args,
        executable_out_dirs=executable_out_dirs
    )

    # get any filters from config to apply to job inputs
    input_filter_dict = config['executables'][executable].get('inputs_filter')

    # check any inputs dependent on previous job outputs to add
    input_dict = ManageDict().link_inputs_to_outputs(
        job_outputs_dict=job_outputs_dict,
        input_dict=input_dict,
        analysis=params["analysis"],
        input_filter_dict=input_filter_dict,
        per_sample=False
    )

    # check input types correctly set in input dict
    input_dict = ManageDict().check_input_classes(
        input_dict=input_dict,
        input_classes=input_classes[executable]
    )

    # find all jobs for previous analyses if next job depends on them
    if params.get("depends_on"):
        dependent_jobs = ManageDict().get_dependent_jobs(
            params=params,
            job_outputs_dict=job_outputs_dict
        )
    else:
        dependent_jobs = []

    # check that all INPUT- have been parsed in config
    ManageDict().check_all_inputs(input_dict)

    # create output directory structure in config
    ManageDict().populate_output_dir_config(
        executable=executable,
        exe_names=exe_names,
        output_dict=output_dict,
        out_folder=out_folder
    )

    # passing all samples to workflow
    prettier_print(f'\nCalling {params["name"]} for all samples')
    job_id = call_dx_run(
        executable=executable,
        job_name=params['executable_name'],
        input_dict=input_dict,
        output_dict=output_dict,
        prev_jobs=dependent_jobs,
        extra_args=extra_args,
        instance_types=instance_types
    )

    # map workflow id to created dx job id
    job_outputs_dict[params['analysis']] = job_id

    return job_outputs_dict


def call_dx_run(
    executable, job_name, input_dict,
    output_dict, prev_jobs, extra_args, instance_types
) -> str:
    """
    Call workflow / app with populated input and output dicts

    Returns id of submitted job

    Parameters
    ----------
    executable : str
        human readable name of executable (i.e. workflow / app / applet)
    job_name : str
        name to assign to job, will be combination of human readable name of
        exectuable and sample ID
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
            project=self.args.dx_project_id
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
            project=self.args.dx_project_id,
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
            project=self.args.dx_project_id,
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
        f'Started analysis in project {self.args.dx_project_id}, '
        f'job: {job_id}'
    )

    with open('job_id.log', 'a') as fh:
        # log of current executable jobs
        fh.write(f'{job_id} ')

    with open('all_job_ids.log', 'a') as fh:
        # log of all launched job IDs
        fh.write(f'{job_id},')

    if self.args.testing:
        with open('testing_job_id.log', 'a') as fh:
            fh.write(f'{job_id} ')

    return job_id
