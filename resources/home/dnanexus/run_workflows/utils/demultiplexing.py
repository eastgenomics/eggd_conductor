import os
import re

import dxpy as dx

from utils.utils import prettier_print, select_instance_types, time_stamp
from utils.WebClasses import Slack


def set_config_for_demultiplexing(*configs):
    """Select the config parameters that will be used in the
    demultiplexing job using the biggest instance type as the tie breaker.
    This also allows selection of additional args that could be
    antagonistic.

    Parameters
    ----------
    *configs:
        Variable number of config files to compare between themselves
    """

    demultiplex_configs = []
    core_nbs = []

    for config in configs:
        demultiplex_config = config.get("demultiplex_config", None)

        if demultiplex_config:
            instance_type = demultiplex_config.get("instance_type", 0)

            if instance_type:
                demultiplex_configs.append(config)
                core_nbs.append(int(instance_type.split("_")[-1].strip("x")))

    if core_nbs:
        bigger_core_nb = max(core_nbs)
        return demultiplex_configs[core_nbs.index(bigger_core_nb)].get(
            "demultiplex_config", None
        )

    return


def move_demultiplex_qc_files(
    project_id, demultiplex_project, demultiplex_folder
):
    """Move demultiplex qc files to the appropriate projects

    Parameters
    ----------
    project_id : str
        Project id to move the files to
    demultiplex_project : str
        Project id where the files to move are located
    demultiplex_folder : str
        Folder where the files to move should be located
    """

    # check for and copy / move the required files for multiQC from
    # bclfastq or bclconvert into a folder in root of the analysis project
    qc_files = [
        "Stats.json",  # bcl2fastq
        "RunInfo.xml",  # ↧ bclconvert
        "Demultiplex_Stats.csv",
        "Quality_Metrics.csv",
        "Adapter_Metrics.csv",
        "Top_Unknown_Barcodes.csv",
    ]

    # need to first create destination folder
    dx.api.project_new_folder(
        object_id=project_id,
        input_params={"folder": "/demultiplex_multiqc_files", "parents": True},
    )

    for file in qc_files:
        dx_object = list(
            dx.bindings.search.find_data_objects(
                name=file,
                project=demultiplex_project,
                folder=demultiplex_folder,
            )
        )

        if dx_object:
            dx_file = dx.DXFile(
                dxid=dx_object[0]["id"], project=dx_object[0]["project"]
            )

            if project_id == demultiplex_project:
                # demultiplex output in the analysis project => need to move
                # instead of cloning (this is most likely just for testing)
                dx_file.move(folder="/demultiplex_multiqc_files")
            else:
                # copying to separate analysis project
                dx_file.clone(
                    project=project_id, folder="/demultiplex_multiqc_files"
                )


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
    demultiplex_project = demultiplex_job["project"]
    demultiplex_folder = demultiplex_job["folder"]

    # find all fastqs from demultiplex job, return list of dicts with details
    fastq_details = list(
        dx.search.find_data_objects(
            name="*.fastq*",
            name_mode="glob",
            project=demultiplex_project,
            folder=demultiplex_folder,
            describe=True,
        )
    )
    # build list of tuples with fastq name and file ids
    fastq_details = [(x["id"], x["describe"]["name"]) for x in fastq_details]
    # filter out Undetermined fastqs
    fastq_details = [
        x for x in fastq_details if not x[1].startswith("Undetermined")
    ]

    prettier_print(f"\nFastqs parsed from demultiplexing job {job_id}")
    prettier_print(fastq_details)

    return fastq_details


def demultiplex(
    app_id,
    app_name,
    testing,
    demultiplex_config,
    demultiplex_output,
    sentinel_file,
    run_id,
) -> str:
    """Run demultiplexing app, holds until app completes.

    Either an app name, app ID or applet ID may be specified as input

    Parameters
    ----------
    app_id : str
        App/applet id to use for running demultiplexing
    app_name : _type_
        Name of the app id
    testing : bool
        Testing mode boolean
    demultiplex_config : dict
        Dict containing demultiplexing parameters
    demultiplex_output : str
        Path to the demultiplexing directory
    sentinel_file : DXRecord
        DXRecord object for the sentinel file
    run_id : str
        Run id

    Returns
    -------
    str
        ID of demultiplexing job

    Raises
    ------
    AssertionError
        Raised if fastqs are already present in the given output directory
        for the demultiplexing job
    RuntimeError
        Raised when app ID / name for demultiplex name are invalid
    """

    if not testing:
        if not demultiplex_output:
            # set output path to parent of sentinel file
            out = dx.describe(sentinel_file)
            sentinel_path = f"{out.get('project')}:{out.get('folder')}"
            demultiplex_output = sentinel_path.replace("/runs", "")
    else:
        if not demultiplex_output:
            # running in testing and going to demultiplex -> dump output to
            # our testing analysis project to not go to sentinel file dir
            demultiplex_output = f"{run_id}:/demultiplex_{time_stamp()}"

    (demultiplex_project, demultiplex_folder) = demultiplex_output.split(":")

    prettier_print(f"demultiplex app ID set: {app_id}")
    prettier_print(f"demultiplex app name set: {app_name}")
    prettier_print(
        f"optional config specified for demultiplexing: {demultiplex_config}"
    )
    prettier_print(f"demultiplex out: {demultiplex_output}")
    prettier_print(f"demultiplex project: {demultiplex_project}")
    prettier_print(f"demultiplex folder: {demultiplex_folder}")

    instance_type = None
    additional_args = None

    if demultiplex_config:
        instance_type = demultiplex_config.get("instance_type", None)
        additional_args = demultiplex_config.get("additional_args", "")

    if isinstance(instance_type, dict):
        # instance type defined in config is a mapping for multiple
        # flowcells, select appropriate one for current flowcell
        instance_type = select_instance_types(
            run_id=run_id, instance_types=instance_type
        )

    prettier_print(
        f"Instance type selected for demultiplexing: {instance_type}"
    )

    inputs = {"upload_sentinel_record": {"$dnanexus_link": sentinel_file}}

    if additional_args:
        inputs["advanced_opts"] = additional_args

    if os.environ.get("SAMPLESHEET_ID"):
        #  get just the ID of samplesheet in case of being formatted as
        # {'$dnanexus_link': 'file_id'} and add to inputs as this
        match = re.search(r"file-[\d\w]*", os.environ.get("SAMPLESHEET_ID"))

        if match:
            inputs["sample_sheet"] = {"$dnanexus_link": match.group()}

    prettier_print(f"\nInputs set for running demultiplexing: {inputs}")

    # check no fastqs are already present in the output directory for
    # demultiplexing, exit if any present to prevent making a mess
    # with demultiplexing output
    fastqs = list(
        dx.find_data_objects(
            name="*.fastq*",
            name_mode="glob",
            project=demultiplex_project,
            folder=demultiplex_folder,
        )
    )

    assert not fastqs, Slack().send(
        "FastQs already present in output directory for demultiplexing: "
        f"`{demultiplex_output}`.\n\n"
        "Exiting now to not potentially pollute a previous demultiplex "
        "job output. \n\n"
        "Please either move the sentinel file or set the demultiplex "
        "output directory with `-iDEMULTIPLEX_OUT`"
    )

    if app_id.startswith("applet-"):
        job = dx.bindings.dxapplet.DXApplet(dxid=app_id).run(
            applet_input=inputs,
            project=demultiplex_project,
            folder=demultiplex_folder,
            priority="high",
            instance_type=instance_type,
        )
    elif app_id.startswith("app-") or app_name:
        # running from app, prefer name over ID
        # have to set to None to only use ID or name if both set
        if app_name:
            app_id = None
        else:
            app_name = None

        job = dx.bindings.dxapp.DXApp(dxid=app_id, name=app_name).run(
            app_input=inputs,
            project=demultiplex_project,
            folder=demultiplex_folder,
            priority="high",
            instance_type=instance_type,
        )
    else:
        raise RuntimeError(
            f"Provided demultiplex app ID does not appear valid: {app_id}"
        )

    # tag demultiplexing job so we easily know it was launched by conductor
    job.add_tags(
        tags=[f'Job run by eggd_conductor: {os.environ.get("PARENT_JOB_ID")}']
    )

    prettier_print(
        f"Starting demultiplexing ({job.id}), "
        "holding app until completed..."
    )

    try:
        # holds app until demultiplexing job returns success
        job.wait_on_done()
    except dx.exceptions.DXJobFailureError as err:
        # dx job error raised (i.e. failed, timed out, terminated)
        job_url = (
            f"platform.dnanexus.com/projects/"
            f"{demultiplex_project.replace('project-', '')}"
            "/monitor/job/"
            f"{job.id}"
        )

        Slack().send(
            f"Demultiplexing job failed!\n\nError: {err}\n\n"
            f"Demultiplexing job: {job_url}"
        )
        raise dx.exceptions.DXJobFailureError()

    prettier_print("Demuliplexing completed!")

    return job
