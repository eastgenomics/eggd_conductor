import os
import re

import dxpy as dx
from utils.utils import (
    prettier_print,
    select_instance_types,
    sort_key,
    time_stamp,
)
from utils.WebClasses import Slack


def set_config_for_demultiplexing(*configs):
    """Select the config parameters that will be used in the
    demultiplexing job using the biggest instance type as the tie breaker.
    If there is no bigger instance type, the additional args and app id will be
    used as tiebreaker.

    Tiebreakers are applied in order (instance type, additional args,
    app id). Each tiebreaker only considers the demultiplex configs that
    define the key it compares. As soon as a tiebreaker narrows the
    candidates down to a single value, the first demultiplex config
    containing that value is returned.

    Parameters
    ----------
    *configs : dict
        Variable number of assay config dicts to compare between
        themselves, each of which may contain a "demultiplex_config" key

    Returns
    -------
    dict | None
        Demultiplex config selected from one of the given configs, or
        None if no config has a demultiplex config or no tiebreaker
        could select a single one

    Raises
    ------
    AssertionError
        Raised by the additional args or app id tiebreakers if the
        configs specify conflicting values
    """

    # only keep the configs that have a non-empty demultiplex config
    demultiplex_configs = [
        config.get("demultiplex_config")
        for config in configs
        if config.get("demultiplex_config")
    ]

    if len(demultiplex_configs) == 0:
        return

    # config keys to compare and the function used to compare them,
    # ordered from highest to lowest priority
    tiebreakers = (
        ("instance_type", instance_type_tiebreaker),
        ("additional_args", additional_args_tiebreaker),
        ("app_id", app_id_tiebreaker),
    )

    no_key_flags = []

    for config_key, function in tiebreakers:
        # demultiplex configs that define the key for this tiebreaker
        candidate_configs = [
            demultiplex_config
            for demultiplex_config in demultiplex_configs
            if demultiplex_config.get(config_key)
        ]
        if not any(candidate_configs):
            # no config defines this key, move on to the next tiebreaker
            no_key_flags.append(True)
            continue

        # values that won the tiebreaker
        values = function(*candidate_configs)

        # demultiplex configs that contain one of the winning values
        demultiplex_config_from_tiebreaker = get_demultiplex_config_from_value(
            candidate_configs, values
        )

        if len(values) == 1:
            # single winning value, no need for further tiebreakers
            return demultiplex_config_from_tiebreaker[0]

    # couldn't find any match keys in the demultiplex configs
    if no_key_flags == [True, True, True]:
        return

    raise Exception(
        f"Couldn't select a demultiplex config given the options: {demultiplex_configs}"
    )


def instance_type_tiebreaker(*configs):
    """Select the biggest instance type from the given demultiplex
    configs, using sort_key to order the instance types by number of
    cores, then memory, then storage, then version.

    Parameters
    ----------
    *configs : dict
        Variable number of demultiplex config dicts, each of which may
        contain an "instance_type" key

    Returns
    -------
    list
        Instance types sharing the biggest size, i.e. a single instance
        type unless several configs specify instance types of the same
        size. Empty list if no config specifies an instance type.
    """

    instance_types = [
        config.get("instance_type")
        for config in configs
        if config.get("instance_type")
    ]

    if not instance_types:
        return []

    # sort key of the biggest instance type
    top_instance_type = max(
        sort_key(instance_type) for instance_type in instance_types
    )

    # keep every instance type that is as big as the biggest one
    return [
        instance_type
        for instance_type in instance_types
        if sort_key(instance_type) == top_instance_type
    ]


def additional_args_tiebreaker(*configs):
    """Check that the given demultiplex configs agree on the additional
    args to pass to the demultiplexing app.

    Parameters
    ----------
    *configs : dict
        Variable number of demultiplex config dicts, each of which may
        contain an "additional_args" key

    Returns
    -------
    list
        Additional args from every config that specifies them. As they
        must all be identical, the list only has more than one element
        if several configs specify the same additional args.

    Raises
    ------
    AssertionError
        Raised if the configs specify different additional args
    """

    additional_args = [
        config.get("additional_args")
        for config in configs
        if config.get("additional_args")
    ]

    unique_additional_args = list(
        {additional_args for additional_args in additional_args}
    )

    # additional args are conflicting
    assert (
        len(unique_additional_args) == 1
    ), "Multiple conflicting additional args specified"

    if len(additional_args) != len(unique_additional_args):
        return unique_additional_args

    return additional_args


def app_id_tiebreaker(*configs):
    """Check that the given demultiplex configs agree on the app id to
    use for demultiplexing.

    Parameters
    ----------
    *configs : dict
        Variable number of demultiplex config dicts, each of which may
        contain an "app_id" key

    Returns
    -------
    list
        Single element list containing the app id shared by the configs

    Raises
    ------
    AssertionError
        Raised if the configs specify different app ids
    """

    app_ids = [
        config.get("app_id") for config in configs if config.get("app_id")
    ]
    # deduplicate app ids so that configs specifying the same app id
    # don't conflict
    unique_app_ids = list({app_id for app_id in app_ids})
    # app ids are conflicting
    assert len(unique_app_ids) == 1, "Multiple conflicting app ids specified"

    if len(app_ids) != len(unique_app_ids):
        return unique_app_ids

    return app_ids


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
        "interop_summary.csv",  # InterOp QC metrics for multiQC
        "interop_index_summary.csv",
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
            if not re.match(r"project-[A-Za-z0-9]", run_id):
                projects = [project for project in dx.find_projects(run_id)]

                if len(projects) > 1 or len(projects) == 0:
                    raise Exception(
                        f"'{run_id}' found no or multiple projects in DNAnexus"
                    )
                else:
                    run_id = projects[0].get("id")

            demultiplex_output = f"{run_id}:/demultiplex_{time_stamp()}"

    demultiplex_project, demultiplex_folder = demultiplex_output.split(":")

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
        job_id = job.id.replace("job-", "")
        job_url = (
            f"https://platform.dnanexus.com/projects/"
            f"{demultiplex_project.replace('project-', '')}"
            "/monitor/job/"
            f"{job_id}"
        )

        Slack().send(
            f"Demultiplexing job failed!\n\nError: {err}\n\n"
            f"Demultiplexing job: {job_url}"
        )
        raise dx.exceptions.DXJobFailureError()

    prettier_print("Demuliplexing completed!")

    return job, demultiplex_output


def get_demultiplex_config_from_value(configs, values=None):
    if values is None:
        return configs

    configs_to_return = []

    for config in configs:
        for v in config.values():
            if v in values:
                configs_to_return.append(config)

    return configs_to_return
