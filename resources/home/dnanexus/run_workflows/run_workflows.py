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
from itertools import zip_longest
import json
import math
import os
import re
import subprocess

import dxpy as dx

from utils.AssayHandler import AssayHandler
from utils.dx_utils import (
    get_json_configs,
    filter_highest_config_version,
    wait_on_done,
    terminate_jobs,
    invite_participants_in_project,
)
from utils import manage_dict
from utils.WebClasses import Jira, Slack
from utils.utils import (
    load_config,
    load_test_data,
    match_samples_to_assays,
    parse_run_info_xml,
    parse_sample_sheet,
    prettier_print,
    time_stamp,
    select_instance_types,
    create_project_name,
)
from utils.demultiplexing import (
    demultiplex,
    get_demultiplex_job_details,
    move_demultiplex_qc_files,
    set_config_for_demultiplexing,
)


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
        "--assay_config",
        nargs="+",
        help="Assay specific config file that defines all executables to run",
    )
    parser.add_argument(
        "--sentinel_file", help="Sentinel file uploaded by dx-streaming-upload"
    )
    parser.add_argument(
        "--samplesheet", help="Samplesheet to parse sample IDs from"
    )
    parser.add_argument(
        "--samples",
        help="Command seperated string of sample names to run analysis on",
    )
    parser.add_argument(
        "--run_info_xml", help="RunInfo.xml file, used to parse run ID from"
    )
    parser.add_argument(
        "--dx_project_id",
        required=False,
        help=(
            "DNAnexus project ID to use to run analysis in, "
            "if not specified will create one based off run ID and assay name"
        ),
    )
    parser.add_argument(
        "--run_id", help="ID of run, used to name output project."
    )
    parser.add_argument(
        "--assay_name", help="Assay name, used for naming outputs"
    )
    parser.add_argument(
        "--development",
        "-d",
        action="store_true",
        help="Created project will be prefixed with 003 instead of 002.",
    )
    parser.add_argument(
        "--testing",
        action="store_true",
        help=(
            "Controls if to terminate and clean up jobs after launching "
            "for testing purposes"
        ),
    )
    parser.add_argument(
        "--testing_sample_limit",
        type=int,
        help=(
            "For use when testing only - no."
            "Samples to limit running analyses for"
        ),
    )
    parser.add_argument(
        "--demultiplex_job_id",
        help="ID of job from running demultiplexing (if run)",
    )
    parser.add_argument(
        "--demultiplex_output",
        help=(
            "DX path to store output from demultiplexing, defaults to parent "
            "of sentinel file if not specified"
        ),
    )
    parser.add_argument(
        "--fastqs",
        help="Comma separated string of fastq file ids for starting analysis",
    )
    parser.add_argument(
        "--test_samples",
        help=(
            "For test use only. Pass in file with 1 sample per line "
            "specifing file-id of fastq and sample name"
        ),
    )
    parser.add_argument(
        "--mismatch_allowance",
        type=int,
        help=(
            "# of samples allowed to not match to any assay code and use "
            "the assay code of other samples (default: 1, set in dxapp.json)"
        ),
    )
    parser.add_argument(
        "--job_reuse",
        help=(
            "JSON formatted string mapping analysis step -> job ID to reuse "
            "outputs from instead of running analysis (i.e. "
            '\'{"analysis_1": "job-xxx"}\')'
        ),
    )
    parser.add_argument(
        "--exclude_samples",
        help=(
            "Comma separated string of sample names to exclude from "
            "per sample analysis steps"
        ),
    )

    args = parser.parse_args()

    # turn comma separated str to python list
    if args.samples:
        args.samples = [
            x.replace(" ", "") for x in args.samples.split(",") if x
        ]
        prettier_print(
            f"\nsamples specified to run jobs for: \n\t{args.samples}\n"
        )
    if args.fastqs:
        args.fastqs = [x.replace(" ", "") for x in args.fastqs.split(",") if x]

    if args.run_info_xml:
        args.run_id = parse_run_info_xml(args.run_info_xml)

    if not args.samples:
        args.samples = parse_sample_sheet(args.samplesheet)

    if args.job_reuse:
        # check given JOB_REUSE is valid JSON
        try:
            args.job_reuse = json.loads(args.job_reuse)
        except json.decoder.JSONDecodeError:
            raise SyntaxError(
                Slack().send(
                    "`-iJOB_REUSE` provided does not appear to be valid "
                    f"JSON format: `{args.job_reuse}`"
                )
            )
    else:
        args.job_reuse = {}

    if args.exclude_samples:
        args.exclude_samples = [
            x.replace(" ", "") for x in args.exclude_samples.split(",") if x
        ]

    return args


def main():
    """
    Main entry point to run all apps and workflows
    """

    args = parse_args()

    if args.assay_config:
        configs = [load_config(config) for config in args.assay_config]
        configs = {config.get("assay_code"): config for config in configs}

    else:
        # get all json assay configs from path in conductor config
        configs = get_json_configs()
        configs = filter_highest_config_version(configs)

    assay_handlers = []

    for config_content in configs.values():
        assay_handler = AssayHandler(config_content)
        assay_handlers.append(assay_handler)

    assay_codes = [
        assay_handler.assay_code for assay_handler in assay_handlers
    ]

    # add the file ID of assay config file used as job output, this
    # is to make it easier to audit what configs were used for analysis
    subprocess.run(
        "dx-jobutil-add-output assay_config_file_ids "
        f"{'|'.join(assay_codes)} --class=string",
        shell=True,
        check=False,
    )

    assay_to_samples = match_samples_to_assays(
        configs=configs,
        all_samples=args.samples,
        testing=args.testing,
    )

    if args.dx_project_id:
        project = dx.DXProject(args.dx_project_id)
        run_id = project.name

    else:
        # output project not specified, create new one from run id
        run_id = args.run_id

    limiting_nb_per_assay = []

    # in order to avoid using randomness for limiting the sample number per
    # assay, determine how many samples need to be kept per assay
    if args.testing_sample_limit:
        limiting_nb = args.testing_sample_limit / len(assay_handlers)

        # first assay will have one more sample than the rest to handle cases
        # where the division returns a decimal component
        limiting_nb_per_assay.append(math.ceil(limiting_nb))
        # for the rest get the floor of the rest of the assays
        limiting_nb_per_assay.extend(
            [math.floor(limiting_nb) for i in range(len(assay_handlers) - 1)]
        )

    jira = Jira(
        os.environ.get("JIRA_QUEUE_URL"),
        os.environ.get("JIRA_ISSUE_URL"),
        os.environ.get("JIRA_TOKEN"),
        os.environ.get("JIRA_EMAIL"),
    )

    all_tickets = jira.get_all_tickets()
    ticket_errors = []

    filtered_tickets = jira.filter_tickets_using_run_id(run_id, all_tickets)

    if filtered_tickets == []:
        prettier_print(f"No ticket found for {run_id}")
    else:
        assay_info = [
            f"{handler.assay} - {handler.version}"
            for handler in assay_handlers
        ]
        if len(filtered_tickets) > len(assay_handlers):
            ticket_errors.append(
                "Too many tickets found for the number of configs detected "
                f"to be used for {run_id}: {filtered_tickets} - {assay_info}"
            )

        elif len(filtered_tickets) < len(assay_handlers):
            ticket_errors.append(
                "Not enough tickets found for the number of configs detected "
                f"to be used for {run_id}: {filtered_tickets} - {assay_info}"
            )

    run_time = time_stamp()

    for assay_handler, limiting_nb in zip_longest(
        assay_handlers, limiting_nb_per_assay
    ):
        for assay_code, samples in assay_to_samples.items():
            if assay_code == assay_handler.assay_code:
                assay_handler.samples.extend(samples)

        if limiting_nb:
            assay_handler.limit_samples(limit_nb=limiting_nb)

        if args.exclude_samples:
            prettier_print(
                "Attempting to exclude following samples from "
                f"{assay_handler.assay}: {args.exclude_samples}"
            )
            assay_handler.limit_samples(
                samples_to_exclude=args.exclude_samples
            )

        assay_handler.subset_samples()

        # A project id was passed, no need to try and get/create one.
        # This also means that for mixed assay runs, only one project will be
        # used for launching the jobs
        if project:
            assay_handler.project = project

        else:
            # create dnanexus project name
            project_name = create_project_name(
                run_id, assay_handler.assay, args.development, args.testing
            )
            assay_handler.get_or_create_dx_project(project_name, run_id)
            users = assay_handler.config.get("users")
            invite_participants_in_project(users, assay_handler.project)

        assay_handler.create_analysis_project_logs()

        # set parent output directory, each app will have sub dir in here
        assay_handler.set_parent_out_dir(run_time)

        # get upload tars from sentinel file
        assay_handler.get_upload_tars(args.sentinel_file)

        # sense check per_sample defined for all workflows / apps in config
        # before starting as we want this explicitly defined for everything to
        # ensure it is launched correctly
        for executable, params in assay_handler.config["executables"].items():
            assert "per_sample" in params.keys(), Slack().send(
                f"per_sample key missing from {executable} in config, check "
                "config and re-run"
            )

        assay_handler.ticket = None

        # go through the tickets to try and assign one to the assay handler
        for ticket in filtered_tickets:
            # get the assay code in the ticket
            assay_options = [
                subfield["value"]
                for field, subfields in ticket["fields"].items()
                if field == "customfield_10070"
                for subfield in subfields
            ]

            # tickets should only have one assay code
            if len(assay_options) == 1:
                if assay_options[0] in assay_handler.assay:
                    prettier_print(
                        f"Assigned {ticket['key']} to {assay_handler.assay}"
                    )
                    assay_handler.ticket = ticket["id"]

                    # add comment to Jira ticket for run to link to
                    # this eggd_conductor job
                    jira.add_comment(
                        comment=(
                            "This run was processed automatically by "
                            "eggd_conductor: "
                        ),
                        url=f"http://{os.environ.get('conductor_job_url')}",
                        ticket=ticket["id"],
                    )

            elif len(assay_options) == 0:
                ticket_errors.append(f"Ticket {ticket['key']} has no assays")

            else:
                ticket_errors.append(
                    f"Ticket {ticket['key']} has multiple assays: "
                    f"{assay_options}"
                )

        # check if a ticket has been assigned to the assay handler
        if assay_handler.ticket is None:
            ticket_errors.append(
                f"{run_id} - {assay_handler.assay} couldn't be assigned a "
                "ticket"
            )

        if ticket_errors:
            for error in ticket_errors:
                Slack().send(error)

    if args.demultiplex_job_id:
        # previous demultiplexing job specified to use fastqs from
        fastq_details = get_demultiplex_job_details(args.demultiplex_job_id)

    elif args.fastqs:
        fastq_details = []

        # fastqs specified to start analysis from, call describe on
        # files to get name and build list of tuples of (file id, name)
        for fastq_id in args.fastqs:
            fastq_name = dx.api.file_describe(
                fastq_id, input_params={"fields": {"name": True}}
            )
            fastq_name = fastq_name["name"]
            fastq_details.append((fastq_id, fastq_name))

    elif args.test_samples:
        # test files of fastq names : file ids given
        fastq_details = load_test_data(args.test_samples)

    elif any(
        [
            assay_handler.config.get("demultiplex")
            for assay_handler in assay_handlers
        ]
    ):
        demultiplex_config = set_config_for_demultiplexing(
            assay_handler.config for assay_handler in assay_handlers
        )

        demultiplex_app_id = None
        demultiplex_app_name = None

        if demultiplex_config:
            demultiplex_app_id = demultiplex_config.get("app_id", "")
            demultiplex_app_name = demultiplex_config.get("app_name", "")

        if not demultiplex_app_id and not demultiplex_app_name:
            # ID for demultiplex app not in assay config, use default from
            # app config
            demultiplex_app_id = os.environ.get("DEMULTIPLEX_APP_ID")

        demultiplex_job = demultiplex(
            app_id=demultiplex_app_id,
            app_name=demultiplex_app_name,
            testing=args.testing,
            demultiplex_config=demultiplex_config,
            demultiplex_output=args.demultiplex_output,
            sentinel_file=args.sentinel_file,
            run_id=run_id,
        )

        for assay_handler in assay_handlers:
            move_demultiplex_qc_files(
                assay_handler.project.id, *args.demultiplex_output.split(":")
            )

        fastq_details = get_demultiplex_job_details(demultiplex_job.id)

    elif any(
        [
            manage_dict.search(
                identifier="INPUT-UPLOAD_TARS",
                input_dict=assay_handler.config,
                check_key=False,
                return_key=False,
            )
            for assay_handler in assay_handlers
        ]
    ):
        # an app / workflow takes upload tars as an input => valid start point
        pass
    else:
        # not demultiplexing or given fastqs, exit as we aren't handling
        # this for now
        raise RuntimeError(
            Slack().send(
                "No fastqs passed or demultiplexing specified. Exiting now"
            )
        )

    for assay_handler in assay_handlers:
        assay_handler.fastq_details = fastq_details
        # build a dict mapping executable names to human readable names
        assay_handler.get_executable_names_per_config()

        # build mapping of executables input fields => required types (i.e.
        # file, array:file, boolean), used to correctly build input dict
        assay_handler.get_input_classes_per_config()

    prettier_print("\nExecutable names identified:")
    prettier_print(
        [
            list(assay_handler.execution_mapping)
            for assay_handler in assay_handlers
        ]
    )

    prettier_print("\nExecutable input classes found:")
    prettier_print(
        [
            list(assay_handler.input_class_mapping)
            for assay_handler in assay_handlers
        ]
    )

    # log file of all jobs, used to set as app output for picking up
    # by separate monitoring script
    open("all_job_ids.log", "w").close()

    total_jobs = 0

    for handler in assay_handlers:
        prettier_print(f"Samples for {handler.assay_code}: {handler.samples}")
        project_id = handler.project.id

        # set context to project for running jobs
        dx.set_workspace_id(project_id)

        for executable, params in handler.config["executables"].items():
            # for each workflow/app, check if its per sample or all samples and
            # run correspondingly
            prettier_print(
                f'\n\nConfiguring {params.get("name")} ({executable}) to '
                "start jobs"
            )

            # first check if specified to reuse a previous job for this step
            if args.job_reuse.get(params["analysis"]):
                previous_job = args.job_reuse.get(params["analysis"])

                assert re.match(r"(job|analysis)-[\w]+", previous_job), (
                    "Job specified to reuse does not appear valid: "
                    f"{previous_job}"
                )

                if params["per_sample"]:
                    # ensure we're only doing this for per run jobs for now
                    raise NotImplementedError(
                        "-iJOB_REUSE not yet implemented for per sample jobs"
                    )

                prettier_print(
                    f"Reusing provided job {previous_job} for analysis step "
                    f"{params['analysis']} for {params['name']}"
                )

                # dict to add all stage output names and job ids for every
                # sample to used to pass correct job ids to subsequent
                # workflow / app calls
                handler.job_outputs[params["analysis"]] = previous_job

                continue

            prettier_print("\nParams parsed from config before modifying:")
            prettier_print(params)

            # log file of all jobs run for current executable, used in case of
            # failing to launch all jobs to be able to terminate all analyses
            open("job_id.log", "w").close()

            executable_name = handler.execution_mapping[executable]["name"]

            # get instance types to use for executable from config for flowcell
            instance_type = select_instance_types(
                run_id=run_id, instance_types=params.get("instance_types")
            )

            if params["per_sample"] is True:
                prettier_print(f"\nCalling {executable_name} per sample")

                for sample in handler.samples:
                    handler.build_job_inputs(executable, params, sample)
                    # create new dict to store sample outputs
                    handler.job_outputs.setdefault(handler.assay_code, {})
                    handler.job_outputs[handler.assay_code].setdefault(
                        sample, {}
                    )
                    handler.populate_output_dir_config(executable, sample)

                for sample in handler.job_info_per_sample:
                    total_jobs += handler.call_job(
                        executable, params["analysis"], instance_type, sample
                    )

            elif params["per_sample"] is False:
                prettier_print(f"\nCalling {executable_name} per run")

                handler.build_job_inputs(executable, params)
                handler.populate_output_dir_config(executable)

                total_jobs += handler.call_job(
                    executable, params["analysis"], instance_type
                )

            else:
                # per_sample is not True or False, exit
                raise ValueError(
                    f"per_sample declaration for {executable} is not True or "
                    f"False ({params['per_sample']}). \n\nPlease check the "
                    "config."
                )

            prettier_print(
                f'\n\nAll jobs for {params.get("name")} ({executable}) '
                f"launched successfully!\n\n"
            )

            if params.get("hold"):
                # specified to hold => wait for all jobs to complete

                # tag conductor whilst waiting to make it clear its being held
                conductor_job = dx.DXJob(os.environ.get("PARENT_JOB_ID"))
                hold_tag = [
                    (f"Holding job until {executable_name} " "job(s) complete")
                ]
                conductor_job.add_tags(hold_tag)

                wait_on_done(
                    analysis=params["analysis"],
                    analysis_name=executable_name,
                    all_job_ids=handler.job_outputs,
                )

                conductor_job.remove_tags(hold_tag)

        # add comment to Jira ticket for run to link to analysis project
        jira.add_comment(
            comment=(
                "All jobs successfully launched by eggd_conductor. "
                "\nAnalysis project(s): "
            ),
            url=(
                "http://platform.dnanexus.com/panx/projects/"
                f"{handler.project.name.replace('project-', '')}/monitor/"
            ),
            ticket=handler.ticket,
        )

    if args.testing:
        terminate_jobs(
            [
                job
                for assay_handler in assay_handlers
                for job in assay_handler.jobs
            ]
        )

    with open("total_jobs.log", "w") as fh:
        fh.write(str(total_jobs))

    prettier_print("\nCompleted calling jobs")


if __name__ == "__main__":
    main()
