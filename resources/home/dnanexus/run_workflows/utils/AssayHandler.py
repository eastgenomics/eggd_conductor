"""
Functions related to querying and managing objects in DNAnexus, as well
as running jobs.
"""

from collections import defaultdict
import os
import random
import re

import dxpy as dx

from utils.dx_utils import find_dx_project, get_job_output_details, dx_run
from utils import manage_dict
from utils.utils import (
    prettier_print,
)
from utils.WebClasses import Slack


class AssayHandler:
    def __init__(self, config):
        self.config = config
        self.assay_code = config.get("assay_code")
        self.assay = config.get("assay")
        self.version = config.get("version")
        self.samples = []
        self.job_info_per_sample = {}
        self.job_info_per_run = {}
        self.job_outputs = {}
        self.jobs = []
        self.missing_output_samples = []
        self.job_summary = defaultdict(lambda: defaultdict(dict))

    def limit_samples(self, limit_nb=None):
        """Limit samples using a number or specific names

        Args:
            limit_nb (int, optional): Limit number for samples.
            Defaults to None.
            patterns_to_exclude (list, optional): List of samples to exclude.
            Defaults to [].
        """

        original_sample_list = self.samples

        if limit_nb:
            # use randomness to choose the samples in order to not be limited
            # to a single config in testing
            self.samples = random.sample(self.samples, limit_nb)

        excluded_samples = list(
            set(original_sample_list).difference(self.samples)
        )

        if sorted(original_sample_list) == sorted(self.samples):
            prettier_print("No samples were removed")
        else:
            prettier_print(
                (
                    f"Limiting samples to {limit_nb}, the following samples "
                    f"were excluded: {excluded_samples}"
                )
            )

    def subset_samples(self):
        """Subset samples using the config information

        Raises:
            re.error: Invalid regex pattern provided
        """

        subset = self.config.get("subset_samplesheet", None)

        if subset:
            # check that a valid pattern has been provided
            try:
                re.compile(subset)
            except re.error:
                raise re.error("Invalid subset pattern provided")

            self.samples = [
                sample for sample in self.samples if re.search(subset, sample)
            ]

            assert (
                self.samples
            ), f"No samples left after filtering using pattern {subset}"

        else:
            prettier_print(
                f"No subset samplesheet found for {self.config['assay']} - "
                f"v{self.config['version']}"
            )

    def get_or_create_dx_project(self, project_name, run_id) -> str:
        """
        Create new project in DNAnexus if one with given name doesn't
        already exist.

        Returns
        -------
        str : ID of DNAnexus project
        """

        assay = self.config.get("assay")
        version = self.config.get("version")

        project_id = find_dx_project(project_name)

        if not project_id:
            # create new project and capture returned project id and store
            project_id = dx.bindings.dxproject.DXProject().new(
                name=project_name,
                summary=(
                    f"Analysis of run {run_id} with "
                    f"{assay} {version} config"
                ),
                description=(
                    "This project was automatically created by "
                    f"eggd_conductor from {os.environ.get('PARENT_JOB_ID')}"
                ),
            )
            prettier_print(
                f"\nCreated new project for output: {project_name} "
                f"({project_id})"
            )
        else:
            prettier_print(
                f"\nUsing existing found project: {project_name} "
                f"({project_id})"
            )

        # link project id to config and samples
        self.project = dx.bindings.dxproject.DXProject(dxid=project_id)

    def create_analysis_project_logs(self):
        """Create an analysis project log with info per config file contained
        in the DXBuilder object"""

        with open("analysis_project.log", "a") as f:
            f.write(
                f"{self.project.id} "
                f"{self.config.get('assay_code')} "
                f"{self.config.get('version')} "
                f"{len(self.jobs)}\n"
            )

    def get_upload_tars(self, sentinel_file) -> list:
        """
        Get list of upload tar file IDs from given sentinel file,
        and return formatted as a list of $dnanexus_link dicts

        Returns
        -------
        list
            list of file ids formated as {"$dnanexus_link": file-xxx}
        """

        if not sentinel_file:
            # sentinel file not provided as input -> no tars to parse
            self.upload_tars = None
        else:
            sentinel_file_obj = dx.bindings.dxrecord.DXRecord(
                dxid=sentinel_file
            )
            details = sentinel_file_obj.describe(incl_details=True)

            upload_tars = details["details"]["tar_file_ids"]

            prettier_print(
                f"\nFollowing upload tars found to add as input: {upload_tars}"
            )

            # format in required format for a dx input
            self.upload_tars = [{"$dnanexus_link": x} for x in upload_tars]

    def set_parent_out_dir(self, run_time):
        """Set the parent output directory for each config/assay/project

        Args:
            run_time (str): String to represent execution time in YYMMDD_HHMM
            format
        """

        parent_out_dir = (
            f"{os.environ.get('DESTINATION', '')}/output/"
            f"{self.assay}-{run_time}"
        )
        self.parent_out_dir = parent_out_dir.replace("//", "/")

    def get_executable_names_per_config(self) -> dict:
        """
        Build a dict of all executable IDs parsed from config to human
        readable names, used for naming outputs needing workflow/app names

        Sets
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

        executables = self.config.get("executables").keys()
        prettier_print(f"\nGetting names for all executables: {executables}")
        execution_mapping = defaultdict(dict)

        # sense check everything is a valid dx executable
        assert all(
            [
                x.startswith("workflow-")
                or x.startswith("app-")
                or x.startswith("applet-")
                for x in executables
            ]
        ), Slack().send(
            f"Executable(s) from the config not valid: {executables}"
        )

        for exe in executables:
            if exe.startswith("workflow-"):
                workflow_details = dx.api.workflow_describe(exe)
                workflow_name = workflow_details.get("name")
                workflow_name.replace("/", "-")
                execution_mapping[exe]["name"] = workflow_name
                execution_mapping[exe]["stages"] = defaultdict(dict)

                for stage in workflow_details.get("stages"):
                    stage_id = stage.get("id")
                    stage_name = stage.get("executable")

                    if stage_name.startswith("applet-"):
                        # need an extra describe for applets
                        stage_name = dx.api.workflow_describe(stage_name).get(
                            "name"
                        )

                    if stage_name.startswith("app-"):
                        # apps are prefixed with app- which is ugly
                        stage_name = stage_name.replace("app-", "")

                    # app names will be in format app-id/version
                    stage_name = stage_name.replace("/", "-")
                    execution_mapping[exe]["stages"][stage_id] = stage_name

            elif exe.startswith("app-") or exe.startswith("applet-"):
                app_details = dx.api.workflow_describe(exe)
                app_name = app_details["name"].replace("/", "-")

                if app_name.startswith("app-"):
                    app_name = app_name.replace("app-", "")
                execution_mapping[exe] = {"name": app_name}

        self.execution_mapping = execution_mapping

    def get_input_classes_per_config(self) -> dict:
        """
        Get classes of all inputs for each app / workflow stage, used
        when building out input dict to ensure correct type set

        Sets
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

        executables = self.config.get("executables").keys()
        input_class_mapping = defaultdict(dict)

        for exe in executables:
            describe = dx.describe(exe)

            for input_spec in describe["inputSpec"]:
                input_class_mapping[exe][input_spec["name"]] = defaultdict(
                    dict
                )
                input_spec_name = input_class_mapping[exe][input_spec["name"]]
                input_spec_name["class"] = input_spec["class"]
                input_spec_name["optional"] = input_spec.get("optional", False)

        self.input_class_mapping = input_class_mapping

    def build_job_inputs(
        self, executable: str, params: dict, sample: str = None
    ):
        """Build the job inputs dictionary by adding necessary information or
        replacing placeholders in the description of the inputs

        Parameters
        ----------
        executable : str
            Executable name
        params : dict
            Dict containing the parameters set in the config for the executable
        sample : str, optional
            Sample name, by default None
        """

        # select input and output dict from config for current workflow / app
        input_dict = self.config["executables"][executable]["inputs"]

        sample_prefix = sample
        executable_name = self.execution_mapping[executable]["name"]

        job_name = f"{executable_name}"

        if sample:
            per_sample = True
            self.job_info_per_sample.setdefault(sample, {})
            self.job_info_per_sample[sample].setdefault(executable, {})
            job_info = self.job_info_per_sample[sample][executable]

            if params.get("sample_name_delimeter"):
                # if delimeter specified to split sample name on, use it
                delim = params.get("sample_name_delimeter")

                if delim in sample:
                    sample_prefix = sample.split(delim)[0]
                else:
                    prettier_print(
                        (
                            f"Specified delimeter ({delim}) is not in sample name "
                            f"({sample}), ignoring and continuing..."
                        )
                    )

            # set job name as executable name and sample name
            job_name += f"-{sample}"

        else:
            per_sample = False
            self.job_info_per_run.setdefault(executable, {})
            job_info = self.job_info_per_run[executable]

        job_info["job_name"] = job_name

        if executable_name.startswith("TSO500_reports_workflow") and sample:
            input_dict, missing_output_sample = self.handle_TSO500_inputs(
                input_dict, sample, self.job_outputs
            )

            if missing_output_sample:
                # send message
                self.missing_output_samples.append(missing_output_sample)

        # check if stage requires fastqs passing
        if params["process_fastqs"] is True:
            input_dict = manage_dict.add_fastqs(
                input_dict=input_dict,
                fastq_details=self.fastq_details,
                sample=sample,
            )

        # find all jobs for previous analyses if next job depends on them
        if params.get("depends_on"):
            dependent_jobs = manage_dict.get_dependent_jobs(
                params=params, job_outputs_dict=self.job_outputs, sample=sample
            )
        else:
            dependent_jobs = []

        job_info["dependent_jobs"] = dependent_jobs

        # handle other inputs defined in config to add to inputs
        # sample_prefix passed to pass to INPUT-SAMPLE_NAME
        input_dict = manage_dict.add_other_inputs(
            input_dict=input_dict,
            parent_out_dir=self.parent_out_dir,
            project_id=self.project.id,
            project_name=self.project.name,
            sample=sample,
            sample_prefix=sample_prefix,
        )

        job_info["extra_args"] = params.get("extra_args", {})

        # add upload tars as input if INPUT-UPLOAD_TARS present
        if self.upload_tars:
            input_dict = manage_dict.add_upload_tars(
                input_dict=input_dict, upload_tars=self.upload_tars
            )

        # check any inputs dependent on previous job outputs to add
        input_dict = manage_dict.link_inputs_to_outputs(
            job_outputs_dict=self.job_outputs,
            input_dict=input_dict,
            analysis=params["analysis"],
            per_sample=per_sample,
            sample=sample,
        )

        # check input types correctly set in input dict
        input_dict = manage_dict.fix_invalid_inputs(
            input_dict=input_dict,
            input_classes=self.input_class_mapping[executable],
        )

        # check that all INPUT- have been parsed in config
        manage_dict.check_all_inputs(input_dict)

        job_info["inputs"] = input_dict

    def handle_TSO500_inputs(
        self, input_dict: dict, sample: str, job_outputs_config: dict
    ):
        """Small wrapper for the populate_tso500_reports_workflow function with
        gathering of tso500 job files/ids

        Parameters
        ----------
        input_dict : dict
            Dict containing the inputs needed for the future job to run
        sample : str
            Sample name
        job_outputs_config : dict
            Dict containing the jobs that have been launched

        Returns
        -------
        dict
            Modified input_dict
        """

        # handle specific inputs of eggd_TSO500 -> TSO500 workflow

        # get the job ID for previous eggd_tso500 job, this _should_ just
        # be analysis_1, but check anyway incase other apps added in future
        # per sample jobs would be stored in prev_jobs dict under sample
        # key, so we can just check for analysis_ for prior apps run once
        # per run
        jobs = [
            job_outputs_config[x]
            for x in job_outputs_config
            if x.startswith("analysis_")
        ]
        jobs = {dx.describe(job_id).get("name"): job_id for job_id in jobs}
        tso500_id = [v for k, v in jobs.items() if k.startswith("eggd_tso500")]

        assert len(tso500_id) == 1, (
            "Could not correctly find prior eggd_tso500 "
            f"job, jobs found: {jobs}"
        )

        tso500_id = tso500_id[0]

        # get details of the job to pull files from
        all_output_files, job_output_ids = get_job_output_details(tso500_id)

        # try add all eggd_tso500 app outputs to reports workflow input
        return manage_dict.populate_tso500_reports_workflow(
            input_dict=input_dict,
            sample=sample,
            all_output_files=all_output_files,
            job_output_ids=job_output_ids,
        )

    def populate_output_dir_config(self, executable, sample=None):
        """
        Loops over stages in dict for output directory naming and adds
        worlflow app name.

        i.e. will be named /output/{out_folder}/{stage_name}/, where stage
        name is the human readable name of each stage defined in the config

        Parameters
        ----------
        executable : str
            human readable name of executable (workflow-, app-, applet-)
        exe_names : dict
            mapping of executable IDs to human readable names
        output_dict : dict
            dictionary of output paths for each executable
        out_folder : str
            name of parent dir path

        Returns
        -------
        output_dict : dict
            populated dict of output directory paths
        """
        prettier_print(f"\nPopulating output dict for {executable}")

        output_dict = self.config["executables"][executable]["output_dirs"]

        for stage, dir_path in output_dict.items():
            if "OUT-FOLDER" in dir_path:
                # OUT-FOLDER => /output/{ASSAY}_{TIMESTAMP}
                dir_path = dir_path.replace("OUT-FOLDER", self.parent_out_dir)
            if "APP-NAME" in dir_path or "WORKFLOW-NAME" in dir_path:
                app_name = self.execution_mapping[executable]["name"]
                dir_path = dir_path.replace("APP-NAME", app_name)
            if "STAGE-NAME" in dir_path:
                app_name = self.execution_mapping[executable]["stages"][stage]
                dir_path = dir_path.replace("STAGE-NAME", app_name)

            # ensure we haven't accidentally got double slashes in path
            dir_path = dir_path.replace("//", "/")

            # ensure we don't end up with double /output if given in config and
            # using OUT-FOLDER
            dir_path = dir_path.replace("output/output", "output")

            output_dict[stage] = dir_path

        prettier_print(f"\nOutput dict for {executable}:")
        prettier_print(output_dict)

        if sample:
            self.job_info_per_sample[sample][executable][
                "output_dirs"
            ] = output_dict
        else:
            self.job_info_per_run[executable]["output_dirs"] = output_dict

    def call_job(self, executable, analysis, instance_type, sample=None):
        """Call job per sample using the sample and its job information

        Parameters
        ----------
        sample : str
            Sample name
        executable : str
            Executable name
        analysis : str
            Analysis name to associate the job id to
        instance_type : str
            Name of the instance to use for the executable

        Returns
        -------
        int
            Int to indication that the job started
        """

        # get the job information given the sample name and the executable
        if sample:
            job_info = self.job_info_per_sample[sample][executable]
        else:
            job_info = self.job_info_per_run[executable]

        job_id = dx_run(
            executable=executable,
            job_name=job_info["job_name"],
            input_dict=job_info["inputs"],
            output_dict=job_info["output_dirs"],
            prev_jobs=job_info["dependent_jobs"],
            extra_args=job_info["extra_args"],
            instance_types=instance_type,
            project_id=self.project.id,
        )

        self.jobs.append(job_id)

        if sample:
            self.job_summary[executable][sample] = job_id
            self.job_outputs.setdefault(sample, {})
            # map analysis id to dx job id for sample
            self.job_outputs[sample].update({analysis: job_id})
        else:
            self.job_summary[executable] = job_id
            # map workflow id to created dx job id
            self.job_outputs[analysis] = job_id

        return 1
