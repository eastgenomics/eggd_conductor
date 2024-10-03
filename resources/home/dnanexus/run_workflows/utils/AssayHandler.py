"""
Functions related to querying and managing objects in DNAnexus, as well
as running jobs.
"""

from collections import defaultdict
from datetime import datetime
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


class AssayHandler():
    def __init__(self, config):
        self.config = config
        self.assay_code = config.get("assay_code")
        self.assay = config.get("assay")
        self.assay_version = config.get("assay_version")
        self.samples = []
        self.job_info_per_sample = {}
        self.job_info_per_run = {}
        self.job_outputs = {}
        self.jobs = []

    def limit_samples(self, limit_nb=None, samples_to_exclude=[]):
        """ Limit samples using a number or specific names

        Args:
            limit_nb (int, optional): Limit number for samples.
            Defaults to None.
            samples_to_exclude (list, optional): List of samples to exclude.
            Defaults to [].
        """

        if limit_nb:
            # use randomness to choose the samples in order to not be limited
            # to a single config in testing
            self.samples = random.sample(self.samples, limit_nb)

        sample_list = self.samples

        self.samples = [
            sample
            for sample in self.samples
            if sample not in samples_to_exclude
        ]

        if sample_list == self.samples:
            prettier_print(f"No samples were removed: {samples_to_exclude}")
        else:
            prettier_print(
                f"The following samples were removed: {samples_to_exclude}"
            )

    def subset_samples(self):
        """ Subset samples using the config information

        Raises:
            re.error: Invalid regex pattern provided
        """

        subset = self.config.get("subset_samplesheet", None)

        if subset:
            # check that a valid pattern has been provided
            try:
                re.compile(subset)
            except re.error:
                raise re.error('Invalid subset pattern provided')

            self.samples = [
                sample for sample in self.samples if re.search(subset, sample)
            ]

            assert self.samples, (
                f"No samples left after filtering using pattern {subset}"
            )

        else:
            prettier_print(
                f"No subset samplesheet found for {self.config['assay']} - "
                f"v{self.config['version']}"
            )

    def get_or_create_dx_project(self, run_id, development, testing) -> str:
        """
        Create new project in DNAnexus if one with given name doesn't
        already exist.

        Returns
        -------
        str : ID of DNAnexus project
        """

        if development:
            prefix = f'003_{datetime.now().strftime("%y%m%d")}_run-'
        else:
            prefix = '002_'

        suffix = ''

        if testing:
            suffix = '-EGGD_CONDUCTOR_TESTING'

        assay = self.config.get("assay")
        version = self.config.get("version")
        output_project = f'{prefix}{run_id}_{assay}{suffix}'

        project_id = find_dx_project(output_project)

        if not project_id:
            # create new project and capture returned project id and store
            project_id = dx.bindings.dxproject.DXProject().new(
                name=output_project,
                summary=(
                    f'Analysis of run {run_id} with '
                    f'{assay} {version} config'
                ),
                description=(
                    "This project was automatically created by "
                    f"eggd_conductor from {os.environ.get('PARENT_JOB_ID')}"
                )
            )
            prettier_print(
                f"\nCreated new project for output: {output_project} "
                f"({project_id})"
            )
        else:
            prettier_print(
                f"\nUsing existing found project: {output_project} "
                f"({project_id})"
            )

        # link project id to config and samples
        self.project = dx.bindings.dxproject.DXProject(dxid=project_id)

        users = self.config.get('users')

        if users:
            # users specified in config to grant access to project
            for user, access_level in users.items():
                self.project.invite(user, access_level, send_email=False)
                prettier_print(
                    f"\nGranted {access_level} priviledge to {user}"
                )

    def create_analysis_project_logs(self):
        """ Create an analysis project log with info per config file contained
        in the DXBuilder object """

        with open("analysis_project.log", "w") as f:
            f.write(
                f"{self.project.id} "
                f"{self.config.get('assay_code')} "
                f"{self.config.get('version')}\n"
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

        details = dx.bindings.dxrecord.DXRecord(dxid=sentinel_file).describe(
            incl_details=True
        )

        upload_tars = details['details']['tar_file_ids']

        prettier_print(
            f"\nFollowing upload tars found to add as input: {upload_tars}"
        )

        # format in required format for a dx input
        self.upload_tars = [
            {"$dnanexus_link": x} for x in upload_tars
        ]

    def set_parent_out_dir(self, run_time):
        """ Set the parent output directory for each config/assay/project

        Args:
            run_time (str): String to represent execution time in YYMMDD_HHMM
            format
        """

        parent_out_dir = (
            f"{os.environ.get('DESTINATION', '')}/output/"
            f"{self.assay}-{run_time}"
        )
        self.parent_out_dir = parent_out_dir.replace('//', '/')

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
        prettier_print(
            f'\nGetting names for all executables: {executables}'
        )
        execution_mapping = defaultdict(dict)

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
                execution_mapping[exe]['name'] = workflow_name
                execution_mapping[exe]['stages'] = defaultdict(dict)

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
                    execution_mapping[exe]['stages'][stage_id] = stage_name

            elif exe.startswith('app-') or exe.startswith('applet-'):
                app_details = dx.api.workflow_describe(exe)
                app_name = app_details['name'].replace('/', '-')

                if app_name.startswith('app-'):
                    app_name = app_name.replace('app-', '')
                execution_mapping[exe] = {'name': app_name}

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

            for input_spec in describe['inputSpec']:
                input_class_mapping[exe][input_spec['name']] = defaultdict(
                    dict
                )
                input_spec_name = input_class_mapping[exe][input_spec['name']]
                input_spec_name['class'] = input_spec['class']
                input_spec_name['optional'] = input_spec.get('optional', False)

        self.input_class_mapping = input_class_mapping

    def build_job_info_per_sample(
        self, executable, sample, executable_out_dirs
    ):
        """ Build job information for jobs per sample for their inputs, outputs
        dependent jobs and job name.

        Args:
            executable (str): Name of the executable
            config (dict): Dict containing the information for the config
            params (dict): Dict containing the parameters expected that
            executable
            sample (str): Sample for which the job information is gathered for
            executable_out_dirs (dict): Dict containing the executable output
            directory
        """

        self.job_info_per_sample.setdefault(sample, {})
        self.job_info_per_sample[sample].setdefault(executable, {})

        job_outputs_config = self.job_outputs[self.assay_code]

        params = self.config['executables'][executable]
        # select input and output dict from config for current workflow / app
        input_dict = self.config['executables'][executable]['inputs']
        output_dict = self.config['executables'][executable]['output_dirs']

        self.job_info_per_sample[sample][executable]["extra_args"] = params.get(
            "extra_args", {}
        )

        if params['executable_name'].startswith('TSO500_reports_workflow'):
            # handle specific inputs of eggd_TSO500 -> TSO500 workflow

            # get the job ID for previous eggd_tso500 job, this _should_ just
            # be analysis_1, but check anyway incase other apps added in future
            # per sample jobs would be stored in prev_jobs dict under sample
            # key, so we can just check for analysis_ for prior apps run once
            # per run
            jobs = [
                job_outputs_config[x]
                for x in job_outputs_config
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
            all_output_files, job_output_ids = get_job_output_details(
                tso500_id
            )

            # try add all eggd_tso500 app outputs to reports workflow input
            input_dict = manage_dict.populate_tso500_reports_workflow(
                input_dict=input_dict,
                sample=sample,
                all_output_files=all_output_files,
                job_output_ids=job_output_ids
            )

        # check if stage requires fastqs passing
        if params["process_fastqs"] is True:
            input_dict = manage_dict.add_fastqs(
                input_dict=input_dict,
                fastq_details=self.fastq_details,
                sample=sample
            )

        # find all jobs for previous analyses if next job depends on them
        if params.get("depends_on"):
            dependent_jobs = manage_dict.get_dependent_jobs(
                params=params,
                job_outputs_dict=job_outputs_config,
                sample=sample
            )
        else:
            dependent_jobs = []

        self.job_info_per_sample[sample][executable]["dependent_jobs"] = dependent_jobs

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

        project_id = self.project.id
        project_name = self.project.name

        # handle other inputs defined in config to add to inputs
        # sample_prefix passed to pass to INPUT-SAMPLE_NAME
        input_dict = manage_dict.add_other_inputs(
            input_dict=input_dict,
            parent_out_dir=self.parent_out_dir,
            project_id=project_id,
            project_name=project_name,
            executable_out_dirs=executable_out_dirs,
            sample=sample,
            sample_prefix=sample_prefix
        )

        # check any inputs dependent on previous job outputs to add
        input_dict = manage_dict.link_inputs_to_outputs(
            job_outputs_dict=job_outputs_config,
            input_dict=input_dict,
            analysis=params["analysis"],
            per_sample=True,
            sample=sample
        )

        # check input types correctly set in input dict
        input_dict = manage_dict.check_input_classes(
            input_dict=input_dict,
            input_classes=self.input_class_mapping[executable]
        )

        # check that all INPUT- have been parsed in config
        manage_dict.check_all_inputs(input_dict)

        # set job name as executable name and sample name
        job_name = f"{params['executable_name']}-{sample}"

        self.job_info_per_sample[sample][executable]["job_name"] = job_name

        # create output directory structure in config
        manage_dict.populate_output_dir_config(
            executable=executable,
            exe_names=self.execution_mapping,
            output_dict=output_dict,
            out_folder=self.parent_out_dir
        )

        # call dx run to start jobs
        prettier_print(
            f"\nCalling {params['executable_name']} ({executable}) "
            f"on sample {sample}"
            )

        if input_dict.keys:
            prettier_print(f'\nInput dict: {input_dict}')

        self.job_info_per_sample[sample][executable]["inputs"] = input_dict
        self.job_info_per_sample[sample][executable]["outputs"] = output_dict

    def build_jobs_info_per_run(
        self,
        executable,
        executable_out_dirs
    ) -> dict:
        """ Build job information for jobs for the whole run for their inputs,
        outputs, dependent jobs and job name.

        Args:
            executable (str): Name of the executable
            config (dict): Dict containing the information for the config
            params (dict): Dict containing the parameters expected that
            executable
            executable_out_dirs (dict): Dict containing the executable output
            directory
        """

        self.job_info_per_run.setdefault(executable, {})

        params = self.config['executables'][executable]

        # select input and output dict from config for current workflow / app
        input_dict = self.config['executables'][executable]['inputs']
        output_dict = self.config['executables'][executable]['output_dirs']

        self.job_info_per_run[executable]["job_name"] = params.get(
            "executable_name"
        )
        self.job_info_per_run[executable]["extra_args"] = params.get(
            "extra_args", {}
        )

        job_outputs_config = self.job_outputs[self.assay_code]

        if params["process_fastqs"] is True:
            input_dict = manage_dict.add_fastqs(input_dict, self.fastq_details)

        # add upload tars as input if INPUT-UPLOAD_TARS present
        if self.upload_tars:
            input_dict = manage_dict.add_upload_tars(
                input_dict=input_dict,
                upload_tars=self.upload_tars
            )

        project_id = self.project.id
        project_name = self.project.name

        # handle other inputs defined in config to add to inputs
        input_dict = manage_dict.add_other_inputs(
            input_dict=input_dict,
            parent_out_dir=self.parent_out_dir,
            project_id=project_id,
            project_name=project_name,
            executable_out_dirs=executable_out_dirs
        )

        # get any filters from config to apply to job inputs
        input_filter_dict = params.get('inputs_filter')

        # check any inputs dependent on previous job outputs to add
        input_dict = manage_dict.link_inputs_to_outputs(
            job_outputs_dict=job_outputs_config,
            input_dict=input_dict,
            analysis=params["analysis"],
            input_filter_dict=input_filter_dict,
            per_sample=False
        )

        # check input types correctly set in input dict
        input_dict = manage_dict.check_input_classes(
            input_dict=input_dict,
            input_classes=self.input_class_mapping[executable]
        )

        # find all jobs for previous analyses if next job depends on them
        if params.get("depends_on"):
            dependent_jobs = manage_dict.get_dependent_jobs(
                params=params,
                job_outputs_dict=job_outputs_config
            )
        else:
            dependent_jobs = []

        self.job_info_per_run[executable]["dependent_jobs"] = dependent_jobs

        # check that all INPUT- have been parsed in config
        manage_dict.check_all_inputs(input_dict)

        # create output directory structure in config
        manage_dict.populate_output_dir_config(
            executable=executable,
            exe_names=self.execution_mapping,
            output_dict=output_dict,
            out_folder=self.parent_out_dir
        )

        self.job_info_per_run[executable]["inputs"] = input_dict
        self.job_info_per_run[executable]["outputs"] = output_dict

    def call_jobs_per_sample(
        self, executable, params, executable_out_dirs, instance_type
    ):
        # run workflow / app on every sample
        prettier_print(
            f'\nCalling {params["executable_name"]} per sample'
        )
        prettier_print(
            f"Samples for {self.assay_code}: "
            f"{self.samples}"
        )

        nb_jobs = 0

        # loop over samples and call app / workflow
        for idx, sample in enumerate(self.samples, 1):
            prettier_print(
                f'\n\nStarting analysis for {sample} - '
                f'[{idx}/{len(self.samples)}]'
            )

            # create new dict to store sample outputs
            self.job_outputs.setdefault(self.assay_code, {})
            self.job_outputs[self.assay_code].setdefault(sample, {})

            self.build_job_info_per_sample(
                executable=executable,
                sample=sample,
                executable_out_dirs=executable_out_dirs
            )

            job_info = self.job_info_per_sample[sample][executable]

            job_id = dx_run(
                executable=executable,
                job_name=job_info["job_name"],
                input_dict=job_info["inputs"],
                output_dict=job_info["outputs"],
                prev_jobs=job_info["dependent_jobs"],
                extra_args=job_info["extra_args"],
                instance_types=instance_type,
                project_id=self.project.id,
            )

            self.jobs.append(job_id)

            # map analysis id to dx job id for sample
            self.job_outputs[self.assay_code][sample].update(
                {params['analysis']: job_id}
            )

            nb_jobs += 1

        return nb_jobs

    def call_job_per_run(
        self, executable, params, executable_out_dirs, instance_type
    ):
        # run workflow / app on all samples at once
        self.build_jobs_info_per_run(
            executable=executable,
            executable_out_dirs=executable_out_dirs
        )

        run_job_info = self.job_info_per_run[executable]

        job_id = dx_run(
            executable=executable,
            job_name=run_job_info["job_name"],
            input_dict=run_job_info["inputs"],
            output_dict=run_job_info["outputs"],
            prev_jobs=run_job_info["dependent_jobs"],
            extra_args=run_job_info["extra_args"],
            instance_types=instance_type,
            project_id=self.project.id,
        )

        self.jobs.append(job_id)

        # map workflow id to created dx job id
        self.job_outputs[self.assay_code][params['analysis']] = job_id

        return 1
