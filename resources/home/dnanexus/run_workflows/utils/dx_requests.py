"""
Functions related to querying and managing objects in DNAnexus, as well
as running jobs.
"""

from collections import defaultdict
from copy import deepcopy
from datetime import datetime
import os
import random
import re

import dxpy as dx

from utils.dx_utils import find_dx_project, get_job_output_details
from utils import manage_dict
from utils.utils import (
    Slack,
    prettier_print,
    select_instance_types,
    time_stamp
)


class DXBuilder():
    def __init__(self):
        self.configs = []
        self.samples = []
        self.config_to_samples = None
        self.project_files = []
        self.total_jobs = 0
        self.fastqs_details = []
        self.job_inputs = {}
        self.job_outputs = {}

    def get_assays(self):
        return sorted(
            [config.get('assay_code') for config in self.configs.values()]
        )

    def add_sample_data(self, config_to_samples):
        """ Add sample data

        Args:
            config_to_samples (dict): Dict containing all samples linked to
            their appropriate config file
        """

        for config, samples in config_to_samples.items():
            self.configs.append(config)
            self.samples.extend(samples)
            self.config_to_samples.setdefault(config, {})
            self.config_to_samples["samples"] = samples

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

        tmp_data = {}

        # limit samples in the config_to_samples dict
        for config, data in self.config_to_samples.items():
            for sample in data["samples"]:
                # limit samples to put in the config_to_samples variable using
                # the limiting number and the samples to exclude
                if sample in self.samples and sample not in samples_to_exclude:
                    tmp_data.setdefault(config, {})
                    tmp_data[config].setdefault("samples", []).append(sample)

        self.config_to_samples = tmp_data
        self.samples = [
            sample
            for samples in self.config_to_samples.values()
            for sample in samples
        ]

    def subset_samples(self):
        """ Subset samples using the config information

        Raises:
            re.error: Invalid regex pattern provided
        """

        tmp_data = {}

        for config, samples in self.config_to_samples.items():
            subset = config.get("subset_samplesheet", None)

            if subset:
                # check that a valid pattern has been provided
                try:
                    re.compile(subset)
                except re.error:
                    raise re.error('Invalid subset pattern provided')

                subsetted_samples = [
                    x for x in samples if re.search(subset, x)
                ]

                assert subsetted_samples, (
                    f"No samples left after filtering using pattern {subset}"
                )

                tmp_data.setdefault(config, {})
                tmp_data[config]["samples"] = subsetted_samples

        self.config_to_samples = tmp_data
        self.samples = [
            sample
            for samples in self.config_to_samples.values()
            for sample in samples
        ]

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

        for config in self.config_to_samples:
            assay = config.get("assay")
            version = config.get("version")
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
            self.config_to_samples[config]["project"] = dx.bindings.dxproject.DXProject(dxid=project_id)

            users = config.get('users')

            if users:
                # users specified in config to grant access to project
                for user, access_level in users.items():
                    dx.bindings.dxproject.DXProject(dxid=project_id).invite(
                        user, access_level, send_email=False
                    )
                    prettier_print(
                        f"\nGranted {access_level} priviledge to {user}"
                    )

    def create_analysis_project_logs(self):
        """ Create an analysis project log per config file contained in the
        DXBuilder object """

        for config, data in self.config_to_samples.items():
            log_file_name = (
                f"{data['project'].describe()['name']}.log"
            )
            # write analysis project to file to pick up at end to send Slack message
            with open(log_file_name, 'w') as fh:
                fh.write(
                    f"{data['project'].describe()['id']} "
                    f"{config.get('assay_code')} "
                    f"{config.get('version')}\n"
                )

        # TODO handle the bash part to send the slack message

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
            run_time (str): String to represent execution time in YYMMDD_HHMM format
        """

        for config in self.config_to_samples:
            parent_out_dir = (
                f"{os.environ.get('DESTINATION', '')}/output/"
                f"{config.get('assay')}-{run_time}"
            )
            self.config_to_samples[config]["parent_out_dir"] = parent_out_dir.replace('//', '/')

    def set_config_for_demultiplexing(self):
        """ Select the config parameters that will be used in the
        demultiplexing job using the biggest instance type as the tie breaker.
        This also allows selection of additional args that could be
        antagonistic. """

        demultiplex_configs = []
        core_nbs = []

        for config in self.configs:
            demultiplex_config = config.get("demultiplex_config", None)

            if demultiplex_config:
                instance_type = demultiplex_config.get("instance_type", 0)
                demultiplex_configs.append(config)
                core_nbs.append(int(instance_type.split("_")[-1]))

        if core_nbs:
            bigger_core_nb = max(core_nbs)
            self.demultiplex_config = demultiplex_configs[
                demultiplex_configs.index(bigger_core_nb)
            ]
        else:
            self.demultiplex_config = {}

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

        for config in self.configs:
            executables = config.get("executables").keys()
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

            self.config_to_samples[config]["execution_mapping"] = execution_mapping

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

        for config in self.configs:
            executables = config.get("executables").keys()
            input_class_mapping = defaultdict(dict)

            for exe in executables:
                describe = dx.describe(exe)

                for input_spec in describe['inputSpec']:
                    input_class_mapping[exe][input_spec['name']] = defaultdict(
                        dict
                    )
                    input_class_mapping[exe][input_spec['name']]['class'] = input_spec['class']
                    input_class_mapping[exe][input_spec['name']]['optional'] = input_spec.get('optional', False)

            self.config_to_samples[config]["input_class_mapping"] = input_class_mapping

    def demultiplex(
        self,
        app_id,
        app_name,
        testing,
        demultiplex_config,
        demultiplex_output,
        sentinel_file,
        run_id,
        dx_project_id
    ) -> str:
        """
        Run demultiplexing app, holds until app completes.

        Either an app name, app ID or applet ID may be specified as input

        Parameters
        ----------
        app_id : str
            ID of demultiplexing app / applet to run
        app_name : str
            app- name of demultiplex app to run

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
                demultiplex_output = sentinel_path.replace('/runs', '')
        else:
            if not demultiplex_output:
                # running in testing and going to demultiplex -> dump output to
                # our testing analysis project to not go to sentinel file dir
                demultiplex_output = (
                    f'{dx_project_id}:/demultiplex_{time_stamp()}'
                )

        (
            self.demultiplex_project, self.demultiplex_folder
        ) = demultiplex_output.split(':')

        prettier_print(f'demultiplex app ID set: {app_id}')
        prettier_print(f'demultiplex app name set: {app_name}')
        prettier_print(f'optional config specified for demultiplexing: {demultiplex_config}')
        prettier_print(f'demultiplex out: {demultiplex_output}')
        prettier_print(f'demultiplex project: {self.demultiplex_project}')
        prettier_print(f'demultiplex folder: {self.demultiplex_folder}')

        instance_type = demultiplex_config.get("instance_type", None)

        if isinstance(instance_type, dict):
            # instance type defined in config is a mapping for multiple
            # flowcells, select appropriate one for current flowcell
            instance_type = select_instance_types(
                run_id=run_id,
                instance_types=instance_type
            )

        prettier_print(
            f"Instance type selected for demultiplexing: {instance_type}"
        )

        additional_args = demultiplex_config.get("additional_args", "")

        inputs = {
            'upload_sentinel_record': {
                "$dnanexus_link": sentinel_file
            }
        }

        if additional_args:
            inputs['advanced_opts'] = additional_args

        if os.environ.get("SAMPLESHEET_ID"):
            #  get just the ID of samplesheet in case of being formatted as
            # {'$dnanexus_link': 'file_id'} and add to inputs as this
            match = re.search(
                r'file-[\d\w]*', os.environ.get('SAMPLESHEET_ID')
            )

            if match:
                inputs['sample_sheet'] = {'$dnanexus_link': match.group()}

        prettier_print(f"\nInputs set for running demultiplexing: {inputs}")

        # check no fastqs are already present in the output directory for
        # demultiplexing, exit if any present to prevent making a mess
        # with demultiplexing output
        fastqs = list(dx.find_data_objects(
            name="*.fastq*",
            name_mode='glob',
            project=self.demultiplex_project,
            folder=self.demultiplex_folder
        ))

        assert not fastqs, Slack().send(
            "FastQs already present in output directory for demultiplexing: "
            f"`{demultiplex_output}`.\n\n"
            "Exiting now to not potentially pollute a previous demultiplex "
            "job output. \n\n"
            "Please either move the sentinel file or set the demultiplex "
            "output directory with `-iDEMULTIPLEX_OUT`"
        )

        if app_id.startswith('applet-'):
            job = dx.bindings.dxapplet.DXApplet(dxid=app_id).run(
                applet_input=inputs,
                project=self.demultiplex_project,
                folder=self.demultiplex_folder,
                priority='high',
                instance_type=instance_type
            )
        elif app_id.startswith('app-') or app_name:
            # running from app, prefer name over ID
            # have to set to None to only use ID or name if both set
            if app_name:
                app_id = None
            else:
                app_name = None

            job = dx.bindings.dxapp.DXApp(dxid=app_id, name=app_name).run(
                app_input=inputs,
                project=self.demultiplex_project,
                folder=self.demultiplex_folder,
                priority='high',
                instance_type=instance_type
            )
        else:
            raise RuntimeError(
                f'Provided demultiplex app ID does not appear valid: {app_id}')

        self.demultiplexing_job = job
        job_id = job.describe().get('id')

        # tag demultiplexing job so we easily know it was launched by conductor
        job.add_tags(tags=[
            f'Job run by eggd_conductor: {os.environ.get("PARENT_JOB_ID")}'
        ])

        prettier_print(
            f"Starting demultiplexing ({job_id}), "
            "holding app until completed..."
        )

        try:
            # holds app until demultiplexing job returns success
            job.wait_on_done()
        except dx.exceptions.DXJobFailureError as err:
            # dx job error raised (i.e. failed, timed out, terminated)
            job_url = (
                f"platform.dnanexus.com/projects/"
                f"{self.demultiplex_project.replace('project-', '')}"
                "/monitor/job/"
                f"{job_id}"
            )

            Slack().send(
                f"Demultiplexing job failed!\n\nError: {err}\n\n"
                f"Demultiplexing job: {job_url}"
            )
            raise dx.exceptions.DXJobFailureError()

        prettier_print("Demuliplexing completed!")

        if testing:
            with open('testing_job_id.log', 'a') as fh:
                fh.write(f'{job_id} ')

    def move_demultiplex_qc_files(self, project_id):
        """ Move demultiplexing QC files to the given project

        Args:
            project_id (str): DNAnexus project id to move the QC files to
        """

        # check for and copy / move the required files for multiQC from
        # bclfastq or bclconvert into a folder in root of the analysis project
        qc_files = [
            "Stats.json",              # bcl2fastq
            "RunInfo.xml",             # ↧ bclconvert
            "Demultiplex_Stats.csv",
            "Quality_Metrics.csv",
            "Adapter_Metrics.csv",
            "Top_Unknown_Barcodes.csv"
        ]

        # need to first create destination folder
        dx.api.project_new_folder(
            object_id=project_id,
            input_params={
                'folder': '/demultiplex_multiqc_files',
                'parents': True
            }
        )

        for file in qc_files:
            dx_object = list(dx.bindings.search.find_data_objects(
                name=file,
                project=self.demultiplex_project,
                folder=self.demultiplex_folder
            ))

            if dx_object:
                dx_file = dx.DXFile(
                    dxid=dx_object[0]['id'],
                    project=dx_object[0]['project']
                )

                if project_id == self.demultiplex_project:
                    # demultiplex output in the analysis project => need to move
                    # instead of cloning (this is most likely just for testing)
                    dx_file.move(folder='/demultiplex_multiqc_files')
                else:
                    # copying to separate analysis project
                    dx_file.clone(
                        project=project_id,
                        folder='/demultiplex_multiqc_files'
                    )

    def build_job_inputs_per_sample(
        self, executable, config, executable_param, sample, executable_out_dirs
    ):
        self.job_inputs.setdefault(sample, {})
        self.job_inputs[sample].setdefault(executable, {})

        job_outputs_config = self.job_outputs[config]

        # select input and output dict from config for current workflow / app
        config_copy = deepcopy(config)
        input_dict = config_copy['executables'][executable]['inputs']
        output_dict = config_copy['executables'][executable]['output_dirs']

        if executable_param['executable_name'].startswith('TSO500_reports_workflow'):
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
        if executable_param["process_fastqs"] is True:
            input_dict = manage_dict.add_fastqs(
                input_dict=input_dict,
                fastq_details=self.fastq_details,
                sample=sample
            )

        # find all jobs for previous analyses if next job depends on them
        if executable_param.get("depends_on"):
            dependent_jobs = manage_dict.get_dependent_jobs(
                param=executable_param,
                job_outputs_dict=job_outputs_config,
                sample=sample
            )
        else:
            dependent_jobs = []

        self.job_inputs[sample][executable]["dependent_jobs"] = dependent_jobs

        sample_prefix = sample

        if executable_param.get("sample_name_delimeter"):
            # if delimeter specified to split sample name on, use it
            delim = executable_param.get("sample_name_delimeter")

            if delim in sample:
                sample_prefix = sample.split(delim)[0]
            else:
                prettier_print((
                    f'Specified delimeter ({delim}) is not in sample name '
                    f'({sample}), ignoring and continuing...'
                ))

        project_info = self.config_to_samples[config]["project"].describe()
        project_id = project_info.get("id")
        project_name = project_info.get("name")

        # handle other inputs defined in config to add to inputs
        # sample_prefix passed to pass to INPUT-SAMPLE_NAME
        input_dict = manage_dict.add_other_inputs(
            input_dict=input_dict,
            parent_out_dir=self.config_to_samples[config]["parent_out_dir"],
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
            analysis=executable_param["analysis"],
            per_sample=True,
            sample=sample
        )

        # check input types correctly set in input dict
        input_dict = manage_dict.check_input_classes(
            input_dict=input_dict,
            input_classes=self.config_to_samples[config]["input_class_mapping"][executable]
        )

        # check that all INPUT- have been parsed in config
        manage_dict.check_all_inputs(input_dict)

        # set job name as executable name and sample name
        job_name = f"{executable_param['executable_name']}-{sample}"

        self.job_inputs[sample][executable]["job_name"] = job_name

        # create output directory structure in config
        manage_dict.populate_output_dir_config(
            executable=executable,
            exe_names=self.config_to_samples[config]["execution_mapping"],
            output_dict=output_dict,
            out_folder=self.config_to_samples[config]["parent_out_dir"]
        )

        # call dx run to start jobs
        prettier_print(
            f"\nCalling {executable_param['executable_name']} ({executable}) "
            f"on sample {sample}"
            )

        if input_dict.keys:
            prettier_print(f'\nInput dict: {input_dict}')

        self.job_inputs[sample][executable]["inputs"] = input_dict
