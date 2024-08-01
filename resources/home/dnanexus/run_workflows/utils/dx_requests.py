"""
Functions related to querying and managing objects in DNAnexus, as well
as running jobs.
"""
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
import json
import os
from pprint import PrettyPrinter
import random
import re
from typing import Tuple

import dxpy as dx
from packaging.version import Version, parse

from utils.dx_utils import find_dx_project
from utils.manage_dict import ManageDict
from utils.utils import (
    Slack,
    prettier_print,
    select_instance_types,
    time_stamp
)


class DXExecute():
    """
    Methods for handling execution of apps / workflows
    """
    def __init__(self, args) -> None:
        self.args = args

    def demultiplex(self, app_id=None, app_name=None, config=None) -> str:
        """
        Run demultiplexing app, holds until app completes.

        Either an app name, app ID or applet ID may be specified as input

        Parameters
        ----------
        app_id : str
            ID of demultiplexing app / applet to run
        app_name : str
            app- name of demultiplex app to run
        config : dict
            optional config values for demultiplex app

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
        if not self.args.testing:
            if not self.args.demultiplex_output:
                # set output path to parent of sentinel file
                out = dx.describe(self.args.sentinel_file)
                sentinel_path = f"{out.get('project')}:{out.get('folder')}"
                self.args.demultiplex_output = sentinel_path.replace('/runs', '')
        else:
            if not self.args.demultiplex_output:
                # running in testing and going to demultiplex -> dump output to
                # our testing analysis project to not go to sentinel file dir
                self.args.demultiplex_output = (
                    f'{self.args.dx_project_id}:/demultiplex_{time_stamp()}'
                )

        demultiplex_project, demultiplex_folder = self.args.demultiplex_output.split(':')

        prettier_print(f'demultiplex app ID set: {app_id}')
        prettier_print(f'demultiplex app name set: {app_name}')
        prettier_print(f'optional config specified for demultiplexing: {config}')
        prettier_print(f'demultiplex out: {self.args.demultiplex_output}')
        prettier_print(f'demultiplex project: {demultiplex_project}')
        prettier_print(f'demultiplex folder: {demultiplex_folder}')

        # instance type and additional args may be specified in assay config
        # for running demultiplexing, get them if present
        instance_type = config.get('instance_type')
        if isinstance(instance_type, dict):
            # instance type defined in config is a mapping for multiple
            # flowcells, select appropriate one for current flowcell
            instance_type = select_instance_types(
                run_id=self.args.run_id,
                instance_types=instance_type)

        prettier_print(f"Instance type selected for demultiplexing: {instance_type}")

        additional_args = config.get('additional_args')

        inputs = {
            'upload_sentinel_record': {
                "$dnanexus_link": self.args.sentinel_file
            }
        }

        if additional_args:
            inputs['advanced_opts'] = additional_args

        if os.environ.get("SAMPLESHEET_ID"):
            #  get just the ID of samplesheet in case of being formatted as
            # {'$dnanexus_link': 'file_id'} and add to inputs as this
            match = re.search(r'file-[\d\w]*', os.environ.get('SAMPLESHEET_ID'))
            if match:
                inputs['sample_sheet'] = {'$dnanexus_link': match.group()}

        prettier_print(f"\nInputs set for running demultiplexing: {inputs}")

        # check no fastqs are already present in the output directory for
        # demultiplexing, exit if any present to prevent making a mess
        # with demultiplexing output
        fastqs = list(dx.find_data_objects(
            name="*.fastq*",
            name_mode='glob',
            project=demultiplex_project,
            folder=demultiplex_folder
        ))

        assert not fastqs, Slack().send(
            "FastQs already present in output directory for demultiplexing: "
            f"`{self.args.demultiplex_output}`.\n\nExiting now to not "
            f"potentially pollute a previous demultiplex job output. \n\n"
            "Please either move the sentinel file or set the demultiplex "
            "output directory with `-iDEMULTIPLEX_OUT`"
        )

        if app_id.startswith('applet-'):
            job = dx.bindings.dxapplet.DXApplet(dxid=app_id).run(
                applet_input=inputs,
                project=demultiplex_project,
                folder=demultiplex_folder,
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
                project=demultiplex_project,
                folder=demultiplex_folder,
                priority='high',
                instance_type=instance_type
            )
        else:
            raise RuntimeError(
                f'Provided demultiplex app ID does not appear valid: {app_id}')

        job_id = job.describe().get('id')
        job_handle = dx.bindings.dxjob.DXJob(dxid=job_id)

        # tag demultiplexing job so we easily know it was launched by conductor
        job_handle.add_tags(tags=[
            f'Job run by eggd_conductor: {os.environ.get("PARENT_JOB_ID")}'
        ])

        prettier_print(
            f"Starting demultiplexing ({job_id}), "
            "holding app until completed..."
        )

        try:
            # holds app until demultiplexing job returns success
            job_handle.wait_on_done()
        except dx.exceptions.DXJobFailureError as err:
            # dx job error raised (i.e. failed, timed out, terminated)
            job_url = (
                f"platform.dnanexus.com/projects/"
                f"{demultiplex_project.replace('project-', '')}/monitor/job/"
                f"{job_id}"
            )

            Slack().send(
                f"Demultiplexing job failed!\n\nError: {err}\n\n"
                f"Demultiplexing job: {job_url}"
            )
            raise dx.exceptions.DXJobFailureError()

        prettier_print("Demuliplexing completed!")

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
            object_id=self.args.dx_project_id,
            input_params={
                'folder': '/demultiplex_multiqc_files',
                'parents':True
            }
        )

        for file in qc_files:
            dx_object = list(dx.bindings.search.find_data_objects(
                name=file,
                project=demultiplex_project,
                folder=demultiplex_folder
            ))

            if dx_object:
                dx_file = dx.DXFile(
                    dxid=dx_object[0]['id'],
                    project=dx_object[0]['project']
                )

                if self.args.dx_project_id == demultiplex_project:
                    # demultiplex output in the analysis project => need to move
                    # instead of cloning (this is most likely just for testing)
                    dx_file.move(folder='/demultiplex_multiqc_files')
                else:
                    # copying to separate analysis project
                    dx_file.clone(
                        project=self.args.dx_project_id,
                        folder='/demultiplex_multiqc_files'
                    )

        if self.args.testing:
            with open('testing_job_id.log', 'a') as fh:
                fh.write(f'{job_id} ')

        return job_id

    def call_dx_run(
        self, executable, job_name, input_dict,
        output_dict, prev_jobs, extra_args, instance_types) -> str:
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

    def call_per_sample(
            self,
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
            instance_types
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
                job_outputs_dict[x] for x in job_outputs_dict if x.startswith('analysis_')
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
            all_output_files, job_output_ids = DXManage(
                args=None).get_job_output_details(tso500_id)

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
            args=self.args,
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

        job_id = self.call_dx_run(
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
        self, executable, exe_names, input_classes, params, config,
        out_folder, job_outputs_dict, executable_out_dirs, fastq_details,
        instance_types) -> dict:
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
        if self.args.upload_tars:
            input_dict = ManageDict().add_upload_tars(
                input_dict=input_dict,
                upload_tars=self.args.upload_tars
            )

        # handle other inputs defined in config to add to inputs
        input_dict = ManageDict().add_other_inputs(
            input_dict=input_dict,
            args=self.args,
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
        job_id = self.call_dx_run(
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


class DXManage():
    """
    Methods for generic handling of dx related things
    """
    def __init__(self, args) -> None:
        self.args = args

    def create_dx_folder(self, out_folder) -> str:
        """
        Create output folder in DNAnexus project for storing analysis output

        Parameters
        ----------
        out_folder : str
            name for analysis output folder

        Returns
        -------
        dx_folder : str
            name of created output directory in given project

        Raises
        ------
        RuntimeError
            If >100 output directories found with given name, very unlikely
            for this to happen and is used as a sanity check to stop any
            ambiguous downstream errors
        """
        for i in range(1, 100):
            # sanity check, should only be 1 or 2 already existing at most
            dx_folder = f'{out_folder}-{i}'

            try:
                dx.api.project_list_folder(
                    self.args.dx_project_id,
                    input_params={"folder": dx_folder, "only": "folders"},
                    always_retry=True
                )
            except dx.exceptions.ResourceNotFound:
                # can't find folder => create one
                dx.api.project_new_folder(
                    self.args.dx_project_id, input_params={
                        'folder': dx_folder, "parents": True
                    }
                )
                prettier_print(f'Created output folder: {dx_folder}')
                return dx_folder
            else:
                # folder already exists, increase _i suffix on folder name
                # and check again
                prettier_print(f'{dx_folder} already exists, incrementing suffix integer')
                continue

        # got to end of loop, highly unlikely we would ever run this many in a
        # project but catch it here to stop some ambiguous downstream error
        raise RuntimeError(
            "Found 100 output directories in project, exiting now as "
            "there is likely an issue in the project."
        )

    def get_upload_tars(self) -> list:
        """
        Get list of upload tar file IDs from given sentinel file,
        and return formatted as a list of $dnanexus_link dicts

        Returns
        -------
        list
            list of file ids formated as {"$dnanexus_link": file-xxx}
        """
        if not self.args.sentinel_file:
            # sentinel file not provided as input -> no tars to parse
            return None

        details = dx.bindings.dxrecord.DXRecord(
            dxid=self.args.sentinel_file).describe(incl_details=True)

        upload_tars = details['details']['tar_file_ids']

        prettier_print(f"\nFollowing upload tars found to add as input: {upload_tars}")

        # format in required format for a dx input
        upload_tars = [
            {"$dnanexus_link": x} for x in upload_tars
        ]

        return upload_tars


class DXBuilder():
    def __init__(self):
        self.args = {}
        self.configs = []
        self.samples = []
        self.config_to_samples = None
        self.project_files = []

    def get_assays(self):
        return sorted(
            [config.get('assay_code') for config in self.configs.values()]
        )

    def add_args(self, **kwargs):
        """ Add args in an agnostic way """

        for k, v in kwargs.items():
            self.args[k] = v

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

        d = {}

        # limit samples in the config_to_samples dict
        for config, data in self.config_to_samples.items():
            for sample in data["samples"]:
                # limit samples to put in the config_to_samples variable using
                # the limiting number and the samples to exclude
                if sample in self.samples and sample not in samples_to_exclude:
                    d.setdefault(config, {})
                    d[config].setdefault("samples", []).append(sample)

        self.config_to_samples = d
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

        d = {}

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

                d.setdefault(config, {})
                d[config]["samples"] = subsetted_samples

        self.config_to_samples = d
        self.samples = [
            sample
            for samples in self.config_to_samples.values()
            for sample in samples
        ]

    def get_or_create_dx_project(self) -> str:
        """
        Create new project in DNAnexus if one with given name doesn't
        already exist.

        Returns
        -------
        str : ID of DNAnexus project
        """

        run_id = self.args.get("run_id")

        if self.args.get("development", None):
            prefix = f'003_{datetime.now().strftime("%y%m%d")}_run-'
        else:
            prefix = '002_'

        suffix = ''

        if self.args.get("testing", None):
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

    def get_upload_tars(self) -> list:
        """
        Get list of upload tar file IDs from given sentinel file,
        and return formatted as a list of $dnanexus_link dicts

        Returns
        -------
        list
            list of file ids formated as {"$dnanexus_link": file-xxx}
        """

        sentinel_file = self.args.get("sentinel_file", None)

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
