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
import re
from typing import Tuple

import dxpy as dx
from packaging.version import Version, parse

from utils.manage_dict import ManageDict
from utils.utils import Slack, log, select_instance_types, time_stamp


PPRINT = PrettyPrinter(indent=1).pprint


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

        log.info(f'demultiplex app ID set: {app_id}')
        log.info(f'demultiplex app name set: {app_name}')
        log.info(f'optional config specified for demultiplexing: {PPRINT(config)}')
        log.info(f'demultiplex out: {self.args.demultiplex_output}')
        log.info(f'demultiplex project: {demultiplex_project}')
        log.info(f'demultiplex folder: {demultiplex_folder}')

        # instance type and additional args may be specified in assay config
        # for running demultiplexing, get them if present
        instance_type = config.get('instance_type')
        if isinstance(instance_type, dict):
            # instance type defined in config is a mapping for multiple
            # flowcells, select appropriate one for current flowcell
            instance_type = select_instance_types(
                run_id=self.args.run_id,
                instance_types=instance_type)

        log.info(f"Instance type selected for demultiplexing: {instance_type}")

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

        log.info(f"\nInputs set for running demultiplexing: {inputs}")

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

        log.info(
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

        log.info("Demuliplexing completed!")

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
        log.info(f"\nPopulated input dict for: {executable}")
        log.info(PPRINT(input_dict))

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

        log.info(f'Started analysis in project {self.args.dx_project_id}, job: {job_id}')

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
            tso500_id = jobs.get('eggd_tso500')

            assert tso500_id, "Could not find prior eggd_tso500 job"

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
                log.error((
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
        log.info(
            f"\nCalling {params['executable_name']} ({executable}) "
            f"on sample {sample}"
            )

        if input_dict.keys:
            log.info(f'\nInput dict: {log.info(PPRINT(input_dict))}')

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
        log.info(f'\nCalling {params["name"]} for all samples')
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


    def get_json_configs(self) -> dict:
        """
        Query path in DNAnexus for json config files for each assay, returning
        full data for all unarchived config files found

        ASSAY_CONFIG_PATH comes from the app config file sourced to the env.

        Returns
        -------
        list
            list of dicts of the json object for each config file found

        Raises
        ------
        AssertionError
            Raised when invalid project:path structure defined in app config
        AssertionError
            Raised when no config files found at the given path
        """
        config_path = os.environ.get('ASSAY_CONFIG_PATH', '')

        # check for valid project:path structure
        assert re.match(r'project-[\d\w]*:/.*', config_path), Slack().send(
            f'ASSAY_CONFIG_PATH from config appears invalid: {config_path}'
        )

        log.info(f"\nSearching following path for assay configs: {config_path}")

        project, path = config_path.split(':')

        files = list(dx.find_data_objects(
            name="*.json",
            name_mode='glob',
            project=project,
            folder=path,
            describe=True
        ))

        # sense check we find config files
        assert files, Slack().send(
            f"No config files found in given path: {project}:{path}")

        files_ids='\n\t'.join([
            f"{x['describe']['name']} ({x['id']} - "
            f"{x['describe']['archivalState']})" for x in files])
        log.info(f"\nAssay config files found:\n\t{files_ids}")

        all_configs = []
        for file in files:
            if file['describe']['archivalState'] == 'live':
                config_data = json.loads(
                    dx.bindings.dxfile.DXFile(
                        project=file['project'], dxid=file['id']).read())

                # add file ID as field into the config file
                config_data['file_id'] = file['id']
                all_configs.append(config_data)
            else:
                log.info(
                    "Config file not in live state - will not be used:"
                    f"{file['describe']['name']} ({file['id']}"
                )

        return all_configs


    @staticmethod
    def filter_highest_config_version(all_configs) -> dict:
        """
        Filters all configs found from get_json_configs() to retain highest
        version for each assay code to use for analysis.

        Assay codes are expected to be either a single code in the 'assay_code'
        field in the config file, or a '|' seperated string of multiple.

        This keeps the highest version of each assay code, factoring in where
        one assay code may be a subset of another with a higher version, and
        only keeping the latter, i.e.
        {'EGG2': 1.0.0, 'EGG2|LAB123': 1.1.0} -> {'EGG2|LAB123': 1.1.0}


        Parameters
        ----------
        all_configs : list
            list of dicts of the json object for each config file
            found, returned from get_json_configs()

        Returns
        -------
        dict
            mapping of assay_code to full config data for the highest
            version config file for each assay_code

        Raises
        ------
        AssertionError
            Raised when config file has missing assay_code or version field
        
        """
        # filter all config files to just get full config data for the
        # highest version of each full assay code
        log.info("\nFiltering config files from DNAnexus for highest versions")
        highest_version_config_data = {}

        for config in all_configs:
            current_config_code = config.get('assay_code')
            current_config_ver = config.get('version')

            # sense check config file has code and version fields
            assert current_config_code and current_config_ver, Slack().send(
                f"Config file missing assay_code and/or version field!"
                f"File ID: {config['file_id']}"
            )

            # get highest stored version of config file for current code
            # we have found so far
            highest_version = highest_version_config_data.get(
                current_config_code, {}).get('version', '0')

            if Version(current_config_ver) > Version(highest_version):
                # higher version than stored one for same code => replace
                highest_version_config_data[current_config_code] = config

        # build simple dict of assay_code : version
        all_assay_codes = {
            x['assay_code']: x['version']
            for x in highest_version_config_data.values()
        }

        # get unique list of single codes from all assay codes, split on '|'
        # i.e. ['EGG1', 'EGG2', 'EGG2|LAB123'] -> ['EGG1', 'EGG2', 'LAB123']
        uniq_codes = [
            x.split('|') for x in all_assay_codes.keys()]
        uniq_codes = list(set([
            code for split_codes in uniq_codes for code in split_codes]))

        log.info(
            "\nUnique assay codes parsed from all config "
            f"assay_code fields {uniq_codes}\n"
        )

        # final dict of config files to use as assay_code : config data
        configs_to_use = {}

        # for each single assay code, find the highest version config file
        # that code is present in (i.e. {'EGG2': 1.0.0, 'EGG2|LAB123': 1.1.0}
        # would result in EGG2 -> {'EGG2|LAB123': 1.1.0})
        for uniq_code in uniq_codes:
            matches = {}
            for full_code in all_assay_codes.keys():
                if uniq_code in full_code.split('|'):
                    # this single assay code is in the full assay code
                    # parsed from config, add match as 'assay_code': 'version'
                    matches[full_code] =  all_assay_codes[full_code]

            # check we don't have 2 matches with the same version as we
            # can't tell which to use, i.e. EGG2 : 1.0.0 & EGG2|LAB123 : 1.0.0
            assert sorted(list(matches.values())) == \
                sorted(list(set(matches.values()))), Slack().send(
                f"More than one version of config file found for a single "
                f"assay code!\n\t{matches}"
            )

            # for this unique code, select the full assay code with the highest
            # version this one was found in using packaging.version.parse, and
            # then select the full config file data for it
            full_code_to_use = max(matches, key=parse)
            configs_to_use[
                full_code_to_use] = highest_version_config_data[full_code_to_use]

        # add to log record of highest version of each config found
        usable_configs = '\n\t'.join(
            [f"{k} ({v['version']}): {v['file_id']}"
            for k, v in configs_to_use.items()]
        )

        log.info(
            "\nHighest versions of assay configs found to use:"
            f"\n\t{usable_configs}\n"
        )

        return configs_to_use


    def find_dx_project(self, project_name) -> str:
        """
        Check if project already exists in DNAnexus with given name,
        returns project ID if present and None if not found.

        Parameters
        ----------
        project_name : str
            name of project to search DNAnexus for

        Returns
        -------
        dx_project : str
            dx ID of given project if found, else returns `None`

        Raises
        ------
        AssertionError
            Raised when more than one project found for given name
        """
        dx_projects = list(dx.bindings.search.find_projects(name=project_name))

        log.info('Found the following DNAnexus projects:')
        log.info(PPRINT(dx_projects))

        if not dx_projects:
            # found no project, return None and create one in
            # get_or_create_dx_project()
            return None

        assert len(dx_projects) == 1, Slack().send(
            "Found more than one project matching given "
            f"project name: {project_name}"
        )

        return dx_projects[0]['id']


    def get_or_create_dx_project(self, config) -> str:
        """
        Create new project in DNAnexus if one with given name doesn't
        already exist.

        Parameters
        ----------
        config : dict
            low level assay config read from json file

        Returns
        -------
        str : ID of DNAnexus project
        """
        if self.args.development:
            prefix = f'003_{datetime.now().strftime("%y%m%d")}_run-'
        else:
            prefix = '002_'

        suffix = ''
        if self.args.testing:
            suffix = '-EGGD_CONDUCTOR_TESTING'

        output_project = (
            f'{prefix}{self.args.run_id}_{config.get("assay")}{suffix}'
        )
        project_id = self.find_dx_project(output_project)

        if not project_id:
            # create new project and capture returned project id and store
            project_id = dx.bindings.dxproject.DXProject().new(
                name=output_project,
                summary=(
                    f'Analysis of run {self.args.run_id} with '
                    f'{config.get("assay")} {config.get("version")} config'
                ),
                description=(
                    "This project was automatically created by eggd_conductor "
                    f"from {os.environ.get('PARENT_JOB_ID')}"
                )
            )
            log.info(
                f'\nCreated new project for output: {output_project} ({project_id})'
            )
        else:
            log.info(f'\nUsing existing found project: {output_project} ({project_id})')

        users = config.get('users')
        if users:
            # users specified in config to grant access to project
            for user, access_level in users.items():
                dx.bindings.dxproject.DXProject(dxid=project_id).invite(
                    user, access_level, send_email=False
                )
                log.info(f"\nGranted {access_level} priviledge to {user}")

        return project_id


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
                log.info(f'Created output folder: {dx_folder}')
                return dx_folder
            else:
                # folder already exists, increase _i suffix on folder name
                # and check again
                log.info(f'{dx_folder} already exists, incrementing suffix integer')
                continue

        # got to end of loop, highly unlikely we would ever run this many in a
        # project but catch it here to stop some ambiguous downstream error
        raise RuntimeError(
            "Found 100 output directories in project, exiting now as "
            "there is likely an issue in the project."
        )


    def get_demultiplex_job_details(self, job_id) -> list:
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
        log.info(f"\nGetting fastqs from given demultiplexing job: {job_id}")
        demultiplex_job = dx.bindings.dxjob.DXJob(dxid=job_id).describe()
        demultiplex_project = demultiplex_job['project']
        demultiplex_folder = demultiplex_job['folder']

        # find all fastqs from demultiplex job, return list of dicts with details
        fastq_details = list(dx.search.find_data_objects(
            name="*.fastq*", name_mode="glob", project=demultiplex_project,
            folder=demultiplex_folder, describe=True
        ))
        # build list of tuples with fastq name and file ids
        fastq_details = [
            (x['id'], x['describe']['name']) for x in fastq_details
        ]
        # filter out Undetermined fastqs
        fastq_details = [
            x for x in fastq_details if not x[1].startswith('Undetermined')
        ]

        log.info(f'\nFastqs parsed from demultiplexing job {job_id}')
        log.info(PPRINT(fastq_details))

        return fastq_details


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

        log.info(f"\nFollowing upload tars found to add as input: {upload_tars}")

        # format in required format for a dx input
        upload_tars = [
            {"$dnanexus_link": x} for x in upload_tars
        ]

        return upload_tars


    def get_executable_names(self, executables) -> dict:
        """
        Build a dict of all executable IDs parsed from config to human
        readable names, used for naminmg outputs needing workflow/app names

        Parameters
        ----------
        executables : list
            list of executables to get names for (workflow-, app-, applet-)

        Returns
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
        log.info(f'\nGetting names for all executables: {executables}')
        mapping = defaultdict(dict)

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
                mapping[exe]['name'] = workflow_name
                mapping[exe]['stages'] = defaultdict(dict)

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
                    mapping[exe]['stages'][stage_id] = stage_name

            elif exe.startswith('app-') or exe.startswith('applet-'):
                app_details = dx.api.workflow_describe(exe)
                app_name = app_details['name'].replace('/', '-')
                if app_name.startswith('app-'):
                    app_name = app_name.replace('app-', '')
                mapping[exe] = {'name': app_name}

        return mapping


    def get_input_classes(self, executables) -> dict:
        """
        Get classes of all inputs for each app / workflow stage, used
        when building out input dict to ensure correct type set

        Parameters
        ----------
        executables : list
            list of executables to get names for (workflow-, app-, applet-)

        Returns
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
        mapping = defaultdict(dict)
        for exe in executables:
            describe = dx.describe(exe)
            for input in describe['inputSpec']:
                mapping[exe][input['name']] = defaultdict(dict)
                mapping[exe][input['name']]['class'] = input['class']
                mapping[exe][input['name']]['optional'] = input.get(
                    'optional', False)

        return mapping


    def get_job_output_details(self, job_id) -> Tuple[list, list]:
        """
        Get describe details for all output files from a job

        Parameters
        ----------
        job_id : str
            ID of job to get output files from

        Returns
        -------
        list
            list of describe dicts for each file found
        list
            list of dicts of job output field -> job output file IDs
        """
        print(f"Querying output files for {job_id}")
        # find files in given jobs out directory
        job_details = dx.DXJob(dxid=job_id).describe()
        job_output_ids = job_details.get('output')
        all_output_files = list(dx.find_data_objects(
            project=job_details.get('project'),
            folder=job_details.get('folder'),
            describe=True
        ))

        # ensure these files only came from our given job
        all_output_files = [
            x for x in all_output_files
            if x['describe']['createdBy']['job'] == job_id
        ]

        print(f"Found {len(all_output_files)} from {job_id}")

        return all_output_files, job_output_ids


    def wait_on_done(self, analysis, analysis_name, all_job_ids) -> None:
        """
        Hold eggd_conductor until all job(s) for the given analysis step
        have completed

        Parameters
        ----------
        analysis : str
            analysis step to select job IDs to wait on
        analysis_name : str
            name of analysis step to wait on
        all_job_ids : dict
            mapping of analysis step -> job ID(s)
        """
        # job_outputs_dict for per run jobs structured as
        # {'analysis_1': 'job-xxx'} and per sample as
        # {'sample1': {'analysis_2': 'job-xxx'}...} => try and get both
        job_ids = [all_job_ids.get(analysis)]
        job_ids.extend([
            x.get(analysis) for x in all_job_ids.values()
            if isinstance(x, dict)
        ])

        # ensure we don't have any Nones
        job_ids = [x for x in job_ids if x]

        log.info(
            f'Holding conductor until {len(job_ids)} '
            f'{analysis_name} job(s) complete: {", ".join(job_ids)}'
        )

        for job in job_ids:
            if job.startswith('job-'):
                dx.DXJob(dxid=job).wait_on_done()
            else:
                dx.DXAnalysis(dxid=job).wait_on_done()

        print('All jobs to wait on completed')
