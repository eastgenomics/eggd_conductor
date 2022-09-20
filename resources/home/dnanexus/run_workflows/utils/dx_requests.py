from copy import deepcopy
from datetime import datetime
import json
import os
from pprint import PrettyPrinter
import re
from typing import Union

import dxpy as dx

from utils.manage_dict import ManageDict
from utils.utils import Slack, time_stamp


PPRINT = PrettyPrinter(indent=2).pprint


class DXExecute():
    """
    Methods for handling exeuction of apps / worklfows
    """
    def __init__(self, args) -> None:
        self.args = args


    def demultiplex(self) -> str:
        """
        Run demultiplexing app, hold until app completes

        Returns
        -------
        str
            ID of demultiplexing job
        """
        print("Starting demultiplexing, holding app until comletion...")

        if not self.args.testing:
            # set output path to parent of sentinel file
            sentinel_path = dx.describe(self.args.sentinel_file).get('folder')
            bcl2fastq_out = sentinel_path.replace('/runs', '')
        else:
            # running in testing and going to demultiplex -> dump output to
            # our testing analysis project to not go to semtinel file dir
            bcl2fastq_out = f'{self.args.dx_project_id}:/bcl2fastq_{time_stamp}'


        app_id = os.environ.get('BCL2FASTQ_APP_ID')
        inputs = {
            'upload_sentinel_record': self.args.sentinel_file,
        }

        # check no fastqs are already present in the output directory for
        # bcl2fastq, exit if any present to prevent making a mess
        # with bcl2fastq output
        fastqs = list(dx.find_data_objects(
            name="*.fastq*",
            name_mode='glob',
            folder=bcl2fastq_out
        ))

        assert not fastqs, Slack().send(
            "fastqs already present in directory for bcl2fastq output"
            f"({bcl2fastq_out})"
        )

        job = dx.bindings.dxapp.DXApp(app_id).run(
            app_input=inputs,
            folder=bcl2fastq_out,
            priority='high'
        )

        dx.bindings.dxjob.DXJob(dxid=job).wait_on_done()

        print("Demuliplexing completed!")

        # copy the demultiplexing stats json into the project for multiQC
        stats_json = list(dx.bindings.search.find_data_objects(
            project='project-FpVG0G84X7kzq58g19vF1YJQ',
            folder=f'{bcl2fastq_out}/Data/Intensities/BaseCalls/Stats/',
            name="Stats.json"
        ))

        if stats_json:
            ## TODO: need to figure out copying files
            ## DOESNT WORK ATM
            pass
            dx.bindings.DXDataObject(dxid=stats_json['id']).clone(
                project=self.args.dx_project_id,
                folder='/'
            )


        return job


    def call_dx_run(self, executable, job_name, input_dict, output_dict, prev_jobs) -> str:
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

        Returns
        -------
        job_id : str
            DNAnexus job id of newly started analysis

        Raises
        ------
        RuntimeError
            Raised when workflow-, app- or applet- not present in exe name
        """
        if 'workflow-' in executable:
            job_handle = dx.bindings.dxworkflow.DXWorkflow(
                dxid=executable,
                project=self.args.dx_project_id
            ).run(
                workflow_input=input_dict,
                stage_folders=output_dict,
                rerun_stages=['*'],
                depends_on=prev_jobs,
                name=job_name
            )
        elif 'app-' in executable:
            job_handle = dx.bindings.dxapp.DXApp(dxid=executable).run(
                app_input=input_dict,
                project=self.args.dx_project_id,
                folder=output_dict[executable],
                ignore_reuse=True,
                depends_on=prev_jobs,
                name=job_name
            )
        elif 'applet-' in executable:
            job_handle = dx.bindings.dxapplet.DXApplet(dxid=executable).run(
                applet_input=input_dict,
                project=self.args.dx_project_id,
                folder=output_dict[executable],
                ignore_reuse=True,
                depends_on=prev_jobs,
                name=job_name
            )
        else:
            # doesn't appear to be valid workflow or app
            raise RuntimeError(
                f'Given executable id is not valid: {executable}'
            )

        job_details = job_handle.describe()
        job_id = job_details.get('id')

        print(f'Started analysis in project {self.args.dx_project_id}, job: {job_id}')

        with open('job_id.log', 'a') as fh:
            fh.write(f'{job_id} ')

        if self.args.testing:
            with open('testing_job_id.log', 'a') as fh:
                fh.write(f'{job_id} ')

        return job_id


    def call_per_sample(
        self, executable, params, sample, config, out_folder,
        job_outputs_dict, executable_out_dirs, fastq_details) -> dict:
        """
        Populate input and output dicts for given workflow and sample, then
        call to dx to start job. Job id is returned and stored in output dict
        that maps the workflow to dx job id for given sample.

        Parameters
        ----------
        executable : str
            human readable name of dx executable (workflow-, app- or applet-)
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
            dict of analsysis stage to its output dir path, used to pass output
            of an analysis to input of another (i.e.
            analysis_1 : /path/to/output)
        fastq_details : list of tuples
            list with tuple per fastq containing (DNAnexus file id, filename)

        Returns
        -------
        job_outputs_dict : dict
            dictionary of analysis stages to dx job ids created
        """
        # select input and output dict from config for current workflow / app
        config_copy = deepcopy(config)
        input_dict = config_copy['executables'][executable]['inputs']
        output_dict = config_copy['executables'][executable]['output_dirs']

        # create output directory structure in config
        ManageDict().populate_output_dir_config(executable, output_dict, out_folder)

        # check if stage requires fastqs passing
        if params["process_fastqs"] is True:
            input_dict = ManageDict().add_fastqs(input_dict, fastq_details, sample)

        # find all jobs for previous analyses if next job depends on them
        if params.get("depends_on"):
            dependent_jobs = ManageDict().get_dependent_jobs(
                params, job_outputs_dict, sample=sample)
        else:
            dependent_jobs = []

        if params.get("sample_name_delimeter"):
            # if delimeter specified to split sample name on, use it
            delim = params.get("sample_name_delimeter")

            if delim in sample:
                sample = sample.split(delim)[0]
            else:
                print((
                    f'Specified delimeter ({delim}) is not in sample name '
                    f'({sample}), ignoring and continuing...'
                ))

        # handle other inputs defined in config to add to inputs
        input_dict = ManageDict().add_other_inputs(
            input_dict=input_dict,
            dx_project_id=self.args.dx_project_id,
            executable_out_dirs=executable_out_dirs,
            sample=sample
        )

        # check any inputs dependent on previous job outputs to add
        input_dict = ManageDict().link_inputs_to_outputs(
            job_outputs_dict=job_outputs_dict,
            input_dict=input_dict,
            analysis=params["analysis"],
            per_sample=True,
            sample=sample
        )

        # check that all INPUT- have been parsed in config
        ManageDict().check_all_inputs(input_dict)

        # set job name as executable name and sample name
        job_name = f"{params['executable_name']}-{sample}"

        # call dx run to start jobs
        print(f"Calling {params['executable_name']} ({executable}) on sample {sample}")

        if input_dict.keys:
            print(f'Input dict: {PPRINT(input_dict)}')

        job_id = self.call_dx_run(
            executable=executable,
            job_name=job_name,
            input_dict=input_dict,
            output_dict=output_dict,
            prev_jobs=dependent_jobs
        )

        if sample not in job_outputs_dict.keys():
            # create new dict to store sample outputs
            job_outputs_dict[sample] = {}

        # map analysis id to dx job id for sample
        job_outputs_dict[sample].update({params['analysis']: job_id})

        return job_outputs_dict


    def call_per_run(
        self, executable, params, config, out_folder,
        job_outputs_dict, executable_out_dirs, fastq_details) -> dict:
        """
        Populates input and output dicts from config for given workflow,
        returns dx job id and stores in dict to map workflow -> dx job id.

        Parameters
        ----------
        executable : str
            human readable name of dx executable (workflow-, app- or applet-)
        params : dict
            dictionary of parameters specified in config for running analysis
        config : dict
            low level assay config read from json file
        out_folder : str
            name of parent dir path
        job_outputs_dict : dict
            dictionary of previous job outputs
        executable_out_dirs : dict
            dict of analsysis stage to its output dir path, used to pass
            output of an analysis to input of another (i.e.
            analysis_1 : /path/to/output)
        fastq_details : list of tuples
            list with tuple per fastq containing (DNAnexus file id, filename)

        Returns
        -------
        job_outputs_dict : dict
            dictionary of analysis stages to dx job ids created
        """
        # select input and output dict from config for current workflow / app
        input_dict = config['executables'][executable]['inputs']
        output_dict = config['executables'][executable]['output_dirs']

        # create output directory structure in config
        ManageDict().populate_output_dir_config(executable, output_dict, out_folder)

        if params["process_fastqs"] is True:
            input_dict = ManageDict().add_fastqs(input_dict, fastq_details)

        # handle other inputs defined in config to add to inputs
        input_dict = ManageDict().add_other_inputs(
            input_dict=input_dict,
            dx_project_id=self.args.dx_project_id,
            executable_out_dirs=executable_out_dirs
        )

        # check any inputs dependent on previous job outputs to add
        input_dict = ManageDict().link_inputs_to_outputs(
            job_outputs_dict=job_outputs_dict,
            input_dict=input_dict,
            analysis=params["analysis"],
            per_sample=False
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

        # passing all samples to workflow
        print(f'Calling {params["name"]} for all samples')
        job_id = self.call_dx_run(
            executable=executable,
            job_name=params['executable_name'],
            input_dict=input_dict,
            output_dict=output_dict,
            prev_jobs=dependent_jobs
        )

        PPRINT(input_dict)
        PPRINT(output_dict)

        # map workflow id to created dx job id
        job_outputs_dict[params['analysis']] = job_id

        return job_outputs_dict


class DXManage():
    """
    Methods for generic handling of dx related things
    """
    def __init__(self, args) -> None:
        self.args = args


    def get_json_configs(self) -> list:
        """
        Query path in DNAnexus for json config files fo each assay, returning
        highest version available for each assay code.

        ASSAY_CONFIG_PATH comes from the app config file sourced to the env.

        Returns
        -------
        dict
            dict of dicts of configs, one per assay
        """
        config_path = os.environ.get('ASSAY_CONFIG_PATH', '')

        # check for valid project:path structure
        assert re.match(r'project-[\d\w]*:/.*', config_path), Slack().send(
            f'ASSAY_CONFIG_PATH from config appears invalid: {config_path}'
        )

        project, path = config_path.split(':')

        files = list(dx.find_data_objects(
            name="*.json",
            name_mode='glob',
            project=project,
            folder=path
        ))

        # sense check we find config files
        assert files, Slack().send(
            f"No config files found in given path: {project}:{path}")

        all_configs = {}

        for file in files:
            current_config = json.loads(
                dx.bindings.dxfile.DXFile(
                    project=file['project'], dxid=file['id']).read())

            assay_code = current_config.get('assay_code')
            current_version = current_config.get('version')

            # more sense checking there's an assay and version
            assert assay_code, Slack().send(
                f"No assay code found in config file: {file}")
            assert current_version, Slack().send(
                f"No version found in config file: {file}")

            if all_configs.get(assay_code):
                # config for assay already found, check version if newer
                present_version = all_configs.get(assay_code).get('version')
                if present_version > current_version:
                    continue

            # add config to dict if not already present or newer one found
            all_configs[assay_code] = current_config

        print(f"Found config files for assays: {', '.join(all_configs.keys())}")

        return all_configs


    def find_dx_project(self, project_name) -> Union[None, str]:
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
        """
        dx_projects = list(dx.bindings.search.find_projects(
            name=project_name, limit=1
        ))

        if not dx_projects:
            # found no project, return None and create one in
            # get_or_create_dx_project()
            return None

        return dx_projects[0]['id']


    def get_or_create_dx_project(self, config):
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
                description="This project was automatically created by eggd_conductor"
            )
            print(
                f'Created new project for output: {output_project} ({project_id})'
            )
        else:
            print(f'Using existing found project: {output_project} ({project_id})')

        users = config.get('users')
        if users:
            # users specified in config to grant access to project
            for user, access_level in users.items():
                dx.bindings.dxproject.DXProject(dxid=project_id).invite(
                    user, access_level, send_email=False
                )
                print(f"Granted {access_level} priviledge to user {user}")

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
                print(f'Created output folder: {dx_folder}')
                return dx_folder
            else:
                # folder already exists, increase _i suffix on folder name
                # and check again
                print(f'{dx_folder} already exists, incrementing suffix integer')
                continue

        # got to end of loop, highly unlikely we would ever run this many in a
        # project but catch it here to stop some ambiguous downstream error
        raise RuntimeError(
            "Found 100 output directories in project, exiting now as "
            "there is likely an issue in the project."
        )


    def get_bcl2fastq_details(self, job_id) -> list:
        """
        Given job ID for bcl2fastq, return a list of the fastq file IDs

        Parameters
        ----------
        job_id : str
            job ID of bcl2fastq job

        Returns
        -------
        fastq_ids : list
            list of tuples with fastq file IDs and file name
        """
        print(f"Getting fastqs from given bcl2fastq job: {job_id}")
        bcl2fastq_job = dx.bindings.dxjob.DXJob(dxid=job_id).describe()
        bcl2fastq_project = bcl2fastq_job['project']
        bcl2fastq_folder = bcl2fastq_job['folder']

        # find all fastqs from bcl2fastq job, return list of dicts with details
        fastq_details = list(dx.search.find_data_objects(
            name="*.fastq*", name_mode="glob", project=bcl2fastq_project,
            folder=bcl2fastq_folder, describe=True
        ))
        # build list of tuples with fastq name and file ids
        fastq_details = [
            (x['id'], x['describe']['name']) for x in fastq_details
        ]
        # filter out Undetermined fastqs
        fastq_details = [
            x for x in fastq_details if not x[1].startswith('Undetermined')
        ]

        print(f'fastqs parsed from bcl2fastq job {job_id}')
        print(''.join([f'\t{x}\n' for x in fastq_details]))

        return fastq_details
