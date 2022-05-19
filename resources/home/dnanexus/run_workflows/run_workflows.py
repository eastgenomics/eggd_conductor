"""
Using a JSON config, calls all workflows / apps defined in config for
given samples.

Handles correctly interpreting and parsing inputs, defining output projects
and directory structures, and linking up outputs of jobs to inputs of
subsequent jobs.

See readme for full documentation of how to structure the config file and what
inputs are valid.

Jethro Rainford 210902
"""
import argparse
from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
import pprint
import re
import sys
from typing import Generator, Union

import dxpy


PPRINT = pprint.PrettyPrinter(indent=4).pprint


class manageDict():
    """
    Methods to handle parsing and populating input and output dictionaries
    """

    def find_job_inputs(self, identifier, input_dict, check_key) -> Generator:
        """
        Recursive function to find all values in arbitrarialy structured dict
        with identifying prefix, these require replacing with appropriate
        job output file ids. This funtion is used when needing to link inputs
        to outputs and for adding dependent jobs to new analyses.

        Parameters
        ----------
        identifier : str
            field to check for existence for in dict
        input_dict : dict
            dict of input parameters for calling workflow / app
        check_key : bool
            sets if to check for identifier in keys or values of dict

        Yields
        ------
        value : str
            match of identifier in given dict
        """
        for key, value in input_dict.items():
            # set field to check for identifier to either key or value
            if check_key is True:
                check_field = key
            else:
                check_field = value

            if isinstance(value, dict):
                yield from self.find_job_inputs(identifier, value, check_key)
            if isinstance(value, list):
                # found list of dicts -> loop over them
                for list_val in value:
                    yield from self.find_job_inputs(identifier, list_val, check_key)
            if isinstance(value, bool):
                # stop it breaking on booleans
                continue
            if identifier in check_field:
                # found input to replace
                yield value


    def replace_job_inputs(self, input_dict, job_input, dx_id):
        """
        Recursively traverse through nested dictionary and replace any matching
        job_input with given DNAnexus job/file/project id

        Parameters
        ----------
        input_dict : dict
            dict of input parameters for calling workflow / app
        job_input : str
            input key in `input_dict` to replace (i.e. INPUT-s left to replace)
        dx_id : str
            id of DNAnexus object to link input to
        """
        for key, val in input_dict.items():
            if isinstance(val, dict):
                # found a dict, continue
                self.replace_job_inputs(val, job_input, dx_id)
            if isinstance(val, list):
                # found list of dicts, check each dict
                for list_val in val:
                    self.replace_job_inputs(list_val, job_input, dx_id)
            if val == job_input:
                # replace analysis_ with correct job id
                input_dict[key] = dx_id


    def add_fastqs(self, input_dict, fastq_details, sample=None) -> dict:
        """
        If process_fastqs set to true, function is called to populate input
        dict with appropriate fastq file ids.

        If running per_sample, sample will be specified and the fastqs filtered
        for just those corresponding to the given sample. If not, all fastqs in
        `fastq_details` will be used (i.e. in a multi app / workflow)

        Parameters
        ----------
        input_dict : dict
            dict of input parameters for calling workflow / app
        fastq_details : list of tuples
            list with tuple per fastq containing (DNAnexus file id, filename)
        sample : str, default None
            optional, sample name used to filter list of fastqs

        Returns
        -------
        input_dict : dict
            dict of input parameters for calling workflow / app

        Raises
        ------
        AssertionError
            Raised when unequal number of R1 and R2 fastqs found
        """
        sample_fastqs = []

        if sample:
            sample_regex = re.compile(
                rf'{sample}_[A-za-z0-9]*_L00[0-9]_R[1,2]_001.fastq(.gz)?'
            )
            for fastq in fastq_details:
                # sample specified => running per sample, if not using
                # all fastqs find fastqs for given sample
                match = re.search(sample_regex, fastq[1])

                if match:
                    sample_fastqs.append(fastq)

            # ensure some fastqs found
            assert sample_fastqs, f'No fastqs found for {sample}'
        else:
            # use all fastqs
            sample_fastqs = fastq_details

        # fastqs should always be named with R1/2_001
        r1_fastqs = [x for x in sample_fastqs if 'R1_001.fastq' in x[1]]
        r2_fastqs = [x for x in sample_fastqs if 'R2_001.fastq' in x[1]]

        print(f'Found {len(r1_fastqs)} R1 fastqs & {len(r2_fastqs)} R2 fastqs')

        assert len(r1_fastqs) == len(r2_fastqs), (
            f"Mismatched number of FastQs found.\n"
            f"R1: {r1_fastqs} \nR2: {r2_fastqs}"
        )

        for stage, inputs in input_dict.items():
            # check each stage in input config for fastqs, format
            # as required with R1 and R2 fastqs
            if inputs == 'INPUT-R1':
                r1_input = [{"$dnanexus_link": x[0]} for x in r1_fastqs]
                input_dict[stage] = r1_input

            if inputs == 'INPUT-R2':
                r2_input = [{"$dnanexus_link": x[0]} for x in r2_fastqs]
                input_dict[stage] = r2_input

            if inputs == 'INPUT-R1-R2':
                # stage requires all fastqs, build one list of dicts
                r1_r2_input = []
                r1_r2_input.extend([{"$dnanexus_link": x[0]} for x in r1_fastqs])
                r1_r2_input.extend([{"$dnanexus_link": x[0]} for x in r2_fastqs])
                input_dict[stage] = r1_r2_input

        return input_dict


    def add_other_inputs(
            self, input_dict, dx_project_id, executable_out_dirs, sample=None) -> dict:
        """
        Generalised function for adding other INPUT-s, currently handles
        parsing: workflow output directories, sample name, project id and
        project name.

        Parameters
        ----------
        input_dict : dict
            dict of input parameters for calling workflow / app
        dx_project_id : str
            DNAnexus ID of project to run analysis
        executable_out_dirs : dict
            dict of analsysis stage to its output dir path, used to pass output of
            an analysis to input of another (i.e. analysis_1 : /path/to/output)
        sample : str, default None
            optional, sample name used to filter list of fastqs

        Returns
        -------
        input_dict : dict
            dict of input parameters for calling workflow / app

        Raises
        ------
        KeyError
            Raised when no output dir has been given where a downsteam analysis
            requires it as an input
        """
        # first checking if any INPUT- in dict to fill, if not return
        other_inputs = set(list(self.find_job_inputs(
            'INPUT-', input_dict, check_key=False
        )))

        if not other_inputs:
            # no other inputs found to replace
            return input_dict

        project_name = dxpy.api.project_describe(
            dx_project_id, input_params={'fields': {'name': True}}).get('name')

        self.replace_job_inputs(input_dict, 'INPUT-SAMPLE-NAME', sample)
        self.replace_job_inputs(input_dict, 'INPUT-dx_project_id', dx_project_id)
        self.replace_job_inputs(input_dict, 'INPUT-dx_project_name', project_name)

        # find and replace any out dirs
        regex = re.compile(r'^INPUT-analysis_[0-9]{1,2}-out_dir$')
        out_dirs = [re.search(regex, x) for x in self.other_inputs]
        out_dirs = [x.group(0) for x in out_dirs if x]

        for dir in out_dirs:
            # find the output directory for the given analysis
            analysis_out_dir = executable_out_dirs.get(
                dir.replace('INPUT-', '').replace('-out_dir', ''))

            if not analysis_out_dir:
                raise KeyError((
                    'Error trying to parse output directory to input dict.'
                    f'\nNo output directory found for given input: {dir}\n'
                    'Please check config to ensure job input is in the '
                    'format: INPUT-analysis_[0-9]-out_dir'
                ))

            # removing /output/ for now to fit to MultiQC
            analysis_out_dir = Path(analysis_out_dir).name
            self.replace_job_inputs(input_dict, dir, analysis_out_dir)

            return input_dict


    def get_dependent_jobs(self, params, job_outputs_dict, sample=None):
        """
        If app / workflow depends on previous job(s) completing these will be
        passed with depends_on = [analysis_1, analysis_2...].

        Get all job ids for given analysis to pass to dx run (i.e. if
        analysis_2 depends on analysis_1 finishing, get the dx id of the job
        to pass to current analysis).

        Parameters
        ----------
        params : dict
            dictionary of parameters specified in config for running analysis
        job_outputs_dict : dict
            dictionary of previous job outputs to search
        sample : str, default None
            optional, sample name used to limit searching for previous analyes

        Returns
        -------
        dependent_jobs : list
            list of dependent jobs found
        """
        if sample:
            # running per sample, assume we only wait on the samples previous
            # job and not all instances of the job for all samples
            job_outputs_dict = job_outputs_dict[sample]

        # check if job depends on previous jobs to hold till complete
        dependent_analysis = params.get("depends_on")
        dependent_jobs = []

        if dependent_analysis:
            for id in dependent_analysis:
                for job in self.find_job_inputs(id, job_outputs_dict, check_key=True):
                    # find all jobs for every analysis id
                    # (i.e. all samples job ids for analysis_X)
                    if job:
                        dependent_jobs.append(job)

        print(f'Dependent jobs found: {dependent_jobs}')

        return dependent_jobs


    def link_inputs_to_outputs(
            self, job_outputs_dict, input_dict, analysis, sample=None) -> dict:
        """
        Check input dict for 'analysis_', these will be for linking outputs of
        previous jobs and stored in the job_outputs_dict to input of next job.

        Parameters
        ----------
        job_outputs_dict : dict
            dictionary of previous job outputs to search
        input_dict : dict
            dict of input parameters for calling workflow / app
        analysis : str
            given analysis to check input dict of
        sample : str, default None
            optional, sample name used to limit searching for previous analyes

        Returns
        -------
        input_dict : dict
            dict of input parameters for calling workflow / app

        Raises
        ------
        KeyError
            Sample missing from `job_outputs_dict`
        RuntimeError
            Raised if an input is nota analysis id (i.e analysis_2)
        RuntimeError
            Raised if more than one job for a sample for a given analysis found
        ValueError
            No job id found for given analysis stage from `job_outputs_dict`
        """
        if analysis == "analysis_1":
            # first analysis => no previous outputs to link to inputs
            return input_dict

        if sample:
            # ensure we only use outputs for given sample
            try:
                job_outputs_dict = job_outputs_dict[sample]
            except KeyError:
                raise KeyError((
                    f'{sample} not found in output dict. This is most likely '
                    'from this being the first executable called and having '
                    'a misconfigured input section in config (i.e. misspelt '
                    'input) that should have been parsed earlier. Check '
                    f'config and try again. Input dict given: {input_dict}'
                ))

        # search input dict for job ids to add
        inputs = list(self.find_job_inputs(
            'analysis_', input_dict, check_key=True))

        if not inputs:
            # no inputs found to replace
            return input_dict

        for job_input in inputs:
            # for each input, use the analysis id to get the job id containing
            # the required output from the job outputs dict
            match = re.search(r'^analysis_[0-9]{1,2}$', job_input)
            if not match:
                # doesn't seem to be a valid app or worklfow, we cry
                raise RuntimeError((
                    f'{job_input} does not seem to be a valid analysis id, '
                    'check config and try again'
                ))

            analysis_id = match.group(0)

            # job output has analysis-id: job-id
            # select job id for appropriate analysis id
            job_id = [v for k, v in job_outputs_dict.items() if analysis_id == k]

            # job out should always(?) only have one output with given name,
            # exit for now if more found
            if len(job_id) > 1:
                raise RuntimeError(
                    f'More than one job found for {job_input}: {job_id}'
                )

            if not job_id:
                # this shouldn't happen as it will be caught with the regex but
                # double checking anyway
                raise ValueError((
                    f"No job id found for given analysis id: {job_input}, "
                    "please check that it has the same analysis as a previous "
                    "job in the config"
                ))

            # replace analysis id with given job id in input dict
            self.replace_job_inputs(input_dict, job_input, job_id[0])

        return input_dict


    def populate_output_dir_config(executable, output_dict, out_folder) -> dict:
        """
        Loops over stages in dict for output directory naming and adds
        worlflow app name.

        i.e. will be named /output/{out_folder}/{stage_name}/, where stage
        name is the human readable name of each stage defined in the config

        Parameters
        ----------
        executable : str
            human readable name of executable (workflow-, app-, applet-)
        output_dict : dict
            dictionary of output paths for each executable
        out_folder : str
            name of parent dir path

        Returns
        -------
        output_dict : dict
            populated dict of output directory paths
        """
        for stage, dir in output_dict.items():
            if "OUT-FOLDER" in dir:
                out_folder = out_folder.replace('/output/', '')
                dir = dir.replace("OUT-FOLDER", out_folder)
            if "APP-NAME" in dir:
                # use describe method to get actual name of app with version
                if 'workflow-' in executable:
                    workflow_details = dxpy.api.workflow_describe(executable)
                    stage_app_id = [
                        (x['id'], x['executable'])
                        for x in workflow_details['stages']
                        if x['id'] == stage
                    ]
                    if stage_app_id:
                        # get applet id for given stage id
                        stage_app_id = stage_app_id[0][1]
                        applet_details = dxpy.api.workflow_describe(stage_app_id)
                        app_name = applet_details['name']
                    else:
                        # not found app ID for stage, going to print message
                        # and continue with using stage id
                        print('Error finding applet ID for naming output dir')
                        app_name = stage
                elif 'app-' or 'applet-' in executable:
                    app_details = dxpy.api.workflow_describe(executable)
                    app_name = app_details['name']

                # add app/workflow name to output dir name
                dir = dir.replace("APP-NAME", app_name)
                output_dict[stage] = dir

        return output_dict


    def check_all_inputs(self, input_dict) -> None:
        """
        Check for any remaining INPUT-, should be none, if there is most likely
        either a typo in config or invalid input given => raise AssertionError

        Parameters
        ----------
        input_dict : dict
            dict of input parameters for calling workflow / app

        Raises
        ------
        AssertionError
            Raised if any 'INPUT-' are found in the input dict
        """
        # checking if any INPUT- in dict still present
        inputs = self.find_job_inputs('INPUT-', input_dict, check_key=False)
        _empty = object()

        assert next(inputs, _empty) == _empty, (
            f"Error: unparsed INPUT- still in config, please check readme for "
            f"valid input parameters. Input dict: {input_dict}"
        )


class DXExecute():
    """
    Methods for handling exeuction of apps / worklfows
    """

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
            dict of output directorie paths for each app
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
            job_handle = dxpy.bindings.dxworkflow.DXWorkflow(
                dxid=executable, project=args.dx_project_id
            ).run(
                workflow_input=input_dict, stage_folders=output_dict,
                rerun_stages=['*'], depends_on=prev_jobs, name=job_name
            )
        elif 'app-' in executable:
            job_handle = dxpy.bindings.dxapp.DXApp(dxid=executable).run(
                app_input=input_dict, project=args.dx_project_id,
                folder=output_dict[executable], ignore_reuse=True,
                depends_on=prev_jobs, name=job_name
            )
        elif 'applet-' in executable:
            job_handle = dxpy.bindings.dxapplet.DXApplet(dxid=executable).run(
                applet_input=input_dict, project=args.dx_project_id,
                folder=output_dict[executable], ignore_reuse=True,
                depends_on=prev_jobs, name=job_name
            )
        else:
            # doesn't appear to be valid workflow or app
            raise RuntimeError(
                f'Given executable id is not valid: {executable}'
            )

        job_details = job_handle.describe()
        job_id = job_details.get('id')

        print(f'Started analysis in project {args.dx_project_id}, job: {job_id}')

        with open('job_id.log', 'a') as fh:
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
        manageDict().populate_output_dir_config(executable, output_dict, out_folder)

        # check if stage requires fastqs passing
        if params["process_fastqs"] is True:
            input_dict = manageDict().add_fastqs(input_dict, fastq_details, sample)

        # find all jobs for previous analyses if next job depends on them
        if params.get("depends_on"):
            dependent_jobs = manageDict().get_dependent_jobs(
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
                    f'({sample}), ignoring and continuing.'
                ))

        # handle other inputs defined in config to add to inputs
        input_dict = manageDict().add_other_inputs(
            input_dict, args.dx_project_id, executable_out_dirs, sample)

        # check any inputs dependent on previous job outputs to add
        input_dict = manageDict().link_inputs_to_outputs(
            job_outputs_dict, input_dict, params["analysis"], sample=sample
        )

        # check that all INPUT- have been parsed in config
        manageDict().check_all_inputs(input_dict)

        # set job name as executable name and sample name
        job_name = f"{params['executable_name']}-{sample}"

        # call dx run to start jobs
        print(f"Calling {params['executable_name']} ({executable}) on sample {sample}")
        if input_dict and input_dict.keys:
            print(f'Input dict: {PPRINT(input_dict)}')

        job_id = self.call_dx_run(
            args, executable, job_name, input_dict,
            output_dict, dependent_jobs
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
        manageDict().populate_output_dir_config(executable, output_dict, out_folder)

        if params["process_fastqs"] is True:
            input_dict = manageDict().add_fastqs(input_dict, fastq_details)

        # handle other inputs defined in config to add to inputs
        input_dict = manageDict().add_other_inputs(
            input_dict, args.dx_project_id, executable_out_dirs)

        # check any inputs dependent on previous job outputs to add
        input_dict = manageDict().link_inputs_to_outputs(
            job_outputs_dict, input_dict, params["analysis"]
        )

        # find all jobs for previous analyses if next job depends on them
        if params.get("depends_on"):
            dependent_jobs = manageDict().get_dependent_jobs(params, job_outputs_dict)
        else:
            dependent_jobs = []

        # check that all INPUT- have been parsed in config
        manageDict().check_all_inputs(input_dict)

        # passing all samples to workflow
        print(f'Calling {params["name"]} for all samples')
        job_id = self.call_dx_run(
            args, executable, params['executable_name'], input_dict,
            output_dict, dependent_jobs
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
        dx_projects = list(dxpy.bindings.search.find_projects(
            name=project_name, limit=1
        ))

        if len(dx_projects) == 0:
            # found no project, return None and create one in
            # get_or_create_dx_project()
            return None

        return dx_projects[0]['id']


    def get_or_create_dx_project(self, config):
        """
        Create new project in DNAnexus if one with given name doesn't
        already exist. Adds project ID to global args namespace.

        Parameters
        ----------
        config : dict
            low level assay config read from json file
        """
        if args.development:
            prefix = f'003_{datetime.now().strftime("%y%m%d")}'
        else:
            prefix = '002'

        output_project = f'{prefix}_{args.run_id}_{args.assay_code}'

        project_id = self.find_dx_project(output_project)

        if not project_id:
            # create new project and capture returned project id and store
            project_id = dxpy.bindings.dxproject.DXProject().new(
                name=output_project,
                summary=f'Analysis of run {args.run_id} with {args.assay_code}',
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
                dxpy.bindings.dxproject.DXProject(dxid=project_id).invite(
                    user, access_level, send_email=False
                )

        args.dx_project_id = project_id

        with open('run_workflows_output_project.log', 'w') as fh:
            # record project used to send slack notification
            fh.write(f'{output_project} ({project_id})')


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
                dxpy.api.project_list_folder(
                    args.dx_project_id,
                    input_params={"folder": dx_folder, "only": "folders"},
                    always_retry=True
                )
            except dxpy.exceptions.ResourceNotFound:
                # can't find folder => create one
                dxpy.api.project_new_folder(
                    args.dx_project_id, input_params={
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
        bcl2fastq_job = dxpy.bindings.dxjob.DXJob(dxid=job_id).describe()
        bcl2fastq_project = bcl2fastq_job['project']
        bcl2fastq_folder = bcl2fastq_job['folder']

        # find all fastqs from bcl2fastq job, return list of dicts with details
        fastq_details = list(dxpy.search.find_data_objects(
            name="*.fastq*", name_mode="glob", project=bcl2fastq_project,
            folder=bcl2fastq_folder, describe=True
        ))
        # Build list of tuples with fastq name and file ids
        fastq_details = [
            (x['id'], x['describe']['name']) for x in fastq_details
        ]

        return fastq_details


def time_stamp() -> str:
    """
    Returns string of date & time formatted as YYMMDD_HHMM

    Returns
    -------
    str
        String of current date and time as YYMMDD_HHMM
    """
    return datetime.now().strftime("%Y%m%d_%H%M")


def load_config() -> dict:
    """
    Read in given config json to dict from args.NameSpace

    Raises
    ------
    RuntimeError: raised when a non-json file passed as config

    Returns
    -------
    config : dict
        dictionary of loaded json file
    """
    if not args.config_files.endswith('.json'):
        # sense check a json passed
        raise RuntimeError('Error: invalid config passed - not a json file')

    with open(args.config_file) as file:
        config = json.load(file)

    return config


def load_test_data() -> list:
    """
    Read in file ids of fastqs and sample names from test_samples file to test
    calling workflows

    Returns
    -------
    fastq_details : list of tuples
        list with tuple per fastq containing (DNAnexus file id, filename)

    """
    with open(args.test_samples) as f:
        fastq_details = f.read().splitlines()

    fastq_details = [(x.split()[0], x.split()[1]) for x in fastq_details]

    return fastq_details


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments

    Returns
    -------
    args : Namespace
        Namespace of passed command line argument inputs
    """
    parser = argparse.ArgumentParser(
        description="Trigger workflows from given config file"
    )

    parser.add_argument(
        '--config_file', required=True
    )
    parser.add_argument(
        '--samples', required=True, nargs='+',
        help='list of sample names to run analysis on'
    )
    parser.add_argument(
        '--dx_project_id', required=False,
        help=(
            'DNAnexus project ID to use to run analysis in, '
            'if not specified will create one named 002_{run_id}_{assay_code}'
        )
    )
    parser.add_argument(
        '--run_id',
        help='id of run parsed from sentinel file'
    )
    parser.add_argument(
        '--assay_code',
        help='assay code, used for naming outputs'
    )
    parser.add_argument(
        '--development', '-d', action='store_true',
        help='Created project will be prefixed with 003 instead of 002.'
    )
    parser.add_argument(
        '--bcl2fastq_id',
        help='id of job from running bcl2fastq (if run)'
    )
    parser.add_argument(
        '--fastqs',
        help='comma separated string of fastq file ids for starting analysis'
    )
    parser.add_argument(
        '--upload_tars', action='store_true',
        help='pass all input tar file ids as input to executable'
    )
    parser.add_argument(
        '--test_samples',
        help=(
            'for test use only. Pass in file with 1 sample per line '
            'specifing file-id of fastq and sample name'
        )
    )

    args = parser.parse_args()

    # turn comma separated str to python list
    if args.samples:
        args.samples = [
            x.replace(' ', '') for x in args.samples.split(',') if x
        ]
    if args.fastqs:
        args.fastqs = [x.replace(' ', '') for x in args.fastqs.split(',') if x]

    return args


def main():
    """
    Main function to run apps and workflows
    """
    global args
    args = parse_args()

    config = load_config()
    run_time = time_stamp()

    fastq_details = []
    upload_tars = []

    # log file of all jobs run, used in case of failing to launch all
    # downstream analysis to be able to terminate all analyses
    open('job_id.log', 'w').close()

    if not args.dx_project_id:
        # output project not specified, create new one from run id
        DXManage().get_or_create_dx_project(config)

    # set context to project for running jobs
    dxpy.set_workspace_id()

    if args.bcl2fastq_id:
        # get details of job that ran to perform demultiplexing to get
        # fastq file ids
        fastq_details = DXManage.get_bcl2fastq_details(job_id=args.bcl2fastq_id)
    elif args.fastqs:
        # call describe on files to get name and build list of tuples of
        # (file id, name)
        for fastq_id in args.fastqs:
            fastq_name = dxpy.api.file_describe(
                fastq_id, input_params={'fields': {'name': True}}
            )
            fastq_name = fastq_name['name']
            fastq_details.append((fastq_id, fastq_name))
    elif args.upload_tars:
        # turn file ids into array input
        upload_tars = [{"$dnanexus_link": x} for x in args.upload_tars]
    else:
        # bcl2fastq wasn't run => we have either a list of fastqs being passed,
        # this is for tso500 or something else weird this is going to need some
        # thought and clever handling to know what is being passed
        fastq_details = []

        # test data - myeloid sample
        if args.test_samples:
            fastq_details = load_test_data()

    # sense check per_sample defined for all workflows / apps before starting
    # we want this explicitly defined for everything to ensure it is
    # launched correctly
    for executable, params in config['executables'].items():
        assert 'per_sample' in params.keys(), (
            f"per_sample key missing from {executable} in config, check config"
            "and re-run"
        )

    # dict to add all stage output names and file ids for every sample to,
    # used to pass correct file ids to subsequent worklow/app calls
    job_outputs_dict = {}

    # storing output folders used for each workflow/app, might be needed to
    # store data together / access specific dirs of data
    executable_out_dirs = {}

    for executable, params in config['executables'].items():
        # for each workflow/app, check if its per sample or all samples and
        # run correspondingly
        print(f'\nConfiguring {executable} to start jobs')

        # create output folder for workflow, unique by datetime stamp
        out_folder = f'/output/{params["name"]}-{run_time}'
        out_folder = DXManage().create_dx_folder(out_folder)
        executable_out_dirs[params['analysis']] = out_folder

        params['executable_name'] = dxpy.api.app_describe(
            executable).get('name')

        if params['per_sample'] is True:
            # run workflow / app on every sample
            print(f'\nCalling {params["name"]} per sample')

            # loop over given sample and call workflow
            for idx, sample in enumerate(args.samples):
                print(
                    f'\nStarting analysis for {sample} - '
                    f'({idx}/{len(args.samples)})'
                )
                job_outputs_dict = DXExecute().call_per_sample(
                    executable, params, sample, config, out_folder,
                    job_outputs_dict, executable_out_dirs, fastq_details
                )

        elif params['per_sample'] is False:
            # run workflow / app on all samples at once
            # need to explicitly check if False vs not given, must always be
            # defined to ensure running correctly
            job_outputs_dict = DXExecute().call_per_run(
                executable, params, config, out_folder, job_outputs_dict,
                executable_out_dirs, fastq_details
            )
        else:
            # per_sample is not True or False, exit
            raise ValueError(
                f"per_sample declaration for {executable} is not True or "
                f"False ({params['per_sample']}). Please check the config"
            )

    print("Completed calling jobs")


if __name__ == "__main__":
    main()
