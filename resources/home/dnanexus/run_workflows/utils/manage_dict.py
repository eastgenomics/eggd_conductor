from pathlib import Path
import re
from typing import Generator

import dxpy as dx


class ManageDict():
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
        if not isinstance(input_dict, bool):
            if check_key == True:
                for i in getattr(input_dict, 'keys', lambda:input_dict)():
                    if isinstance(i, str) and identifier in i:
                        yield i
                    if isinstance(input_dict, dict):
                        yield from ManageDict().find_job_inputs(
                            identifier, input_dict[i], check_key=check_key
                        )
                    elif isinstance(input_dict, list) or isinstance(input_dict, set):
                        for item in input_dict:
                            yield from ManageDict().find_job_inputs(
                                identifier, item, check_key=check_key
                            )
            else:
                for i in getattr(input_dict, 'values', lambda:input_dict)():
                    if isinstance(i, str):
                        if identifier in i:
                            yield i
                    elif i:
                        yield from ManageDict().find_job_inputs(
                            identifier, i, check_key=check_key
                        )


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
            # sample not specified => use all fastqs
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
            self, input_dict, dx_project_id,
            executable_out_dirs, sample=None) -> dict:
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

        project_name = dx.api.project_describe(
            dx_project_id, input_params={'fields': {'name': True}}).get('name')

        self.replace_job_inputs(input_dict, 'INPUT-SAMPLE-NAME', sample)
        self.replace_job_inputs(input_dict, 'INPUT-dx_project_id', dx_project_id)
        self.replace_job_inputs(input_dict, 'INPUT-dx_project_name', project_name)
        self.replace_job_inputs(input_dict, 'INPUT-upload_tars', upload_tars)

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
            # job and not all instances of the executable for all samples
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
                    workflow_details = dx.api.workflow_describe(executable)
                    stage_app_id = [
                        (x['id'], x['executable'])
                        for x in workflow_details['stages']
                        if x['id'] == stage
                    ]
                    if stage_app_id:
                        # get applet id for given stage id
                        stage_app_id = stage_app_id[0][1]
                        applet_details = dx.api.workflow_describe(stage_app_id)
                        app_name = applet_details['name']
                    else:
                        # not found app ID for stage, going to print message
                        # and continue with using stage id
                        print('Error finding applet ID for naming output dir')
                        app_name = stage
                elif 'app-' or 'applet-' in executable:
                    app_details = dx.api.workflow_describe(executable)
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
