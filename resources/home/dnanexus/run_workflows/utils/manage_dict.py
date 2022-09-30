from copy import deepcopy
from pprint import PrettyPrinter
import re

import dxpy as dx
from flatten_json import flatten, unflatten_list

from utils.utils import Slack


PPRINT = PrettyPrinter(indent=1).pprint


class ManageDict():
    """
    Methods to handle parsing and populating input and output dictionaries
    """
    def search(
        self, identifier, input_dict, check_key, return_key) -> list:
        """
        Searches nested dictionary for given identifier string in either
        the dict keys or values, and returns the key or value of the match.

        Parameters
        ----------
        identifier : str
            field to check for existence for in dict
        input_dict : dict
            dict of input parameters for calling workflow / app
        check_key : bool
            sets if to check for identifier in keys or values of dict
        return_key : bool
            sets if to return key (True) or value (False)

        Returns
        ------
        list : list of unique keys or values containing identifier
        """
        # flatten to single level dict with keys as paths to end values
        # for easy searching
        flattened_dict = flatten(input_dict, '|')
        found = []

        for key, value in flattened_dict.items():
            if check_key:
                to_check = key
            else:
                to_check = value

            if isinstance(to_check, bool) or not to_check:
                # to_check is True, False or None
                continue

            match = re.search(rf'[^|]*{identifier}[^|]*', to_check)
            if match:
                if return_key:
                    found.append(match.group())
                else:
                    found.append(value)

        return list(set(found))


    def replace(
        self, input_dict, to_replace, replacement,
        search_key, replace_key) -> list:
        """
        Recursively traverse through nested dictionary and replace any matching
        job_input with given DNAnexus job/file/project id

        Parameters
        ----------
        input_dict : dict
            dict of input parameters for calling workflow / app
        to_replace : str
            input key in `input_dict` to replace (i.e. INPUT-s left to replace)
        replacement : str
            id of DNAnexus object to link input to
        search_key : bool
            determines if to search dictionary keys or values
        replace_key : bool
            determines if to replace keys or values

        Returns
        -------
        dict : dict with modified keys or values
        """
        matches = self.search(
            identifier=to_replace,
            input_dict=input_dict,
            check_key=search_key,
            return_key=replace_key
        )

        if not matches:
            return input_dict

        flattened_dict = flatten(input_dict, '|')
        new_dict = {}

        for key, value in flattened_dict.items():
            if replace_key:
                replacing = key
            else:
                replacing = value

            # track if we found a match and already key added to output dict
            added_key = False

            if not isinstance(replacing, bool) and replacing:
                for match in matches:
                    if not match in replacing:
                        continue

                    # match is in this key / value => replace
                    added_key = True
                    if replace_key:
                        new_key = re.sub(match, replacement, replacing)
                        new_dict[new_key] = value
                    else:
                        new_dict[key] = replacement

                    break

            if not added_key:
                # match not in this key - value => add original pair back
                new_dict[key] = value

        return unflatten_list(new_dict, '|')


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
            assert sample_fastqs, Slack().send(f'No fastqs found for {sample}')
        else:
            # sample not specified => use all fastqs
            sample_fastqs = fastq_details

        # fastqs should always be named with R1/2_001
        r1_fastqs = [x for x in sample_fastqs if 'R1_001.fastq' in x[1]]
        r2_fastqs = [x for x in sample_fastqs if 'R2_001.fastq' in x[1]]

        print(f'Found {len(r1_fastqs)} R1 fastqs & {len(r2_fastqs)} R2 fastqs')

        # sense check we have R2 fastqs before across all samples (i.e.
        # checking this isn't single end sequencing) before checking we
        # have equal numbers for the current sample
        all_r2_fastqs = [x for x in fastq_details if 'R2_001.fastq' in x[1]]

        if all_r2_fastqs:
            assert len(r1_fastqs) == len(r2_fastqs), Slack().send(
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
            self, input_dict, args, executable_out_dirs,
            sample=None, sample_prefix=None) -> dict:
        """
        Generalised function for adding other INPUT-s, currently handles
        parsing:
            - workflow output directories (INPUT-analysis[0-9]{1,2}-out_dir)
            - sample name (INPUT-SAMPLE-NAME)
            - sample prefix (INPUT-SAMPLE-PREFIX)
            - project id (INPUT-dx_project_id)
            - project name (INPUT-dx_project_name)
            - parent output directory (INPUT-parent_out_dir)

        Parameters
        ----------
        input_dict : dict
            dict of input parameters for calling workflow / app
        args : argparse.Namespace
            namespace object of passed cmd line arguments
        executable_out_dirs : dict
            dict of analsysis stage to its output dir path, used to pass output of
            an analysis to input of another (i.e. analysis_1 : /path/to/output)
        sample : str, default None
            optional, sample name used to filter list of fastqs
        sample_prefix : str, default None
            optional, prefix of sample name split with delimeter from config

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
        print('Adding other inputs')
        print('Input dict passed to check:')
        PPRINT(input_dict)

        # first checking if any INPUT- in dict to fill
        other_inputs = self.search(
            identifier='INPUT-',
            input_dict=input_dict,
            check_key=False,
            return_key=False
        )

        if not other_inputs:
            return input_dict
        else:
            print(f'Other inputs found to replace: {other_inputs}')

        # removing /output/ for now to fit to MultiQC
        args.parent_out_dir = args.parent_out_dir.replace('/output/', '')

        # mapping of potential user defined keys and variables to replace with
        to_replace = [
            ('INPUT-SAMPLE-NAME', sample),
            ('INPUT-SAMPLE-PREFIX', sample_prefix),
            ('INPUT-dx_project_id', args.dx_project_id),
            ('INPUT-dx_project_name', args.dx_project_name),
            ('INPUT-parent_out_dir', args.parent_out_dir)
        ]

        for pair in to_replace:
            if pair[1]:
                input_dict = self.replace(
                    input_dict=input_dict,
                    to_replace=pair[0],
                    replacement=pair[1],
                    search_key=False,
                    replace_key=False
                )

        # find and replace any out dirs
        regex = re.compile(r'^INPUT-analysis_[0-9]{1,2}-out_dir$')
        out_dirs = [re.search(regex, x) for x in other_inputs]
        out_dirs = [x.group(0) for x in out_dirs if x]

        print(f'Out dirs found to replace: {out_dirs}')

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

            input_dict = self.replace(
                input_dict=input_dict,
                to_replace=dir,
                replacement=analysis_out_dir,
                search_key=False,
                replace_key=False
            )

        print('Input dict after adding other inputs:')
        PPRINT(input_dict)

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
            job_outputs_dict_copy = deepcopy(job_outputs_dict)
            job_outputs_dict = job_outputs_dict[sample]

        # check if job depends on previous jobs to hold till complete
        dependent_analyses = params.get("depends_on")
        dependent_jobs = []

        if dependent_analyses:
            for analysis_id in dependent_analyses:
                # find all jobs for every analysis id
                # (i.e. all samples job ids for analysis_X)
                job_ids = self.search(
                    identifier=analysis_id,
                    input_dict=job_outputs_dict,
                    check_key=True,
                    return_key=False
                )
                if job_ids:
                    for job in job_ids:
                        dependent_jobs.append(job)
                else:
                    # didn't find a job ID for the given analysis_X,
                    # this is possibly due to the analysis being per
                    # run and not in the samples job dict => check if
                    # it is in the main job_outputs_dict keys
                    job = job_outputs_dict_copy.get(analysis_id)
                    if job:
                        # found ID in root of jobs dict => found per
                        # run job to wait on
                        dependent_jobs.append(job)

        print(f'Dependent jobs found: {dependent_jobs}')

        return dependent_jobs


    def link_inputs_to_outputs(
            self, job_outputs_dict, input_dict, analysis,
            per_sample, input_filter_dict=None, sample=None) -> dict:
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
        per_sample : bool
            if the given executable is running per sample or not, if not then
            all job IDs for the linked analysis will be gathered and used
            as input
        input_filter_dict : dict
            mapping of 'stage_ID.inputs' to a list of regex pattern(s) to
            filter sample IDs by
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
            Raised if an input is not an analysis id (i.e analysis_2)
        RuntimeError
            Raised if more than one job for a sample for a given analysis found
        ValueError
            No job id found for given analysis stage from `job_outputs_dict`
        """
        print("Searching input dict for inputs to link to outputs")
        print("Input dict before:")
        PPRINT(input_dict)

        if analysis == "analysis_1":
            # first analysis => no previous outputs to link to inputs
            return input_dict

        if sample:
            # ensure we only use outputs for given sample
            job_outputs_dict = job_outputs_dict.get(sample)
            if not job_outputs_dict:
                raise KeyError((
                    f'{sample} not found in output dict. This is most likely '
                    'from this being the first executable called and having '
                    'a misconfigured input section in config (i.e. misspelt '
                    'input) that should have been parsed earlier. Check '
                    f'config and try again. Input dict given: {input_dict}'
                ))
            print(f"Output dict for sample {sample}:")
            PPRINT(job_outputs_dict)

        # check if input dict has any analysis_X => need to link a previous job
        all_analysis_ids = self.search(
            identifier='analysis_',
            input_dict=input_dict,
            check_key=False,
            return_key=False
        )

        print(f"Found analyses to replace: {all_analysis_ids}")

        print("Input dictionary before modifying")
        PPRINT(input_dict)

        if not all_analysis_ids:
            # no inputs found to replace
            return input_dict

        for analysis_id in all_analysis_ids:
            # for each input, use the analysis id to get the job id containing
            # the required output from the job outputs dict
            if not re.search(r'^analysis_[0-9]{1,2}$', analysis_id):
                # doesn't seem to be a valid analysis_X
                raise RuntimeError((
                    f'{analysis_id} does not seem to be a valid analysis id, '
                    'check config and try again'
                ))

            if per_sample:
                # job_outputs_dict has analysis_X: job-id
                # select job id for appropriate analysis id
                job_id = [
                    v for k, v in job_outputs_dict.items() if analysis_id == k
                ]

                if not job_id:
                    # this shouldn't happen as it will be caught with
                    # the regex but double checking anyway
                    raise ValueError((
                        "No job id found for given analysis id: "
                        f"{analysis_id}, please check that it has the "
                        "same analysis as a previous job in the config"
                    ))

                # replace analysis id with given job id in input dict
                self.replace(
                    input_dict=input_dict,
                    to_replace=analysis_id,
                    replacementg=job_id[0],
                    search_key=False,
                    replace_key=False
                )
            else:
                # current executable is running on all samples => need to
                # gather all previous jobs for all samples and build input
                # array structure
                print("Job outputs dict to search")
                PPRINT(job_outputs_dict)

                job_ids = self.search(
                    identifier=analysis_id,
                    input_dict=job_outputs_dict,
                    check_key=True,
                    return_key=False
                )

                # sense check job IDs prev. launched for given analysis ID
                if not job_ids:
                    raise ValueError((
                        "No job id found for given analysis id: "
                        f"{analysis_id}, please check that it has the "
                        "same analysis as a previous job in the config"
                    ))

                print(f"Found job IDs to link as inputs: {job_ids}")

                # for each input, first check if given analysis_X is present
                # => need to link job IDs to the input. If true, turn that
                # input into an array and create one dict of input structure
                # per job for the given analysis_X found.
                for input_field, link_dict in input_dict.items():
                    for _, stage_input in link_dict.items():
                        if not analysis_id in stage_input.values():
                            # analysis id not present as any input
                            continue

                    # filter job outputs to search by sample name patterns
                    job_outputs_dict_copy = self.filter_job_outputs_dict(
                        stage=input_field,
                        outputs_dict=job_outputs_dict,
                        filter_dict=input_filter_dict
                    )

                    # gather all job IDs for current analysis ID
                    job_ids = self.search(
                        identifier=analysis_id,
                        input_dict=job_outputs_dict_copy,
                        check_key=True,
                        return_key=False
                    )

                    # copy input structure from input dict, turn into an array
                    # input and populate with a link to each job
                    stage_input_template = deepcopy(link_dict)
                    input_dict[input_field] = []
                    for job in job_ids:
                        stage_input_tmp = deepcopy(stage_input_template)
                        stage_input_tmp = self.replace(
                            input_dict=stage_input_tmp,
                            to_replace=analysis_id,
                            replacement=job,
                            search_key=False,
                            replace_key=False
                        )
                        input_dict[input_field].append(stage_input_tmp)

        print("Input dictionary after parsing inputs to outputs")
        PPRINT(input_dict)

        return input_dict


    def populate_output_dir_config(
            self, executable, exe_names, output_dict, out_folder) -> dict:
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
        for stage, dir in output_dict.items():
            if "OUT-FOLDER" in dir:
                # OUT-FOLDER => /output/{ASSAY}_{TIMESTAMP}
                dir = dir.replace("OUT-FOLDER", out_folder)
            if "APP-NAME" in dir or "WORKFLOW-NAME" in dir:
                app_name = exe_names[executable]['name']
                dir = dir.replace("APP-NAME", app_name)
            if "STAGE-NAME" in dir:
                app_name = exe_names[executable]['stages'][stage]
                dir = dir.replace("STAGE-NAME", app_name)

            # ensure we haven't accidentally got double slashes in path
            dir = dir.replace('//', '/')

            # ensure we don't end up with double /ouput if given in config and
            # using OUT-FOLDER
            dir = dir.replace('output/output', 'output')

            output_dict[stage] = dir

        print(f'Output dict for {executable}:')
        PPRINT(output_dict)

        return output_dict


    def filter_job_outputs_dict(
            self, stage, outputs_dict, filter_dict) -> dict:
        """
        Filter given dict of sample names -> job IDs to only keep job IDs
        of jobs for those sample(s) matching given pattern(s).

        Used to filter where downstream jobs need to only take the outputs
        of certain samples as input (i.e. gathering all output bam files but
        only needing those of a control sample)

        Parameters
        ----------
        stage : str
            stage ID to select patterns from filter_dict by
        outputs_dict : dict
            dict of sample IDs -> launched jobs
        filter_dict : dict
            mapping of stage_ID.inputs to a list of regex pattern(s) to
            filter sample IDs by

        Returns
        -------
        dict
            filtered jobs dict
        """
        if not filter_dict:
            # no filter pattens to apply
            return outputs_dict

        print(f'\nFiltering job outputs dict by sample name patterns for {stage}')
        print(f'\nJob outputs dict before filtering:')
        PPRINT(outputs_dict)
        print(f'Filter dict:')
        PPRINT(filter_dict)

        new_outputs = {}
        stage_match = False

        for filter_stage, filter_patterns in filter_dict.items():
            if stage == filter_stage:
                # current stage has filter(s) to apply
                stage_match = True
                for pattern in filter_patterns:
                    for sample, job in outputs_dict.items():
                        if re.search(pattern, sample):
                            new_outputs[sample] = job

        if not stage_match:
            # stage has no filters to apply => just return the outputs dict
            print(f'No filters to apply for stage: {stage}')
            return outputs_dict
        else:
            # there was a filter for given stage to apply, if no
            # matches were found against the given pattern(s) this
            # will be an empty dict
            print(f'Job outputs dict after filtering')
            PPRINT(new_outputs)

            return new_outputs


    def check_all_inputs(self, input_dict) -> None:
        """
        Check for any remaining INPUT- or analysis_, should be none.

        If there is most likely either a typo in config or invalid input
        given (or a bug 🙃) => raise AssertionError

        Parameters
        ----------
        input_dict : dict
            dict of input parameters for calling workflow / app

        Raises
        ------
        AssertionError
            Raised if any 'INPUT-' or 'analysis_' are found in the input dict
        """
        unparsed_inputs = self.search(
            'INPUT-', input_dict, check_key=False, return_key=False)

        assert not unparsed_inputs, Slack().send(
            f"unparsed `INPUT-` still in config, please check readme for "
            f"valid input parameters. \nInput dict:\n```{input_dict}```"
        )

        unparsed_inputs = self.search(
            'analysis_', input_dict, check_key=False, return_key=False)

        assert not unparsed_inputs, Slack().send(
            f"unparsed `analysis-` still in config, please check readme for "
            f"valid input parameters. \nInput dict:\n```{input_dict}```"
        )
