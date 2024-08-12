"""
Functions related to populating and formatting input and output
dictionaries for passing to dx run.
"""
from copy import deepcopy
import os
import re
import sys

from flatten_json import flatten, unflatten_list

sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from utils.utils import Slack, prettier_print


def search(identifier, input_dict, check_key, return_key) -> list:
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

        if isinstance(to_check, (bool, int, float)) or not to_check:
            # to_check is True, False, a number or None
            continue

        match = re.search(rf'[^|]*{identifier}[^|]*', to_check)
        if match:
            if return_key:
                found.append(match.group())
            else:
                found.append(value)

    return list(set(found))


def replace(
    input_dict, to_replace, replacement,
    search_key, replace_key
) -> dict:
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
    matches = search(
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

        if isinstance(replacing, str) and replacing:
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


def add_fastqs(input_dict, fastq_details, sample=None) -> dict:
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
    r1_fastqs = sorted(
        [x for x in sample_fastqs if 'R1_001.fastq' in x[1]],
        key=lambda x: x[1]
    )
    r2_fastqs = sorted(
        [x for x in sample_fastqs if 'R2_001.fastq' in x[1]],
        key=lambda x: x[1]
    )

    prettier_print(f'Found {len(r1_fastqs)} R1 fastqs & {len(r2_fastqs)} R2 fastqs')

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


def add_upload_tars(input_dict, upload_tars) -> dict:
    """
    Add list of upload tars parsed from sentinel record as input
    as defined by INPUT-UPLOAD_TARS

    Parameters
    ----------
    input_dict : dict
        dict of input parameters for calling workflow / app

    Returns
    -------
    input_dict : dict
        dict of input parameters for calling workflow / app
    """
    for app_input, value in input_dict.items():
        if value == 'INPUT-UPLOAD_TARS':
            input_dict[app_input] = upload_tars

    return input_dict


def add_other_inputs(
    input_dict, args, executable_out_dirs,
    sample=None, sample_prefix=None
) -> dict:
    """
    Generalised function for adding other INPUT-s, currently handles
    parsing:
        - workflow output directories (INPUT-analysis[0-9]{1,2}-out_dir)
        - sample name (INPUT-SAMPLE-NAME)
        - sample prefix (INPUT-SAMPLE-PREFIX)
        - project id (INPUT-dx_project_id)
        - project name (INPUT-dx_project_name)
        - parent output directory (INPUT-parent_out_dir)
        - samplesheet (INPUT-SAMPLESHEET)

    Parameters
    ----------
    input_dict : dict
        dict of input parameters for calling workflow / app
    args : argparse.Namespace
        namespace object of passed cmd line arguments
    executable_out_dirs : dict
        dict of analysis stage to its output dir path, used to pass output of
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
    prettier_print('\nAdding other inputs, input dict passed to check:')
    prettier_print(input_dict)

    # first checking if any INPUT- in dict to fill
    other_inputs = search(
        identifier='INPUT-',
        input_dict=input_dict,
        check_key=False,
        return_key=False
    )

    if not other_inputs:
        return input_dict
    else:
        prettier_print(f'\nOther inputs found to replace: {other_inputs}')

    # removing /output prefix for now to fit to MultiQC
    args.parent_out_dir = re.sub(r'^/output/', '', args.parent_out_dir)

    samplesheet = ""
    if os.environ.get('SAMPLESHEET_ID'):
        # get just the ID of samplesheet in case of being formatted as
        # {'$dnanexus_link': 'file_id'}
        match = re.search(r'file-[\d\w]*', os.environ.get('SAMPLESHEET_ID'))
        if match:
            samplesheet = match.group()

    # mapping of potential user defined keys and variables to replace with
    to_replace = [
        ('INPUT-SAMPLE-NAME', sample),
        ('INPUT-SAMPLE-PREFIX', sample_prefix),
        ('INPUT-dx_project_id', args.dx_project_id),
        ('INPUT-dx_project_name', args.dx_project_name),
        ('INPUT-parent_out_dir', args.parent_out_dir),
        ('INPUT-SAMPLESHEET', samplesheet)
    ]

    for pair in to_replace:
        if pair[1]:
            input_dict = replace(
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

    prettier_print(f'\nOut dirs found to replace: {out_dirs}')

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

        input_dict = replace(
            input_dict=input_dict,
            to_replace=dir,
            replacement=analysis_out_dir,
            search_key=False,
            replace_key=False
        )

    prettier_print('\nInput dict after adding other inputs:')
    prettier_print(input_dict)

    return input_dict


def get_dependent_jobs(params, job_outputs_dict, sample=None) -> list:
    """
    If app / workflow depends on previous job(s) completing these will be
    passed with depends_on = [analysis_1, analysis_2...].

    Get all job ids for given analysis to pass to dx run (i.e. if
    analysis_2 depends on analysis_1 finishing, get the dx id of the job
    to pass to current analysis).

    Example job_outputs_dict:

        {
            '2207155-22207Z0091-1-BM-MPD-MYE-M-EGG2': {
                'analysis_1': 'analysis-GGjgz0j4Bv4P8yqJGp9pyyv2',
                'analysis_3': 'job-GGjgyX04Bv44Vz151GGzFKgP'
            },
            'Oncospan-158-1-AA1-BBB-MYE-U-EGG2': {
                'analysis_1': 'analysis-GGjgz004Bv4P8yqJGp9pyyqb',
                'analysis_3': 'job-GGp69xQ4Bv45bk0y4kyVqvJ1'
            },
            'analysis_2': 'job-GGjgz1j4Bv48yF89GpZ6zkGz'
        }

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
    # get jobs in root of job outputs dict => just per run jobs
    per_run_jobs = {
        k: v for k, v in job_outputs_dict.items()
        if k.startswith('analysis_')
    }

    if sample:
        # running per sample, assume we only wait on the samples previous
        # job and not all instances of the given executable for all samples
        job_outputs_dict = job_outputs_dict.get(sample, {})

    # check if job depends on previous jobs to hold till complete
    dependent_analyses = params.get("depends_on")
    dependent_jobs = []

    if dependent_analyses:
        for analysis_id in dependent_analyses:
            # find all jobs for every analysis id
            # (i.e. all samples job ids for analysis_X)
            job_ids = search(
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
                job = per_run_jobs.get(analysis_id)
                if job:
                    # found ID in per run jobs dict => wait on completing
                    dependent_jobs.append(job)

    prettier_print(f'\nDependent jobs found: {dependent_jobs}')

    return dependent_jobs


def link_inputs_to_outputs(
    job_outputs_dict, input_dict, analysis, per_sample,
    input_filter_dict=None, sample=None
) -> dict:
    """
    Check input dict for 'analysis_', these will be for linking outputs of
    previous jobs and stored in the job_outputs_dict to input of next job.

    Parameters
    ----------
    job_outputs_dict : dict
        dictionary of previous job outputs to search
    analysis : str
        given analysis_X to check input dict of
    per_sample : bool
        if the given executable is running per sample or not, if not then
        all job IDs for the linked analysis will be gathered and used
        as input
    input_filter_dict : dict, default None
        (optional) mapping of 'stage_ID.inputs' to a list of regex pattern(s) to
        filter sample IDs by
    sample : str, default None
        (optional) sample name used to limit searching for previous analyses

    Returns
    -------
    input_dict : dict
        dict of input parameters for calling workflow / app

    Raises
    ------
    RuntimeError
        Raised if an input is not an analysis id (i.e analysis_2)
    RuntimeError
        Raised if more than one job for a sample for a given analysis found
    ValueError
        No job id found for given analysis stage from `job_outputs_dict`
    """
    prettier_print("\nSearching input dict for inputs to link to outputs")
    prettier_print("Input dict before:")
    prettier_print(input_dict)

    if analysis == "analysis_1":
        # first analysis => no previous outputs to link to inputs
        return input_dict

    if sample:
        # ensure we only use outputs for given sample
        sample_outputs = job_outputs_dict.get(sample, {})
        if not sample_outputs:
            print(
                f"Sample key {sample} not found in previous outputs, this "
                "is expected if all previous steps were only from per run "
                "jobs. Will continue with checking for analysis inputs."
            )

        # get any per run jobs to select from if an output is to be
        # parsed from there, these will be in the top level of the
        # job _outputs dict (i.e. {'analysis_1': "job-xxx"})
        per_run_outputs = {
            k: v for k, v  in job_outputs_dict.items()
            if k.startswith('analysis_')
        }

        job_outputs_dict = {**per_run_outputs, **sample_outputs}

        prettier_print(f"\nOutput dict for run & sample {sample}:")
        prettier_print(job_outputs_dict)

    # check if input dict has any analysis_X => need to link a previous job
    all_analysis_ids = search(
        identifier='analysis_',
        input_dict=input_dict,
        check_key=False,
        return_key=False
    )

    prettier_print(f"\nFound analyses to replace: {all_analysis_ids}")

    prettier_print("Input dictionary before modifying")
    prettier_print(input_dict)

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
            input_dict = replace(
                input_dict=input_dict,
                to_replace=analysis_id,
                replacement=job_id[0],
                search_key=False,
                replace_key=False
            )
        else:
            # current executable is running on all samples => need to
            # gather all previous jobs for all samples and build input
            # array structure
            prettier_print("\nJob outputs dict to search")
            prettier_print(job_outputs_dict)

            job_ids = search(
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

            prettier_print(f"\nFound job IDs to link as inputs: {job_ids}")

            # for each input, first check if given analysis_X is present
            # => need to link job IDs to the input. If true, turn that
            # input into an array and create one dict of input structure
            # per job for the given analysis_X found.
            for input_field, link_dict in input_dict.items():
                if not isinstance(link_dict, dict):
                    # input is not a dnanexus file or output link
                    continue
                for _, stage_input in link_dict.items():
                    if not isinstance(stage_input, dict):
                        # input is not a previous output format
                        continue
                    if not analysis_id in stage_input.values():
                        # analysis id not present as any input
                        continue

                    # filter job outputs to search by sample name patterns
                    job_outputs_dict_copy = filter_job_outputs_dict(
                        stage=input_field,
                        outputs_dict=job_outputs_dict,
                        filter_dict=input_filter_dict
                    )

                    # gather all job IDs for current analysis ID
                    job_ids = search(
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
                        stage_input_tmp = replace(
                            input_dict=stage_input_tmp,
                            to_replace=analysis_id,
                            replacement=job,
                            search_key=False,
                            replace_key=False
                        )
                        input_dict[input_field].append(stage_input_tmp)

    return input_dict


def populate_output_dir_config(
    executable, exe_names, output_dict, out_folder
) -> dict:
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

        # ensure we don't end up with double /output if given in config and
        # using OUT-FOLDER
        dir = dir.replace('output/output', 'output')

        output_dict[stage] = dir

    prettier_print(f'\nOutput dict for {executable}:')
    prettier_print(output_dict)

    return output_dict


def filter_job_outputs_dict(
    stage, outputs_dict, filter_dict
) -> dict:
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

    prettier_print(
        f'\nFiltering job outputs dict by sample name patterns for {stage}'
    )
    prettier_print(f'\nJob outputs dict before filtering: {outputs_dict}')
    prettier_print(f'\nFilter dict:{filter_dict}')

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
        prettier_print(f'\nNo filters to apply for stage: {stage}')
        return outputs_dict
    else:
        # there was a filter for given stage to apply, if no
        # matches were found against the given pattern(s) this
        # will be an empty dict
        prettier_print('\nJob outputs dict after filtering')
        prettier_print(new_outputs)

        return new_outputs


def check_input_classes(input_dict, input_classes) -> dict:
    """
    Check populated input dict to ensure that the types match what
    the app / workflow expect (i.e if input is array:file that a
    list is given)

    Parameters
    ----------
    executable : str
        dx ID of executable
    input_dict : dict
        dict of input parameters for calling workflow / app
    input_classes : dict
        mapping of current executable inputs -> expected types

    Returns
    -------
    dict
        input dict with classes correctly set (if required)

    Raises
    ------
    RuntimeError
        Raised when an input should be a single file but multiple have
        been found to provide and array built
    RuntimeError
        Raised when an input is non-optional and an empty list has been
        passed
    """
    prettier_print("\nChecking input classes are valid")
    prettier_print("Current input dict state:")
    prettier_print(input_dict)
    input_dict_copy = deepcopy(input_dict)

    prettier_print("\nExpected input classes:")
    prettier_print(input_classes)


    for input_field, configured_input in input_dict.items():
        input_details = input_classes.get(input_field)
        expected_class = input_details.get('class')
        optional = input_details.get('optional')

        if not expected_class in ['file', 'array:file']:
            # we only care about single files and arrays as they are the
            # only ones likely to be wrongly formatted
            continue

        if expected_class == 'array:file' and isinstance(configured_input, dict):
            # we expect a list and have a dict (i.e. only one file
            # being passed) => turn it into a list
            configured_input = [configured_input]

        if expected_class == 'file' and isinstance(configured_input, list):
            # input wants to be a single file and we have a list
            # if its just one => then use it
            # if its empty => could still be okay if input is optional,
            #   drop the input if so, else raise error
            # if more then something has gone wrong and we are sad
            if len(configured_input) == 0:
                if optional:
                    input_dict_copy.pop(input_field)
                    continue
                else:
                    raise RuntimeError(
                        "Non-optional input found and no input has been "
                        "provided or parsed as input.\nInput field: "
                        f"{input_field}\nInput found: {configured_input}"
                    )
            if len(configured_input) == 1:
                configured_input = configured_input[0]
            else:
                raise RuntimeError((
                    "Input expects to be a single file but multiple "
                    f"files were found and provided.\nInput field: "
                    f"{input_field}\nInput found: {configured_input}"
                ))
        input_dict_copy[input_field] = configured_input

    return input_dict_copy


def check_all_inputs(input_dict) -> None:
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
    unparsed_inputs = search(
        'INPUT-', input_dict, check_key=False, return_key=False)

    assert not unparsed_inputs, Slack().send(
        f"unparsed `INPUT-` still in config, please check readme for "
        f"valid input parameters. \nUnparsed input(s): `{unparsed_inputs}`"
    )

    unparsed_inputs = search(
        'analysis_', input_dict, check_key=False, return_key=False)

    assert not unparsed_inputs, Slack().send(
        f"unparsed `analysis-` still in config, please check readme for "
        f"valid input parameters. \nUnparsed analyses: `{unparsed_inputs}`"
    )


def populate_tso500_reports_workflow(
    input_dict,
    sample,
    all_output_files,
    job_output_ids
) -> dict:
    """
    Handle the irritating running of the TSO500 reports workflow
    after eggd_TSO500 app runs.

    This is a pain as the eggd_TSO500 runs once per run, and outputs
    an array of files for each file type (i.e BAMs, VCFs, CVOs etc.).
    In addition, if the run is a mix of DNA and RNA samples, these
    are different output fields (job-xxx.dna_bams vs job-xxx.rna_bams).
    Therefore, we will handle parsing of these inputs separately from
    the standard functions, and maybe one day this can all go away...

    Outline of what we expect to handle here:
        - get all output files for the given sample from the
            eggd_TSO500 app output
        - get either the DNA BAM or RNA BAM output and respective index
            as inputs for mosdepth
        - get either gVCF (for DNA) or SpliceVariants VCF (for RNA)
            as inputs for vcf rescue -> vep -> workbooks
        - get the CombinedVariantOutput tsv for the sample and the
            metricsOutput for the run as inputs for input to
            generate_variant_workbook.additional_files


    Parameters
    ----------
    input_dict : dict
        input dict parsed from the config
    sample : str
        name of current sample
    all_output_files : list
        list of dicts of all output files from eggd_tso500 job
    job_output_ids : dict mapping output field -> list of file IDs
        (e.g. {'vcf': [{'$dnanexus_link': 'file-xxx', ...}]})

    Returns
    -------
    dict
        populated input dict
    """
    print("Adding input files for TSO500 reports workflow")

    # mapping of the value expected in the input dict parsed from
    # the config file -> the eggd_tso500 app output fields to select from
    tso500_input_fields = {
        "eggd_tso500.fastqs": ['fastqs'],
        "eggd_tso500.bam": ['dna_bams', 'rna_bams'],
        "eggd_tso500.idx": ['dna_bam_index', 'rna_bam_index'],
        "eggd_tso500.vcf": ['gvcfs', 'splice_variants_vcfs'],
        "eggd_tso500.cvo": ['cvo']
    }

    for stage_input, output_fields in tso500_input_fields.items():
        # get the actual stage.field from input dict, we will
        # have something like this from the config:
        # inputs: {
        #   "stage-GF22j384b0bpYgYB5fjkk34X.bam": "eggd_tso500.bam",
        #   "stage-GF22j384b0bpYgYB5fjkk34X.index": "eggd_tso500.idx"
        # }
        # the value strings are pretty arbitrary but the main thing is
        # we're not hardcoding the actual stage IDs here in case they
        # change, and then we can just change them in the config
        config_stage_input = list({
            k: v for k, v in input_dict.items() if v == stage_input
        }.keys())

        if not config_stage_input:
            # this input not present in config file, likely been
            # removed => skip trying to add it
            continue

        config_stage_input = config_stage_input[0]

        # get the corresponding eggd_tso500 output files for
        # the given stage input, where there are 2 potential files
        # (i.e. dna_bams and rna_bams) we expect at least one to
        # be present, and for cvo they should always be present
        dx_links = [
            job_output_ids.get(x) for x in output_fields
            if job_output_ids.get(x)
        ]

        assert dx_links, Slack().send(
            "No output files found from eggd_tso500 job from the "
            f"output fields: {output_fields}"
        )

        file_ids = [
            id.get('$dnanexus_link') for sublist in dx_links for id in sublist
        ]
        file_details = [
            x for x in all_output_files if x['id'] in file_ids
        ]
        sample_file = [
            x for x in file_details if x['describe']['name'].startswith(sample)
        ]

        assert sample_file, (
            f"No eggd_tso500 files found for sample {sample} for "
            f"input {stage_input}"
        )

        if output_fields == ['fastqs']:
            # handle fastqs separately since they should be an array
            input_dict[config_stage_input] = [
                {'$dnanexus_link': x['id']} for x in sample_file
            ]
        else:
            input_dict[config_stage_input] = {
                '$dnanexus_link': sample_file[0]['id']
            }

    # get the dnanexus_link we already added for the cvo, and turn
    # this into an array input with the metricsOutput
    additional_files_stage = [
        x for x in input_dict if x.endswith('.additional_files')
    ]

    if additional_files_stage:
        additional_files_stage = additional_files_stage[0]

        # make additional files for generate_workbook also take in the
        # per run metricsOutput file, should already have the sample cvo
        metrics_output = job_output_ids.get('metricsOutput')
        assert metrics_output, "No metrics output file found from tso500 job"

        cvo_dnanexus_link = input_dict[additional_files_stage]
        input_dict[additional_files_stage] = [
            cvo_dnanexus_link, metrics_output
        ]

    print(f"Inputs added to input dict:\n\n{input_dict}")

    return input_dict
