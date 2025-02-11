"""
Random utility functions
"""

from collections import defaultdict
from datetime import datetime
import json
import os
import pathlib
import re
import sys

sys.path.append(
    os.path.abspath(os.path.join(os.path.realpath(__file__), "../"))
)

import dxpy as dx
from packaging.version import parse as parseVersion
import pandas as pd

import utils.WebClasses as WebClasses
from WebClasses import Slack


def prettier_print(log_data) -> None:
    """
    Pretty print for nicer viewing in the logs since pprint does not
    do an amazing job visualising big dicts and long strings.

    Bonus: we're indenting using the Braille Pattern Blank U+2800
    unicode character since the new DNAnexus UI (as of Dec. 2023)
    strips leading tabs and spaces in the logs, which makes viewing
    the pretty dicts terrible. Luckily they don't strip other
    whitespace characters, so we can get around them yet again making
    their UI worse.

    Parameters
    ----------
    log_data : anything json dumpable
        data to print
    """
    start = end = ""

    if isinstance(log_data, str):
        # nicely handle line breaks for spacing in logs
        if log_data.startswith("\n"):
            start = "\n"
            log_data = log_data.lstrip("\n")

        if log_data.endswith("\n"):
            end = "\n"
            log_data = log_data.rstrip("\n")

    print(
        f"{start}[{datetime.now().strftime('%H:%M:%S')}] - "
        f"{json.dumps(log_data, indent='⠀⠀')}{end}"
    )


def time_stamp() -> str:
    """
    Returns string of date & time formatted as YYMMDD_HHMM

    Returns
    -------
    str
        String of current date and time as YYMMDD_HHMM
    """
    return datetime.now().strftime("%y%m%d_%H%M")


def select_instance_types(run_id, instance_types) -> dict:
    """
    Select correct instance types to use for analysis based from those
    defined in the assay config file for the current flowcell used.

    The flowcell ID from the run ID is used to infer what type of flowcell
    has been used for sequencing (i.e. S1, S2, S4...), which allows for
    dynamically setting the appropriate instance type defined in the config.

    Flowcell type in instance types dict from config may be defined as:
        - 'S1/S2/S4' -> human readable flowcell type, map back to IDs
        - 'xxxxxDRxx' -> Illumina ID format, use regex to match
        - '*' -> default to use for flowcell where no others match

    Matching will be done in the above order (i.e. if S1 and xxxxxDRxx are
    defined, S1 will be preferentially used). If no matches are found then
    None is returned.

    Parameters
    ----------
    run_id : str
        run ID
    instance_types : dict
        mapping of flowcell type to instance type(s) to use from config file

    Returns
    -------
    dict | str
        instance types to use for given flowcell, type will be a string if a
        single instance type is defined, or dict if it is a mapping (i.e for
        multiple stages of a workflow)
    """
    prettier_print("Selecting instance types from assay config file")
    if not instance_types:
        # empty dict provided => no user defined instances in config
        prettier_print("No instance types set to use from config")
        return None

    if isinstance(instance_types, str):
        # instance types is string => single instance type
        prettier_print(f"Single instance type set: {instance_types}")
        return instance_types

    # mapping of flowcell ID patterns for NovaSeq flowcells from:
    # https://knowledge.illumina.com/instrumentation/general/instrumentation-general-reference_material-list/000005589
    # "SP": "xxxxxDRxx"
    # "S1": "xxxxxDRxx"
    # "S2": "xxxxxDMx"
    # "S4": "xxxxxDSxx"

    matches = []
    # flowcell ID is last part of run ID
    flowcell_id = re.split("[_-]", run_id)[-1]

    prettier_print(
        f"Instance types set for the following keys:\n\t{instance_types}"
    )

    for type in instance_types.keys():
        if type not in ["SP", "S1", "S2", "S4", "*"]:
            # assume its an Illumina flowcell pattern in the config (i.e. xxxxxDMxx)
            # since the patterns aren't correct and can have more characters
            # than x's, we turn it into a regex pattern matching >= n x's if
            # there are alphanumeric characters at start or end
            prettier_print(f"Illumina flowcell pattern found: {type}")
            start_x = re.search(r"^x*", type).group()
            end_x = re.search(r"x*$", type).group()

            # make x's n or more character regex pattern (e.g. [\d\w]{5,})
            if start_x:
                start_x = rf"[\d\w]{{{len(start_x)},}}"

            if end_x:
                end_x = rf"[\d\w]{{{len(end_x)},}}"

            match = re.search(
                f"{start_x}{type.strip('x')}{end_x}", flowcell_id
            )

            if match:
                matches.append(type)

    if matches:
        # we found a match against the flowcell ID and one of the sets of
        # instance types to use => return this to use
        assert len(matches) == 1, WebClasses.Slack().send(
            "More than one set of instance types set for the same flowcell:"
            f"\n\t{matches}"
        )
        prettier_print(f"Found instance types for flowcell: {matches[0]}")
        prettier_print("The following instance types will be used:")
        prettier_print(instance_types.get(matches[0]))
        return instance_types.get(matches[0])

    # no match against Illumina patterns found in instances types dict and
    # the current flowcell ID, check for SP / S1 / S2 / S4
    prettier_print(
        "No matches found for Illumina flowcell patterns, "
        "checking for SP, S1, S2 and S4"
    )
    if "DR" in flowcell_id:
        # this is an SP or S1 flowcell, both use the same identifier
        # therefore try select S1 first from the instance types since
        # we can't differetiate the two
        if instance_types.get("S1"):
            prettier_print("Match found for S1 instances")
            return instance_types["S1"]
        elif instance_types.get("SP"):
            prettier_print("Match found for SP instances")
            return instance_types["SP"]
        else:
            # no instance types defined for SP/S1 flowcell
            prettier_print(
                "SP/S1 flowcell used but no instance types specified"
            )

    if "DM" in flowcell_id:
        # this is an S2 flowcell, check for S2
        if instance_types.get("S2"):
            prettier_print("Match found for S2 instances")
            return instance_types["S2"]
        else:
            # no instance type defined for S2 flowcell
            prettier_print("S2 flowcell used but no isntance types specified")

    if "DS" in flowcell_id:
        # this is an S4 flowcell, check for S4
        if instance_types.get("S4"):
            prettier_print("Match found for S4 instances")
            return instance_types["S4"]
        else:
            # no instance type defined for S2 flowcell
            prettier_print("S4 flowcell used but no isntance types specified")

    # if we get here then we haven't identified a match for the flowcell
    # used for sequencing against what we have defined in the config,
    # check for '*' being present (i.e. the catch all instances), else
    # return None to use app / workflow defaults
    if instance_types.get("*"):
        prettier_print("Match found for default (*) instances")
        return instance_types["*"]
    else:
        prettier_print(
            "No defined instances types found for flowcell used, will use "
            "app / workflow defaults"
        )
        return None


def parse_sample_sheet(samplesheet) -> list:
    """
    Parses list of sample names from given samplesheet

    Parameters
    ----------
    samplesheet : str
        Path to samplesheet to parse

    Returns
    -------
    list
        list of sample names

    Raises
    ------
    AssertionError
        Raised when no samples parsed from samplesheet
    """

    sheet = pd.read_csv(samplesheet, header=None, usecols=[0])
    column = sheet[0].tolist()
    sample_list = column[column.index("Sample_ID") + 1 :]

    # sense check some samples found and samplesheet isn't malformed
    assert sample_list, Slack().send(
        f"Sample list could not be parsed from samplesheet: {samplesheet}"
    )

    return sample_list


def match_samples_to_assays(configs, all_samples, provided_config) -> dict:
    """
    Match sample list against configs to identify correct config to use
    for each sample

    Parameters
    ----------
    configs : dict
        dict of config dicts for each assay
    all_samples : list
        list of samples parsed from samplesheet or specified with --samples
    provided_config : bool
        if a  config has been provided via the CLI, match the samples to its
        assay code

    Returns
    -------
    dict
        dict of assay_code : list of matching samples, i.e.
            {LAB123 : ['sample1-LAB123', 'sample2-LAB123' ...]}

    Raises
    ------
    AssertionError
        Raised when not all samples have an assay config matched
    AssertionError
        Raised when more than one assay config found to use for given samples
    """

    # build a dict of assay codes from configs found to samples based off
    # matching assay_code in sample names
    prettier_print("\nMatching samples to assay configs")
    all_config_assay_codes = sorted(
        [x.get("assay_code") for x in configs.values()]
    )
    assay_to_samples = defaultdict(list)

    prettier_print(
        f"\nAll assay codes of config files: {all_config_assay_codes}"
    )
    prettier_print("Samples to be used:")
    prettier_print(all_samples)

    # for each sample check each assay code if it matches, then select the
    # matching config with highest version
    for sample in all_samples:
        sample_to_assay_configs = {}

        for code in all_config_assay_codes:
            # find all config files that match this sample
            if re.search(code, sample, re.IGNORECASE):
                sample_to_assay_configs[code] = configs[code]["version"]

        if sample_to_assay_configs:
            # found at least one config to match to sample, select
            # one with the highest version
            highest_ver_config = max(
                sample_to_assay_configs.values(), key=parseVersion
            )

            # select the config key with for the corresponding value found
            # to be the highest
            latest_config_key = list(sample_to_assay_configs)[
                list(sample_to_assay_configs.values()).index(
                    highest_ver_config
                )
            ]

            assay_to_samples[latest_config_key].append(sample)

    # check all samples have an assay code in one of the configs
    samples_w_codes = [x for y in list(assay_to_samples.values()) for x in y]
    samples_without_codes = "\n\t\t".join(
        [f"`{x}`" for x in sorted(set(all_samples) - set(samples_w_codes))]
    )

    # config(s) were provided from the command line
    if provided_config:
        # no sample were able to be matched to the config code
        if not assay_to_samples:
            if len(configs) == 1:
                # if only one config was provided, match samples to the
                # provided config code
                for sample in all_samples:
                    assay_to_samples[all_config_assay_codes[0]] = sample

            else:
                raise Exception(
                    "Not all samples were able to be matched to provided "
                    f"configs: {all_config_assay_codes} vs "
                    f"{samples_without_codes}"
                )
    else:
        assert sorted(all_samples) == sorted(samples_w_codes), Slack().send(
            "Could not identify assay code for all samples!\n\n"
            "Configs for assay codes found: "
            f"`{', '.join(all_config_assay_codes)}`\n\nSamples not matching "
            f"any available config:\n\t\t{samples_without_codes}"
        )

    prettier_print("Total samples per assay identified:")
    prettier_print(dict(assay_to_samples))

    return assay_to_samples


def preprocess_exclusion_patterns(patterns):
    """Preprocess the exclude samples parameter to be sure that exclusion
    patterns are correct

    Parameters
    ----------
    patterns : iterable
        Iterable containing the exclusion patterns

    Returns
    -------
    set
        Set containing the corrected patterns if needed

    Raises
    ------
    AssertionError
        When the pattern doesn't match a typical full sample name pattern or
        an assay code pattern
    """

    new_patterns = set()

    for pattern in patterns:
        if re.fullmatch(r"^(?!-).*-(.*-)+.*(?!-)$", pattern):
            # full sample_name
            new_patterns.add(pattern)

        elif re.search(r"-.*-", pattern):
            # correct non TSO500 assay code
            new_patterns.add(pattern)

        elif re.search(r"-.*", pattern):
            # assume it's a TSO500 code and add component in order to avoid
            # accidental matching
            new_patterns.add(f"{pattern}$")

        else:
            raise AssertionError(
                (
                    f"'{pattern}' doesn't match the expected pattern of a "
                    "sample name or an assay code"
                )
            )

    return list(new_patterns)


def exclude_samples(samples, patterns=[]):
    """Exclude samples given a list of patterns

    Parameters
    ----------
    samples : list
        List of samples to filter
    patterns : list, optional
        List of pattern to filter with, by default []

    Returns
    -------
    list
        List of samples after filtering

    Raises
    ------
    AssertionError
        When the pattern matches multiple times in the sample, in order to be
        sure that the pattern is correct
    """

    samples_to_remove = set()

    for pattern in patterns:
        for sample in samples:
            if len(re.findall(pattern, sample)) > 1:
                raise AssertionError(
                    f"The pattern '{pattern}' matches multiple times in '{sample}'"
                )

            # should be an assay code, look for that in the sample name
            if re.search(pattern, sample):
                samples_to_remove.add(sample)

    return list(set(samples).difference(samples_to_remove))


def get_previous_job_from_job_reuse(job_reuse, configs, handler_assay, params):
    """Given a dict pointing to a job id, perform a series of check to ensure
    that the job id can be used for bypassing job starts

    Parameters
    ----------
    job_reuse : dict
        Dict containing info for the job id to reuse
    configs : list
        List of config data
    handler_assay : str
        Assay used in that context of the function
    params : dict
        Dict containing assay information in the context of the function

    Returns
    -------
    str
        String with the job id to reuse

    Raises
    ------
    AssertionError
        When the top key of job_reuse is not available for use because of
        restriction at the assay config level
    NotImplementedError
        When the analysis specified in the job_reuse variable is a per sample
        analysis which is not supported
    """

    invalid_assay_keys = []
    assay_names_available = [
        config.get("assay") for config in configs.values()
    ]

    # check the job reuse dict if the key are linked to dicts i.e.
    # assay level job reuse
    for key, value in job_reuse.items():
        if isinstance(value, dict):
            if key not in assay_names_available:
                invalid_assay_keys.append(key)

    if invalid_assay_keys:
        raise AssertionError(
            (
                "-ijob_reuse: The following assay names are not "
                f"available to be used {invalid_assay_keys}. "
                "Check if the use of -iassay_config might impact "
                "the assay keys that you can use. Full job_reuse "
                f"parameter passed: {job_reuse}"
            )
        )

    # check if the assay matches the top level in the job reuse arg
    analysis = (
        job_reuse.get(handler_assay)
        if handler_assay in job_reuse
        else job_reuse
    )

    # first check if specified to reuse a previous job for this
    # step
    if analysis.get(params["analysis"]):
        previous_job = analysis.get(params["analysis"])

        assert re.match(r"(job|analysis)-[\w]+", previous_job), (
            "Job specified to reuse does not appear valid: " f"{previous_job}"
        )

        if params["per_sample"]:
            # ensure we're only doing this for per run jobs for now
            raise NotImplementedError(
                "-ijob_reuse not yet implemented for per sample jobs"
            )

        prettier_print(
            f"Reusing provided job {previous_job} for analysis step "
            f"{params['analysis']} for {params['name']}"
        )

        # dict to add all stage output names and job ids for every
        # sample to used to pass correct job ids to subsequent
        # workflow / app calls
        return previous_job

    return None


def load_config(config_file) -> dict:
    """
    Read in given config json to dict

    Parameters
    ----------
    config_file : str
        Path to json config file

    Raises
    ------
    RuntimeError: raised when a non-json file passed as config

    Returns
    -------
    config : dict
        dictionary of loaded json file
    """
    if not config_file.endswith(".json"):
        # sense check a json passed
        raise RuntimeError("Error: invalid config passed - not a json file")

    with open(config_file) as file:
        config = json.load(file)

    return config


def load_test_data(test_samples) -> list:
    """
    Read in file ids of fastqs and sample names from test_samples file to test
    calling workflows

    Parameters
    ----------
    test_samples : str
        filename of test samples to read in

    Returns
    -------
    fastq_details : list of tuples
        list with tuple per fastq containing (DNAnexus file id, filename)

    """
    with open(test_samples) as f:
        fastq_details = f.read().splitlines()

    fastq_details = [(x.split()[0], x.split()[1]) for x in fastq_details]

    return fastq_details


def create_project_name(run_id, assay, development, testing):
    """Create a project name given a few parameters

    Parameters
    ----------
    run_id : str
        Name of the run
    assay : str
        Assay type i.e. CEN, TWE..
    development : bool
        Bool to determine whether the project name should be a 003
    testing : bool
        Bool to determine whether to add a suffix to the project name to tag it
        for testing purposes

    Returns
    -------
    str
        Name of the project to find or to create
    """

    if development:
        prefix = f'003_{datetime.now().strftime("%y%m%d")}_run-'
    else:
        prefix = "002_"

    suffix = ""

    if testing:
        suffix = "-EGGD_CONDUCTOR_TESTING"

    return f"{prefix}{run_id}_{assay}{suffix}"


def write_job_summary(specified_dx_project, *handlers):
    """Write a job summary file for the whole conductor job

    Parameters
    ----------
    specified_dx_project: str
        DX project passed to the conductor app
    *handlers:
        Variable length argument list of handlers objects.
    """

    for i, handler in enumerate(handlers, 1):
        nb_jobs_per_assay = 0
        assay = handler.assay
        project = handler.project

        if specified_dx_project:
            path_to_job_summary = pathlib.Path(
                f"/home/dnanexus/out/job_summaries/{time_stamp()}.{project.name}.{i}-conductor_job_summary.txt"
            )
        else:
            path_to_job_summary = pathlib.Path(
                f"/home/dnanexus/out/job_summaries/{time_stamp()}.{project.name}.conductor_job_summary.txt"
            )

        path_to_job_summary.parent.mkdir(parents=True, exist_ok=True)

        with path_to_job_summary.open("w") as f:
            # write config information
            f.write(f"{handler}\n\n")

            # i.e. if an error was detected during the setup/starting of jobs
            # by the big try/except
            if not handler.job_summary:
                f.write(
                    "No jobs were started for this project. Please check the logs if this is not an expected outcome"
                )
                continue

            for executable, info in handler.job_summary.items():
                nb_jobs_per_executable = 0
                executable_dict = dx.bindings.dxdataobject_functions.describe(
                    executable
                )

                exe_name = executable_dict.get("name")
                exe_version = executable_dict.get("version")

                f.write(f"Jobs started for '{exe_name}")

                if exe_version:
                    f.write(f" - {exe_version}'")
                else:
                    f.write("'")

                if isinstance(info, dict):
                    f.write(":\n")

                    for sample, job_id in info.items():
                        if job_id is None:
                            f.write(
                                f" - {sample}: No job started due to missing previous output\n"
                            )
                        else:
                            f.write(f" - {sample}: {job_id}\n")

                        nb_jobs_per_assay += 1
                        nb_jobs_per_executable += 1
                else:
                    f.write(f": '{info}'\n")

                    nb_jobs_per_assay += 1
                    nb_jobs_per_executable += 1

                f.write(
                    f"Number of jobs started for {exe_name}: {nb_jobs_per_executable}\n\n"
                )

            f.write(
                f"Number of jobs started for {assay}: {nb_jobs_per_assay}\n"
            )
