"""
Random utility functions including those for sending Slack messages
and searching Jira for sequencing run tickets.
"""
from datetime import datetime
import json
import os
import re
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../')
))

import utils.WebClasses as WebClasses


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
    start = end = ''

    if isinstance(log_data, str):
        # nicely handle line breaks for spacing in logs
        if log_data.startswith('\n'):
            start = '\n'
            log_data = log_data.lstrip('\n')

        if log_data.endswith('\n'):
            end = '\n'
            log_data = log_data.rstrip('\n')

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
        run ID parsed from RunInfo.xml
    instance_types : dict
        mapping of flowcell type to instance type(s) to use from config file

    Returns
    -------
    dict | str
        instance types to use for given flowcell, type will be a string if a
        single instance type is defined, or dict if it is a mapping (i.e for
        multiple stages of a workflow)
    """
    prettier_print('Selecting instance types from assay config file')
    if not instance_types:
        # empty dict provided => no user defined instances in config
        prettier_print('No instance types set to use from config')
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
    flowcell_id = re.split('[_-]', run_id)[-1]

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
            start_x = re.search(r'^x*', type).group()
            end_x = re.search(r'x*$', type).group()

            # make x's n or more character regex pattern (e.g. [\d\w]{5,})
            if start_x:
                start_x = f"[\d\w]{{{len(start_x)},}}"

            if end_x:
                end_x = f"[\d\w]{{{len(end_x)},}}"

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
        'No matches found for Illumina flowcell patterns, '
        'checking for SP, S1, S2 and S4'
    )
    if 'DR' in flowcell_id:
        # this is an SP or S1 flowcell, both use the same identifier
        # therefore try select S1 first from the instance types since
        # we can't differetiate the two
        if instance_types.get('S1'):
            prettier_print("Match found for S1 instances")
            return instance_types['S1']
        elif instance_types.get('SP'):
            prettier_print("Match found for SP instances")
            return instance_types['SP']
        else:
            # no instance types defined for SP/S1 flowcell
            prettier_print(
                'SP/S1 flowcell used but no instance types specified'
            )

    if 'DM' in flowcell_id:
        # this is an S2 flowcell, check for S2
        if instance_types.get('S2'):
            prettier_print("Match found for S2 instances")
            return instance_types['S2']
        else:
            # no instance type defined for S2 flowcell
            prettier_print('S2 flowcell used but no isntance types specified')

    if 'DS' in flowcell_id:
        # this is an S4 flowcell, check for S4
        if instance_types.get('S4'):
            prettier_print("Match found for S4 instances")
            return instance_types['S4']
        else:
            # no instance type defined for S2 flowcell
            prettier_print('S4 flowcell used but no isntance types specified')

    # if we get here then we haven't identified a match for the flowcell
    # used for sequencing against what we have defined in the config,
    # check for '*' being present (i.e. the catch all instances), else
    # return None to use app / workflow defaults
    if instance_types.get('*'):
        prettier_print("Match found for default (*) instances")
        return instance_types['*']
    else:
        prettier_print(
            'No defined instances types found for flowcell used, will use '
            'app / workflow defaults'
        )
        return None
