"""
Random utility functions including those for sending Slack messages
and searching Jira for sequencing run tickets.
"""
from datetime import datetime
import json
import os
import re
import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
import sys
import traceback
from urllib3.util import Retry

sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../')
))


class Slack():
    """
    Slack related functions
    """
    def __init__(self) -> None:
        self.slack_token = os.getenv("SLACK_TOKEN")
        self.slack_alert_channel = os.getenv("SLACK_ALERT_CHANNEL")


    def send(self, message, exit_fail=True, warn=False) -> None:
        """
        Send alert to Slack to know something has failed

        Parameters
        ----------
        message : str
            message to send to Slack
        exit_fail : bool
            if the alert is being sent when exiting and to create the
            slack_fail_sent.log file. If false, this will be for an alert only.
        warn : bool
            if to send the alert as a warning or an error (default False =>
            send Slack alert as an error)
        """
        conductor_job_url = os.environ.get('conductor_job_url')
        channel = self.slack_alert_channel

        if warn:
            # sending warning with different wording to alert
            message = (
                f":rotating_light: *Warning - eggd_conductor*\n\n{message}\n\n"
                f"eggd_conductor job: {conductor_job_url}"
            )
        else:
            message = (
                f":warning: *Error - eggd_conductor*\n\nError in processing "
                f"run *{os.environ.get('RUN_ID')}*\n\n{message}\n\n"
                f"eggd_conductor job: {conductor_job_url}"
            )

        prettier_print(f"\nSending message to Slack channel {channel}\n\n{message}")

        http = requests.Session()
        retries = Retry(total=5, backoff_factor=10, method_whitelist=['POST'])
        http.mount("https://", HTTPAdapter(max_retries=retries))

        try:
            response = http.post(
                'https://slack.com/api/chat.postMessage', {
                    'token': self.slack_token,
                    'channel': f"#{channel}",
                    'text': message
                }).json()

            if not response['ok']:
                # error in sending slack notification
                prettier_print(f"Error in sending slack notification: {response.get('error')}")
        except Exception as err:
            prettier_print(f"Error in sending post request for slack notification: {err}")

        if exit_fail:
            # write file to know in bash script a fail alert already sent
            open('slack_fail_sent.log', 'w').close()


class Jira():
    """
    Jira related functions for getting sequencing run ticket for a given
    run to tag analysis links to
    """
    def __init__(self, queue_url, issue_url, token, email) -> None:
        self.queue_url = queue_url
        self.issue_url = issue_url
        self.token = token
        self.email = email
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        self.auth = HTTPBasicAuth(self.email, self.token)
        self.http = self.create_session()

    def create_session(self):
        """
        Create session adapter object to use for queries

        Returns
        -------
        session : http session object
        """
        http = requests.Session()
        retries = Retry(total=5, backoff_factor=10, method_whitelist=['POST'])
        http.mount("https://", HTTPAdapter(max_retries=retries))
        return http

    def get_all_tickets(self) -> list:
        """
        Get all tickets from given queue URL endpoint

        Returns
        -------
        list : list of all tickets with details for given queue
        """
        prettier_print(f"\nGetting all Jira tickets from endpoint: {self.queue_url}")
        start = 0
        response_data = []

        while True:
            response = self.http.get(
                url=f"{self.queue_url}/issue?start={start}",
                headers=self.headers,
                auth=self.auth
            )

            if not response.ok:
                self.send_slack_alert(
                    f"Error querying Jira for tickets."
                    f"\nAPI endpoint: {self.queue_url}/issue?start={start}\n"
                    f"Status code: *{response.status_code}*\n"
                    f"Error:```{response.content.decode()}```\n"
                    "Continuing analysis without linking to Jira ticket."
                )
            else:
                response = response.json()

            if response['size'] == 0:
                break

            response_data.extend(response['values'])
            start += 50

        prettier_print(f"Found {len(response_data)} tickets")

        return response_data

    def get_run_ticket_id(self, run_id, tickets) -> str:
        """
        Given a list of tickets, filter out the one for the current
        sequencing run and return its ID

        Parameters
        ----------
        run_id : str
            run ID of current sequencing run
        tickets : list
            list of tickets form Jira queue

        Returns
        -------
        str
            ticket ID
        """
        prettier_print("Filtering Jira tickets for current run")
        run_ticket = list(set([
            x['id'] for x in tickets if run_id in x['fields']['summary']
        ]))
        prettier_print(f"Run ticket(s) found: {run_ticket}")

        if not run_ticket:
            # didn't find a ticket -> either a typo in the name or ticket
            # has not yet been raised / forgotten about, send an alert and
            # continue with things since linking to Jira is non-essential
            self.send_slack_alert(
                f"No Jira ticket found for the current sequencing run "
                f"*{run_id}*.\n\nContinuing with analysis without linking to Jira."
            )

            self.run_ticket_id = None

        elif len(run_ticket) > 1:
            # found multiple tickets, this should not happen so send us an
            # alert and don't touch the tickets
            self.send_slack_alert(
                f"Found more than one Jira ticket for given run (`{run_id}`)"
                f"\n{run_ticket}"
            )
            self.run_ticket_id = None

        else:
            self.run_ticket_id = run_ticket[0]

    def send_slack_alert(self, message) -> None:
        """
        Send warning alert to Slack that there was an issue with querying Jira

        Parameters
        ----------
        message : str
            message to send to Slack
        """
        if not os.path.exists('jira_alert.log'):
            # create file to not send multiple Slack messages
            os.mknod('jira_alert.log')
            Slack().send(
                message=message,
                exit_fail=False,
                warn=True
            )
        else:
            prettier_print(
                "Slack notification for fail with Jira already sent, "
                f"won't send current error: {message}"
            )

    def add_comment(self, comment, url) -> None:
        """
        Find Jira ticket for given run ID and add internal comment

        Parameters
        ----------
        run_id : str
            ID of sequencing run
        comment : str
            comment to add to Jira ticket
        url : str
            any url to add to message after comment
        """
        if not any([self.queue_url, self.issue_url, self.token, self.email]):
            # none of Jira related variables defined in config, assume we
            # aren't wanting to use Jira and continue
            prettier_print(
                "No required Jira variables set in config, continuinung "
                "without using Jira"
            )
            return

        if not all([self.queue_url, self.issue_url, self.token, self.email]):
            # one or more required Jira variables not set in config
            if self.token:
                # hide the token to not send it in the Slack message
                self.token = f"{self.token[:4]}{'*' * (len(self.token) - 4)}"

            variables = (
                f"`JIRA_QUEUE_URL: {self.queue_url}`\n"
                f"`JIRA_ISSUE_URL: {self.issue_url}`\n"
                f"`JIRA_TOKEN: {self.token}`\n"
                f"`JIRA_EMAIL: {self.email}`\n"
            )

            self.send_slack_alert(
                "Unable to query Jira - one or more variables "
                f"not defined in the config\n{variables}"
                "Continuing analysis without linking to Jira ticket."
            )
            return

        comment_url = f"{self.issue_url}/{self.run_ticket_id}/comment"

        payload = json.dumps({
            "body": {
                "type": "doc",
                "version": 1,
                "content": [{
                    "type": "paragraph",
                    "content": [
                        {
                            "text": f"{comment}",
                            "type": "text"
                        },
                        {
                            "text": f"{url}",
                            "type": "text",
                            "marks": [{
                                "type": "link",
                                "attrs": {
                                    "href": f"{url}"
                                }
                            }]
                        }
                    ]
                }]
            },
            "properties": [{
                "key": "sd.public.comment",
                "value": {
                    "internal": True
                }
            }]
        })

        response = self.http.post(
            url=comment_url,
            data=payload,
            headers=self.headers,
            auth=self.auth
        )

        if not response.status_code == 201:
            # some kind of error occurred adding Jira comment =>
            # send a non-exiting Slack alert
            self.send_slack_alert(
                f"failed to add comment to Jira ticket ({self.run_ticket_id})\n\n"
                f"Status code: {response.status_code}\n\n"
                f"Error response: `{response.text}`"
            )


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
    flowcell_id = re.split('[_-]', run_id)[-1]  # flowcell ID is last part of run ID

    prettier_print(f"Instance types set for the following keys:\n\t{instance_types}")

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

            match = re.search(f"{start_x}{type.strip('x')}{end_x}", flowcell_id)
            if match:
                matches.append(type)

    if matches:
        # we found a match against the flowcell ID and one of the sets of
        # instance types to use => return this to use
        assert len(matches) == 1, Slack().send(
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
            prettier_print('SP/S1 flowcell used but no instance types specified')

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
