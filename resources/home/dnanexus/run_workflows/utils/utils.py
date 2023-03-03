"""
Random utility functions including those for sending Slack messages
and searching Jira for sequencing run tickets.
"""
from datetime import datetime
import json
import os
import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
import traceback
from urllib3.util import Retry

from utils.dx_log import dx_log

log = dx_log()

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
                f":warning: *Error - eggd_conductor*\n\n{message}\n\n"
                f"eggd_conductor job: {conductor_job_url}"
            )

        log.info(f"Sending message to Slack channel {channel}\n\n{message}")

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
                log.error(f"Error in sending slack notification: {response.get('error')}")
        except Exception as err:
            log.error(f"Error in sending post request for slack notification: {err}")

        if exit_fail:
            # write file to know in bash script a fail alert already sent
            open('slack_fail_sent.log', 'w').close()


class Jira():
    """
    Jira related functions for getting sequencing run ticket for a given
    run to tag analysis links to
    """
    def __init__(self) -> None:
        self.queue_url = os.environ.get('JIRA_QUEUE_URL')
        self.issue_url = os.environ.get('JIRA_ISSUE_URL')
        self.token = os.environ.get('JIRA_TOKEN')
        self.email = os.environ.get('JIRA_EMAIL')
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


    def get_all_tickets(self, run_id) -> list:
        """
        Get all tickets from given queue URL endpoint

        Parameters
        ----------
        run_id : str
            ID of current run

        Returns
        -------
        list : list of all tickets with details for given queue
        """
        log.info(f"Getting all Jira tickets from endpoint: {self.queue_url}")
        start = 0
        response_data = []

        while True:
            response = self.http.get(
                url=f"{self.queue_url}/issue?start={start}",
                headers=self.headers,
                auth=self.auth
            )

            log.info(response)

            if not response.ok:
                self.send_slack_alert(
                    f"Error querying Jira for tickets for current run `{run_id}`"
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

        log.info(f"Found {len(response_data)} tickets")

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
        log.info("Filtering Jira tickets for current run")
        run_ticket = list(set([
            x['id'] for x in tickets if run_id in x['fields']['summary']
        ]))
        log.info(f"Run ticket(s) found: {run_ticket}")

        if not run_ticket:
            # didn't find a ticket -> either a typo in the name or ticket
            # has not yet been raised / forgotten about, send an alert and
            # continue with things since linking to Jira is non-essential
            self.send_slack_alert(
                f"No Jira ticket found for the current sequencing run "
                f"*{run_id}*.\n\nContinuing with analysis without linking to Jira."
            )

            return None

        if len(run_ticket) > 1:
            # found multiple tickets, this should not happen so send us an
            # alert and don't touch the tickets
            self.send_slack_alert(
                f"Found more than one Jira ticket for given run (`{run_id}`)"
                f"\n{run_ticket}"
            )
            return None

        return run_ticket[0]


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
            log.info(
                "Slack notification for fail with Jira already sent, "
                f"won't send current error: {message}"
            )


    def add_comment(self, run_id, comment, url) -> None:
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
            log.info(
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

        try:
            # put whole thing in a try execept to not cause analysis to stop
            # if there's an issue with Jira, just send an alert to slack
            log.info("Finding Jira ticket to add comment to")
            tickets = self.get_all_tickets(run_id=run_id)
            ticket_id = self.get_run_ticket_id(run_id=run_id, tickets=tickets)
            log.info(f"Found Jira ticket ID {ticket_id} for run {run_id}")
        except Exception:
            self.send_slack_alert(
                f"Error finding Jira ticket for given run (`{run_id}`).\n"
                f"Continuing analysis without linking to Jira ticket.\n"
                f"Error: ```{traceback.format_exc()}```"
            )
            return

        if not ticket_id:
            # no ticket found or more than one ticket found, Slack alert
            # will have been sent in get_run_ticket_id()
            return

        comment_url = f"{self.issue_url}/{ticket_id}/comment"

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
                f"failed to add comment to Jira ticket ({ticket_id})\n\n"
                f"Status code: {response.status_code}\n\n"
                f"Error response: `{response.text}`"
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
