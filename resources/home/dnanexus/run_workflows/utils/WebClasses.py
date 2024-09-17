import json
import os
from urllib3.util import Retry

import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth

from utils.utils import prettier_print


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

        prettier_print(
            f"\nSending message to Slack channel {channel}\n\n{message}"
        )

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
                prettier_print(
                    "Error in sending slack notification: "
                    f"{response.get('error')}"
                )
        except Exception as err:
            prettier_print(
                f"Error in sending post request for slack notification: {err}"
            )

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
        prettier_print(
            f"\nGetting all Jira tickets from endpoint: {self.queue_url}"
        )
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

    def filter_tickets_using_run_id(self, run_id, tickets) -> str:
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
        run_tickets = [x for x in tickets if run_id in x['fields']['summary']]
        prettier_print(
            f"Run ticket(s) found: {[ticket['key'] for ticket in run_tickets]}"
        )

        if not run_tickets:
            # didn't find a ticket -> either a typo in the name or ticket
            # has not yet been raised / forgotten about, send an alert and
            # continue with things since linking to Jira is non-essential
            self.send_slack_alert(
                "No Jira ticket found for the current sequencing run "
                f"*{run_id}*.\n\nContinuing with analysis without linking to "
                "Jira."
            )
            return
        else:
            return run_tickets

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

    def add_comment(self, comment, url, ticket=None) -> None:
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

        if ticket is None:
            prettier_print(
                "No ticket passed, continuing without commenting"
            )
            return

        if not any([self.queue_url, self.issue_url, self.token, self.email]):
            # none of Jira related variables defined in config, assume we
            # aren't wanting to use Jira and continue
            prettier_print(
                "No required Jira variables set in config, continuing "
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

        comment_url = f"{self.issue_url}/{ticket}/comment"

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
                f"failed to add comment to Jira ticket ({ticket})\n\n"
                f"Status code: {response.status_code}\n\n"
                f"Error response: `{response.text}`"
            )
