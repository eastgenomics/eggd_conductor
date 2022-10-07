from datetime import datetime
import json
import os
import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from urllib3.util import Retry


class Slack():
    """
    Slack related functions
    """
    def __init__(self) -> None:
        self.slack_token = os.getenv("SLACK_TOKEN")
        self.slack_alert_channel = os.getenv("SLACK_ALERT_CHANNEL")


    def send(self, message, exit_fail=True):
        """
        Send alert to Slack

        Parameters
        ----------
        message : str
            message to send to Slack
        exit_fail : bool
            if the alert is being sent when exiting and to create the
            slack_fail_sent.log file. If false, this will be for an alert

        """
        conductor_job_url = os.environ.get('conductor_job_url')
        channel = self.slack_alert_channel
        message = (
            f":warning: *Error in eggd_conductor*\n\n{message}\n\n"
            f"eggd_conductor job: {conductor_job_url}"
        )

        print(f"Sending message to Slack channel {channel}\n\n{message}")

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
                print(f"Error in sending slack notification: {response.get('error')}")
        except Exception as err:
            print(f"Error in sending post request for slack notification: {err}")

        if exit_fail:
            # write file to know in bash script an alert already sent
            open('slack_fail_sent.log', 'w').close()


class Jira():
    """
    Jira related functions for getting sequencing run ticket for a given
    run to tag analysis links to
    """
    def __init__(self) -> None:
        self.url = os.environ.get('JIRA_URL')
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
        start = 0
        response_data = []

        while True:
            response = self.http.get(
                f"{self.url}?start={start}",
                headers=self.headers,
                auth=self.auth
            ).json()

            if response['size'] == 0:
                break

            response_data.extend(response['values'])
            start += 50

        return list(set(response_data))
    

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
        run_ticket = [
            x['id'] for x in tickets if run_id in x['fields']['summary']
        ]

        if not run_ticket:
            # didn't find a ticket -> either a typo in the name or ticket
            # has not yet been raised / forgotten about, send an alert and
            # continue with things since linking to Jira is non-essential
            message = ""
            Slack().send(message=message, exit_fail=False)
            return None

        if len(run_ticket) > 1:
            # found multiple tickets, this should not happen so send us an
            # alert and don't touch the tickets
            message = ""
            Slack().send(message=message, exit_fail=False)
            return None
        
        return run_ticket[0]

    
    def add_comment(self, run_id, comment):
        """
        Find Jira ticket for given run ID and add internal comment

        Parameters
        ----------
        run_id : str
            ID of sequencing run
        comment : str
            comment to add to Jira ticket
        """
        print("Finding Jira ticket to add comment to")
        tickets = self.get_all_tickets()
        ticket_id = self.get_run_ticket_id(run_id=run_id, tickets=tickets)
        print(f"Found Jira ticket ID {ticket_id} for run {run_id}")

        if not ticket_id:
            # no ticket found or more than one ticket found, Slack alert
            # will have been sent in get_run_ticket_id()
            return
        
        comment_url = (
            "https://cuhbioinformatics.atlassian.net/rest/api/3/"
            f"issue/{ticket_id}/comment"
        )

        payload = json.dumps({
            "body": {
                "type": "doc",
                "version": 1,
                "content": [
                {
                    "type": "paragraph",
                    "content": [
                    {
                        "text": f"{comment}",
                        "type": "text"
                    }
                    ]
                }
                ]
            },
            "properties": [{
                "key": "sd.public.comment",
                "value": {
                    "internal": True
                }
            }]
        })

        response = self.http.post(
            comment_url,
            data=payload,
            headers=self.headers,
            auth=self.auth
        )

        if not response.status_code == 201:
            # some kind of error occurred adding Jira comment =>
            # send a non-exiting Slack alert 
            message = (
                f"failed to add comment to Jira ticket ({ticket_id})\n\n"
                f"Status code: {response.status_code}\n\n"
                f"Error response: `{response.text}`"
            )
            Slack().send(message=message, exit_fail=False)


        


def time_stamp() -> str:
    """
    Returns string of date & time formatted as YYMMDD_HHMM

    Returns
    -------
    str
        String of current date and time as YYMMDD_HHMM
    """
    return datetime.now().strftime("%y%m%d_%H%M")
