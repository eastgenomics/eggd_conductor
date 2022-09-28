from datetime import datetime
import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


class Slack():
    """
    Slack related functions
    """
    def __init__(self) -> None:
        self.slack_token = os.getenv("SLACK_TOKEN")
        self.slack_alert_channel = os.getenv("SLACK_ALERT_CHANNEL")


    def send(self, message):
        """
        Send alert to Slack

        Parameters
        ----------
        message : str
            message to send to Slack

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

        # write file to know in bash script an alert already sent
        open('slack_fail_sent.log', 'w').close()


def time_stamp() -> str:
    """
    Returns string of date & time formatted as YYMMDD_HHMM

    Returns
    -------
    str
        String of current date and time as YYMMDD_HHMM
    """
    return datetime.now().strftime("%y%m%d_%H%M")
