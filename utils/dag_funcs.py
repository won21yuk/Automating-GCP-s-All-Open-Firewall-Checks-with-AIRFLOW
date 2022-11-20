"""
pip install oauth2client
pip install google-api-python-client
pip install tabulate
pip install pandas
pip install apache-airflow-providers-slack
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from pendulum import yesterday

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

import pandas as pd

class auto_firewall_check:
    def __init__(self):
        pass

    # GCP All-Open 방화벽 체크 메서드
    def firewall_checks(self):
            credentials = GoogleCredentials.get_application_default()

            service = discovery.build('compute', 'v1', credentials=credentials)

            # Project ID for this request.
            project = 'angelic-turbine-366714'
            # TODO: Update placeholder value.
            request = service.firewalls().list(project=project)
            lst = []
            while request is not None:
                response = request.execute()
                for firewall in response['items']:
                    # TODO: Change code below to process each `firewall` resource
                    sourceRanges = firewall['sourceRanges']
                    # all open 방화벽 여부 체크
                    if '0.0.0.0/0' not in sourceRanges:
                        continue
                    # json 형태로 만들기
                    firewall_id = firewall['id']
                    firewall_name = firewall['name']
                    firewall_network = firewall['network'].split('/')[-1]
                    traffic_direction = firewall['direction']
                    creation_date = firewall['creationTimestamp']
                    dic = {}
                    dic['firewall_id'] = firewall_id
                    dic['firewall_name'] = firewall_name
                    dic['firewall_network'] = firewall_network
                    dic['traffic_direction'] = traffic_direction
                    dic['creation_date'] = creation_date
                    lst.append(dic)
                request = service.firewalls().list_next(previous_request=request, previous_response=response)
            df = pd.DataFrame(lst)
            return df

    # exception이 작동하여 강제로 dag 실패시키기 위한 더미 메서드
    def exception_method(self, **context):
        raise AirflowException("Exception Happen")

    # failure callback
    def on_failure_callback(self, context):
        # 전송할 메세지
        slack_msg = """
            :red_circle: Task Failed.
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            """.format(
                task=context.get('task_instance').task_id,
                dag=context.get('task_instance').dag_id,
                ti=context.get('task_instance'),
                exec_date=context.get('execution_date'),
                log_url=context.get('task_instance').log_url,
            )
        # SlackWebhookOperator 사용하여 http 통신
        failed_alert = SlackWebhookOperator(
            task_id='slack_notification_failure',
            http_conn_id='slack_webhook',
            message=slack_msg)
        return failed_alert.execute(context=context)

    # success callback
    def on_success_callback(self, context):
        # df 형태로 all-open 방화벽 가져오기
        df = self.firewall_checks()
        # 전송할 메세지
        message_result = ("GCP 인스턴스 방화벽에 0.0.0.0/0 으로 오픈 된 방화벽 정책이 있습니다.\n\n"
                          + "```"
                          + df.to_markdown()
                          + "```"
                          + "\n")
        slack_message = ":bell:" + " *방화벽 모니터링* \n" + message_result
        # SlackWebhookOperator 사용하여 http 통신
        success_alert = SlackWebhookOperator(
            task_id='slack_notification_success',
            http_conn_id='slack_webhook',
            message=slack_message
        )
        return success_alert.execute(context=context)

