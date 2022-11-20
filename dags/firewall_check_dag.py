from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import yesterday
from utils.dag_funcs import auto_firewall_check

# 인스턴스 선언
auto = auto_firewall_check()

# arguments 작성
default_args = {
    "owner": "won21yuk",
    "email": ["won21yuk@gmail.com"],
    "email_on_failure": True,
    "on_failure_callback": auto.on_failure_callback,
    "on_success_callback": auto.on_success_callback
}

# dag 선언
dag = DAG(
    dag_id="Automating_with_AIRFLOW",
    schedule_interval=None,
    start_date=yesterday("Asia/Seoul"),
    default_args=default_args
)


# 방화벽 체크가 정상 작동되는지 확인하는 테스크
# 방화벽 체크가 정상적으로 되는지에 따라 failure나 success callback함수 작동하도록하기 위함
t1 = PythonOperator(
        task_id="firewall_check",
        python_callable=auto.firewall_checks,
        dag=dag
        )
"""
# email과 slack으로 에러메세지 전송하기 위해 강제로 실패하도록 만든 테스크
t2 = PythonOperator(
        task_id="email",
        python_callable=auto.exception_method,
        dag=dag
        )
"""

# t1 >> t2
t1
