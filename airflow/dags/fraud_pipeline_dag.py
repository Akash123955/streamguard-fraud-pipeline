"""
StreamGuard — Airflow DAG
==========================
Schedules dbt runs daily at 2am to refresh Bronze → Silver → Gold models.
Sends Slack alerts if any step fails.

How to run Airflow locally:
  pip install apache-airflow
  airflow db init
  airflow webserver --port 8082 &
  airflow scheduler &
  # Then open http://localhost:8082 and enable this DAG

How to add your Slack webhook:
  airflow connections add slack_webhook \
    --conn-type http \
    --conn-host hooks.slack.com \
    --conn-password /services/YOUR/WEBHOOK/PATH
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.dates import days_ago

# ── Default args applied to all tasks ────────────────────────────────────────
default_args = {
    "owner": "streamguard",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="streamguard_fraud_pipeline",
    description="Daily dbt Bronze→Silver→Gold refresh for StreamGuard",
    default_args=default_args,
    schedule_interval="0 2 * * *",   # 2am every day
    start_date=days_ago(1),
    catchup=False,
    tags=["streamguard", "fraud", "dbt"],
) as dag:

    # ── Task 1: Check data freshness ─────────────────────────────────────────
    # Before running dbt, verify that new data arrived in TRANSACTIONS_RAW
    # within the last 2 hours. If not, the pipeline may be broken.
    check_freshness = BashOperator(
        task_id="check_data_freshness",
        bash_command="""
            python3 -c "
import snowflake.connector, os
conn = snowflake.connector.connect(
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    database='STREAMGUARD',
    schema='PUBLIC',
)
cursor = conn.cursor()
cursor.execute('''
    SELECT COUNT(*) FROM TRANSACTIONS_RAW
    WHERE ingested_at >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
''')
count = cursor.fetchone()[0]
print(f'Rows ingested in last 2 hours: {count}')
if count == 0:
    raise ValueError('No data in last 2 hours — pipeline may be stalled!')
conn.close()
"
        """,
    )

    # ── Task 2: dbt Bronze ───────────────────────────────────────────────────
    dbt_bronze = BashOperator(
        task_id="dbt_bronze",
        bash_command="cd /opt/airflow/dbt && dbt run --select bronze --profiles-dir .",
    )

    # ── Task 3: dbt Silver ───────────────────────────────────────────────────
    dbt_silver = BashOperator(
        task_id="dbt_silver",
        bash_command="cd /opt/airflow/dbt && dbt run --select silver --profiles-dir .",
    )

    # ── Task 4: dbt Gold ─────────────────────────────────────────────────────
    dbt_gold = BashOperator(
        task_id="dbt_gold",
        bash_command="cd /opt/airflow/dbt && dbt run --select gold --profiles-dir .",
    )

    # ── Task 5: dbt tests ────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir .",
    )

    # ── Task 6: Refresh Snowflake customer risk profiles ─────────────────────
    refresh_profiles = BashOperator(
        task_id="refresh_customer_risk_profiles",
        bash_command="""
            python3 -c "
import snowflake.connector, os
conn = snowflake.connector.connect(
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    database='STREAMGUARD',
    schema='PUBLIC',
)
cursor = conn.cursor()
cursor.execute('CALL refresh_customer_risk_profiles()')
result = cursor.fetchone()
print(result[0])
conn.close()
"
        """,
    )

    # ── Slack alert on failure ────────────────────────────────────────────────
    def build_failure_message(context):
        """Called by on_failure_callback — formats a Slack alert."""
        task_id = context["task_instance"].task_id
        dag_id = context["dag"].dag_id
        exec_date = context["execution_date"]
        return f":red_circle: *StreamGuard Pipeline Failed*\nDAG: `{dag_id}`\nTask: `{task_id}`\nTime: `{exec_date}`"

    slack_alert = SlackWebhookOperator(
        task_id="slack_failure_alert",
        slack_webhook_conn_id="slack_webhook",
        message=":red_circle: StreamGuard dbt pipeline failed! Check Airflow logs.",
        trigger_rule="one_failed",   # Only runs if any upstream task fails
    )

    # ── Task dependencies (execution order) ──────────────────────────────────
    # check_freshness → dbt_bronze → dbt_silver → dbt_gold → dbt_test → refresh_profiles
    # If anything fails → slack_alert
    (
        check_freshness
        >> dbt_bronze
        >> dbt_silver
        >> dbt_gold
        >> dbt_test
        >> refresh_profiles
        >> slack_alert
    )
