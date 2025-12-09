"""
Slack Notification Configuration
Token-based notifications using Slack API
"""

from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.configuration import conf


def get_airflow_ui_url():
    """Get Airflow UI URL from configuration"""
    try:
        base_url = conf.get('webserver', 'base_url', fallback=None)
        if base_url:
            return base_url
    except:
        pass

    return "https://d0bbfc61639643c5add53f27b09c754a-dot-us-central1.composer.googleusercontent.com"


AIRFLOW_UI_URL = get_airflow_ui_url()


def get_task_failure_notification():
    """Returns task failure notification configuration"""
    return send_slack_notification(
        text="❌ *Task Failed*",
        channel="#failure-notifs",
        blocks=[
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*❌ Task Failed*\n\n*Task:* `{{{{ ti.task_id }}}}`\n*DAG:* `{{{{ dag.dag_id }}}}`\n*Run ID:* `{{{{ dag_run.run_id }}}}`\n*Execution Date:* {{{{ ts }}}}\n*Error:* ```{{{{ exception }}}}```\n\n<{AIRFLOW_UI_URL}/dags/{{{{ dag.dag_id }}}}/grid?tab=details&task_id={{{{ ti.task_id }}}}|View Task>"
                }
            }
        ],
        slack_conn_id="slack_failure",
    )


def get_task_success_notification():
    """Returns task success notification configuration"""
    return send_slack_notification(
        text="✅ *Task Succeeded*",
        channel="#success-notifs",
        blocks=[
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*✅ Task Succeeded*\n\n*Task:* `{{{{ ti.task_id }}}}`\n*DAG:* `{{{{ dag.dag_id }}}}`\n*Run ID:* `{{{{ dag_run.run_id }}}}`\n*Execution Date:* {{{{ ts }}}}\n*Duration:* {{{{ ti.duration }}}}s\n\n<{AIRFLOW_UI_URL}/dags/{{{{ dag.dag_id }}}}/grid?tab=details&task_id={{{{ ti.task_id }}}}|View Task>"
                }
            }
        ],
        slack_conn_id="slack_success",
    )


def get_dag_failure_notification():
    """Returns DAG failure notification configuration"""
    return send_slack_notification(
        text="❌ *Pipeline Failed*",
        channel="#failure-notifs",
        blocks=[
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*❌ Pipeline Failed*\n\n*DAG:* `{{{{ dag.dag_id }}}}`\n*Run ID:* `{{{{ dag_run.run_id }}}}`\n*Execution Date:* {{{{ ts }}}}\n\n<{AIRFLOW_UI_URL}/dags/{{{{ dag.dag_id }}}}/grid?tab=graph&dag_run_id={{{{ dag_run.run_id }}}}|View DAG>"
                }
            }
        ],
        slack_conn_id="slack_failure",
    )


def get_dag_success_notification():
    """Returns DAG success notification configuration"""
    return send_slack_notification(
        text="✅ *Pipeline Completed Successfully*",
        channel="#success-notifs",
        blocks=[
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*✅ Pipeline Completed Successfully*\n\n*DAG:* `{{{{ dag.dag_id }}}}`\n*Run ID:* `{{{{ dag_run.run_id }}}}`\n*Execution Date:* {{{{ ts }}}}\n\n<{AIRFLOW_UI_URL}/dags/{{{{ dag.dag_id }}}}/grid?tab=graph&dag_run_id={{{{ dag_run.run_id }}}}|View DAG>"
                }
            }
        ],
        slack_conn_id="slack_success",
    )