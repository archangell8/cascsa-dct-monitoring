import pandas as pd
import logging

from pathlib import Path

from . import databases, mail

def fetch_tasks_dataframe(conn_snowflake, env: str) -> pd.DataFrame:
    """Fetch tasks from Snowflake databases depending on the environment.
        env = 'prd' -> only class_prd
        env = 'dev' -> class_qas + class_uat

    Parameters:
        conn_snowflake : Snowflake connector connection.
        env : 'prd' or 'dev'.

    Returns:
        Pandas DataFrame: Dataframe with the data.
    """
    sql = "SHOW TASKS IN DATABASE {0}"

    if env == 'dev':
        # Get tasks from class_qas
        df_qas = databases.execute_query(
            conn_snowflake, sql.format('class_qas')
        )

        # Get tasks from class_uat
        df_uat = databases.execute_query(
            conn_snowflake, sql.format('class_uat')
        )

        # Remove rows from class_qas where schema == CLASS_RAW_QAS
        # It's because we dont use the CLASS_QAS to JDE
        df_qas = df_qas[df_qas['SCHEMA_NAME'] != "CLASS_RAW_QAS"]

        # Concatenate
        df = pd.concat([df_qas, df_uat], ignore_index=True)

    elif env == 'prd':
        df = databases.execute_query(conn_snowflake, sql.format('class_prd'))

    else:
        raise ValueError("Environment should be 'prd' or 'dev'.")

    return df


def get_inactive_tasks_dataframe(
    conn_snowflake,
    env: str,
    task_list: list = None,
) -> pd.DataFrame:
    """
    Return a DataFrame with NON-active tasks for the given environment.
    Columns returned:
        NAME, DATABASE_NAME, SCHEMA_NAME, LAST_SUSPENDED_REASON, SCHEDULE.

    Parameters:
        conn_snowflake : Snowflake connector connection.
        env : 'prd' or 'dev'.
        task_list (list): iterable of task names to filter (optional).

    Returns:
        pd.DataFrame with the inactive tasks and the requested columns.
    """
    df_all_tasks = fetch_tasks_dataframe(conn_snowflake, env)

    # Trasnform values of the task list to upper
    task_list = [t.upper() for t in task_list]

    # Filter by task names
    df_tasks = df_all_tasks[df_all_tasks["NAME"].isin(task_list)]

    if df_tasks.empty:
        return pd.DataFrame()

    # Determine inactive
    df_tasks_error = df_tasks[df_tasks["STATE"].str.lower() != 'started']

    if df_tasks_error.empty:
        return pd.DataFrame()

    # Ensure required columns
    rename_cols = {
        "NAME": "TASK_NAME",
        "STATE": "STATE",
        "DATABASE_NAME": "DATABASE",
        "SCHEMA_NAME": "SCHEMA",
        "LAST_SUSPENDED_REASON": "SUSPENDED_REASON",
        "SCHEDULE": "CRONTAB",
    }

    # Filter just necessary columns
    df_tasks_error = (
        df_tasks_error[rename_cols.keys()]
    ).rename(columns=rename_cols)

    return df_tasks_error

def get_previous_alerts(alert_file) -> pd.DataFrame:
    """
    Load the DataFrame with previously reported inactive tasks from the last alert.

    Parameters:
        alert_file (str or Path): File path to the CSV containing task alerts from the previous run.

    Returns:
        pd.DataFrame: DataFrame containing the previous inactive tasks.
        If the file does not exist or is empty, returns an empty DataFrame.
    """
    path = Path(alert_file)
    if path.exists():
        try:
            return pd.read_csv(path)
        except pd.errors.EmptyDataError:
            return pd.DataFrame()
    else:
        return pd.DataFrame()

def save_current_alerts(df, alert_file):
    """Save the current DataFrame of the alerts to a CSV file for persistence
    between executions.

    Parameters:
        df (pd.DataFrame): DataFrame of alerts from the current run.
        alert_file (Path or str): File path where the DataFrame should be saved.
    """
    df.to_csv(alert_file, index=False)

def get_persisted_alerts(current_df, previous_df) -> pd.DataFrame:
    """
    Returns only the alerts that are present both in current_df and previous_df,
    based on the key column.

    Parameters:
        current_df (pd.DataFrame): DataFrame of the alerts from the current run.
        previous_df (pd.DataFrame): DataFrame of the alerts from the previous run.

    Returns:
        pd.DataFrame: DataFrame containing only the persisted alerts.
    """
    # If one of the dataframes is empty, an empty dataframe is returned.
    # The only case of an alert is if there is data in both dataframes,
    #   as this would be an alert that existed in the last execution and
    #   in the current execution.
    if current_df.empty or previous_df.empty:
        return pd.DataFrame()

    # Merge only on TASK_NAME, keep columns from current_df (avoid duplicates)
    persisted = (
        current_df[current_df['TASK_NAME'].isin(previous_df['TASK_NAME'])]
    )

    return persisted

def send_email_alert(
    df: pd.DataFrame,
    args: dict,
    config: dict,
    logger: logging.Logger
) -> bool:
    alert_file = Path(__file__).parent.parent / f"output/last_alert_tasks_{args.env}.csv"
    # Get last alerts and compare with the new
    df_previous = get_previous_alerts(alert_file)
    df_persisted = get_persisted_alerts(df, df_previous)

    if df_persisted.empty:
        logger.info('All tasks are running, nothing to send in the e-mail.')
        save_current_alerts(df, alert_file)
        return True

    logger.info('Sending email alert to recipients...')

    # Build HTML table with the necessary info
    html_table = df_persisted.to_html(index=False, escape=False, border=1)

    # Read the HTML template
    base_dir = Path(__file__).parent.parent.resolve()
    with open(
        f"{base_dir}/assets/html/email_body_alert_tasks.html",
        encoding="utf-8"
    ) as f:
        html_template = f.read()

    # Replace fields
    body = html_template.format(
        environment=args.env.upper(),
        html_table=html_table
    )

    subject = f"[{args.env.upper()}] DCT Ingestion Tasks Alert"

    email_config = {
        "email_server": config['email']['server'],
        "sender": config['email']['sender'],
        "recipients": config['email']['recipients'],
        "subject": subject,
        "body": body,
        "maintype": "html",
        "subtype": "html"
    }

    mail.send_email(email_config)
    save_current_alerts(df, alert_file)

    return True