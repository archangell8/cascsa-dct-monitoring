import ast
import logging
import pandas as pd

from pathlib import Path

from . import controls, databases, mail

PATH_HOME = Path(__file__).parent.parent

def get_database_audit_date_differences(
    conn_snowflake,
    source_name: str,
    env: str,
    tables: dict,
    config: dict,
    logger: logging.Logger
) -> pd.DataFrame:
    """Compares MAX_AUDIT_DATE between source and Snowflake for a table.

    Args:
        conn_snowflake: Connection object to Snowflake.
        source_name (str): Name of the source system.
        env (str): Environment (e.g., 'prd', 'dev').
        tables (dict): Dict with tablename as key / audit column name as value.
        config (dict): Dict with the configs.
        logger (logger): Object logger.

    Returns:
        pd.DataFrame: DataFrame with columns TABELA, MAX_AUDIT_DATE_SOURCE,
        and MAX_AUDIT_DATE_SNOWFLAKE.
    """
    # Create source connection
    conn_source = databases.get_connection(
        db_type='transactional',
        config=config[source_name]
    )

    # Get the dictionary of tables
    dict_tables = ast.literal_eval(tables['tables'][source_name])

    logger.info(f"Reading audit timestamp from {source_name.upper()}...")

    results = []
    for schema, tables in dict_tables.items():
        for table, details in tables.items():
            # Check if the table has audit column
            if details['audit_column'] == 'None':
                continue

            logger.info(f"\tReading table: {table}")
            # Get query to extract snowflake info
            query_snowflake = controls.read_query(
                './assets/sql/check_audit',
                f'get_max_audit_snowflake_{source_name}.sql',
                params={'table': table, 'env': env, **details}
            )

            # Get query to extract source info
            query_source = controls.read_query(
                './assets/sql/check_audit',
                f'get_max_audit_source_{source_name}.sql',
                params={'table': table, 'schema': schema, **details}
            )

            df_sf = databases.execute_query(conn_snowflake, query_snowflake)
            df_source = databases.execute_query(conn_source, query_source)

            df_merge = pd.merge(
                df_sf,
                df_source,
                on='TABELA',
                how='outer'
            )

            df_merge = df_merge[
                ['TABELA', 'MAX_AUDIT_DATE_SOURCE', 'MAX_AUDIT_DATE_SNOWFLAKE']
            ]

            # Normalize date columns to datetime and format
            for col in ['MAX_AUDIT_DATE_SOURCE', 'MAX_AUDIT_DATE_SNOWFLAKE']:
                df_merge[col] = pd.to_datetime(
                    df_merge[col], errors='coerce'
                ).dt.strftime('%Y-%m-%d %H:%M:%S')

            # Calculate difference in hours (absolute, ignoring NaT)
            dt_source = pd.to_datetime(
                df_merge['MAX_AUDIT_DATE_SOURCE'], errors='coerce'
            )
            dt_snowflake = pd.to_datetime(
                df_merge['MAX_AUDIT_DATE_SNOWFLAKE'], errors='coerce'
            )

            # Get the dataframe difference
            diff_days = (dt_snowflake - dt_source).abs() / pd.Timedelta(days=1)
            df_merge['DIFF_DAYS'] = diff_days

            results.append(df_merge)

    df_final = pd.concat(results, ignore_index=True)

    print(df_final)

    return df_final

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

    # Merge only on TABELA, keep columns from current_df (avoid duplicates)
    persisted = (
        current_df[current_df['TABELA'].isin(previous_df['TABELA'])]
    )

    return persisted

def send_email_alert(
    df: pd.DataFrame,
    args: dict,
    config: dict,
    logger: logging.Logger
) -> True:
    # Change the days to generate alert based on the source
    if args.source == 'jde':
        time_alert = 2
    else:
        time_alert = 1

    # File to save the current alerts
    alert_file = f"{PATH_HOME}/output/last_alert_audit_{args.source}_{args.env}.csv"

    # Filter rows to alert
    df_alert = df[pd.to_numeric(df['DIFF_DAYS']) >= time_alert]

    # If we have any data in the dataframe
    if df_alert.empty:
        logger.info(
            f'All tables are updated in the last {time_alert} day(s). '
            'Nothing to do.'
        )
        save_current_alerts(df_alert, alert_file)
        return True

    # Get last alerts and compare with the new outdated rows
    df_previous = get_previous_alerts(alert_file)
    df_persisted = get_persisted_alerts(df_alert, df_previous)

    if df_persisted.empty:
        logger.info(
            f'There are outdated tables  in the last {time_alert} day(s) but '
            'the next run will be awaited to check if the problem persists.'
        )
        save_current_alerts(df_alert, alert_file)
        return True

    try:
        logger.info('Sending email alert to recipients...')

        # Setup output directory
        base_dir = Path(__file__).parent.parent.resolve()
        output_dir = base_dir / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        report_path = output_dir / f"report_audit_{args.source}.csv"

        # Save report
        df_persisted.to_csv(report_path, index=False, encoding='utf-8')


        # Build HTML table with the necessary info
        html_table = df_persisted.to_html(index=False, escape=False, border=1)

        # Read the HTML template
        with open(
            f"{base_dir}/assets/html/email_body_alert_audit.html",
            encoding="utf-8"
        ) as f:
            html_template = f.read()

        # PReplace fields
        body = html_template.format(
            source_db=args.source.upper(),
            environment=args.env.upper(),
            time_alert=time_alert,
            html_table=html_table
        )

        subject = (
            f"[{args.env.upper()}] DCT Ingestion Audit Date Alert "
            f"- {args.source.upper()} Source"
        )

        email_config = {
            "email_server": config['email']['server'],
            "sender": config['email']['sender'],
            "recipients": config['email']['recipients'],
            "subject": subject,
            "body": body,
            "maintype": "html",
            "subtype": "html"
        }

        mail.send_email(email_config, str(report_path))

        save_current_alerts(df_alert, alert_file)

    finally:
        try:
            if report_path.exists():
                report_path.unlink()
        except Exception as e:
            logger.warning(
                f"Could not remove temporary report file {report_path}: {e}"
            )

    return True