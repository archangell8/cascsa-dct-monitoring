import ast
import logging
import pandas as pd

from pathlib import Path

from . import controls, databases, mail

def get_snowflake_duplicates_table(
    conn_snowflake,
    env: str,
    # tables: dict,
    config: dict,
    logger: logging.Logger
) -> pd.DataFrame:
    """Return new table from union of all duplacates monitoring tables if has data

    Args:
        conn_snowflake: Connection object to Snowflake.
        env (str): Environment (e.g., 'prd', 'dev').
        tables (dict): Dict with tablename as key / audit column name as value. --not necessary for now
        config (dict): Dict with the configs.
        logger (logger): Object logger.

    Returns:
        pd.DataFrame: DataFrame with columns ENTITY, AND COUNT_DUPLICATES.
    """
    # Get the dictionary of tables
    # dict_tables = ast.literal_eval(tables['tables'][source_name])
    list_tables = [
      'tab_monitoring_duplicates_capacity',
      'tab_monitoring_duplicates_demand',
      'tab_monitoring_duplicates_master_data',
      'tab_monitoring_duplicates_parameters',
      'tab_monitoring_duplicates_transactional',
    ]

    logger.info(f"Reading sf tables {list_tables}...")
    
    results = []
    
    # Get query to extract snowflake info
    query_snowflake = controls.read_query(
        './assets/sql/check_quality',
        'duplicates.sql',
        params={'tables': list_tables, 'env': env}
    )

    read_sf = databases.execute_query(conn_snowflake, query_snowflake)

    print(read_sf)

    return read_sf

def get_snowflake_duplicates_details_table(
    conn_snowflake,
    env: str,
    # tables: dict,
    config: dict,
    logger: logging.Logger
) -> pd.DataFrame:
    """Return new table from union of all duplacates monitoring tables if has data

    Args:
        conn_snowflake: Connection object to Snowflake.
        env (str): Environment (e.g., 'prd', 'dev').
        tables (dict): Dict with tablename as key / audit column name as value. --not necessary for now
        config (dict): Dict with the configs.
        logger (logger): Object logger.

    Returns:
        pd.DataFrame: DataFrame with columns ENTITY, ENTITY_KEY, QUANTITY_OF_DUPLICATES.
    """
    # Get the dictionary of tables
    # dict_tables = ast.literal_eval(tables['tables'][source_name])
    list_tables = [
      'tab_monitoring_duplicates_capacity',
      'tab_monitoring_duplicates_demand',
      'tab_monitoring_duplicates_master_data',
      'tab_monitoring_duplicates_parameters',
      'tab_monitoring_duplicates_transactional',
    ]

    logger.info(f"Reading sf tables {list_tables} details")
    
    results = []
    
    query_snowflake_details = controls.read_query(
        './assets/sql/check_quality',
        'duplicates_details.sql',
        params={'tables': list_tables, 'env': env}
    )

    read_sf_details = databases.execute_query(conn_snowflake, query_snowflake_details)

    print(read_sf_details)

    return read_sf_details


def send_email_alert(
    df: pd.DataFrame,
    df_details: pd.DataFrame,
    args: dict,
    config: dict,
    logger: logging.Logger
) -> True:

    # Filter rows to alert
    df_alert = df.sort_values(by=['QUANTITY_OF_DUPLICATES'], ascending=False)
    df_det = df_details.sort_values(by=['ENTITY'])

    # If we have any data in the dataframe
    if df_alert.empty:
        logger.info(
            f'All tables are good. '
            'Nothing to do.'
        )
        return True

    logger.info('Sending email alert to recipients...')

    # Setup output directory
    base_dir = Path(__file__).parent.parent.resolve()
    output_dir = base_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / f"report_transformation_duplicates.csv"

    try:
        # Save report
        df_det.to_csv(report_path, index=False, encoding='utf-8')
        

        # Build HTML table with the necessary info
        html_table = df_alert.to_html(index=False, escape=False, border=1)
        html_table = html_table.replace('<tr>', '<tr align="center">')
        html_table = html_table.replace('<td>', '<td style="padding: 5 10px;">')
        html_table = html_table.replace('<th>', '<th style="padding: 5 10px;">')
        
        # Read the HTML template
        with open(
            f"{base_dir}/assets/html/email_body_alert_transf.html",
            encoding="utf-8"
        ) as f:
            html_template = f.read()        

        # PReplace fields
        body = html_template.format(
            environment=args.env.upper(),
            html_table=html_table,
            table='staging tables',
            name='Duplicates'
        )
        # print(body)

        subject = (
            f"[{args.env.upper()}] DCT Data Quality Duplicates Alert "
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

    finally:
        try:
            if report_path.exists():
                report_path.unlink()
        except Exception as e:
            logger.warning(
                f"Could not remove temporary report file {report_path}: {e}"
            )

    return True

