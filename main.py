import ast
import sys
import warnings
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

from src import (
    cli, controls, databases, mail, check_delay, check_tasks,
    check_audit, check_quality_masterdata, check_quality_shipment,
    check_quality_duplicates, check_kafka_offset
)

# Remove warnings
warnings.filterwarnings("ignore")

if not load_dotenv():
    print('Error, check if the file .env was created.')
    sys.exit(1)

# Define logger
logger = controls.setup_logging()

# Define configuration variables
config = controls.load_yaml_config('./assets/configs', 'config.yml')
tables = controls.load_yaml_config('./assets/configs', 'tables.yml')
tasks = controls.load_yaml_config('./assets/configs', 'tasks.yml')

def get_snowflake_connection(env):
    conn_snowflake = databases.get_connection(
        db_type=f'snowflake_{env}',
        config=config[f'snowflake_{env}']
    )

    db_snowflake = f'casc_digital_control_tower_{env}'
    sc_snowflake = 'digital_control_tower_trns'

    return conn_snowflake, db_snowflake, sc_snowflake

if __name__ == '__main__':
    # Get arguments from cli
    args = cli.cli()
    print(args)

    # Get snowflake connection
    conn_snowflake, db_snowflake, sc_snowflake = get_snowflake_connection(
        args.env
    )

    if args.type == 'ingestion-delay':
        # Get the differences between source and snowflake
        df = check_delay.get_database_differences(
            args=args,
            conn_snowflake=conn_snowflake,
            db_snowflake=db_snowflake,
            sc_snowflake=sc_snowflake,
            config=config,
            tables=tables,
            logger=logger,
        )

        # Send the report to teams and email alert if was passed on the cli
        if args.send_alerts:
            # Teams report
            df = check_delay.send_teams_report(
                df=df, args=args, config=config, logger=logger
            )
            # Email alerts
            check_delay.send_email_alert(
                df=df, args=args, config=config, logger=logger
            )

    if args.type == 'ingestion-tasks':
        # Run the monitoring to check tasks
        df = check_tasks.get_inactive_tasks_dataframe(
            conn_snowflake=conn_snowflake,
            env=args.env,
            task_list=tasks['tasks'][args.env]
        )

        # Send the email alert if was passed on the cli
        if args.send_alerts:
            check_tasks.send_email_alert(
                df=df, args=args, config=config, logger=logger
            )

    if args.type == 'ingestion-audit':
        # Run the monitoring to check tasks
        df = check_audit.get_database_audit_date_differences(
            conn_snowflake=conn_snowflake,
            source_name=args.source,
            env=args.env,
            tables=tables,
            config=config,
            logger=logger
        )

        # Send the email alert if was passed on the cli
        if args.send_alerts:
            check_audit.send_email_alert(
                df=df, args=args, config=config, logger=logger
            )

    if args.type == 'ingestion-kafka':
        # Get snowflake connection
        conn_snowflake_dev, db_snowflake_dev, sc_snowflake_dev = get_snowflake_connection(
            'dev'
        )
        conn_snowflake_prd, db_snowflake_prd, sc_snowflake_prd = get_snowflake_connection(
            'prd'
        )
        # Get the differences between source and snowflake
        df = check_kafka_offset.get_database_differences(
            args=args,
            conn_snowflake_dev=conn_snowflake_dev,
            conn_snowflake_prd=conn_snowflake_prd,
            tables=tables,
            logger=logger,
        )

        # Send the report to email alert if was passed on the cli
        if args.send_alerts:
            # Email alerts
            check_kafka_offset.send_email_alert(
                df=df, args=args, config=config, logger=logger
            )


    if args.type == 'transform':
        if args.group in ('masterdata', 'all'):
          # Run the monitoring of masterdata data quality
          df = check_quality_masterdata.get_snowflake_masterdata_table(
              conn_snowflake=conn_snowflake,
              env=args.env,
              # tables=tables,
              config=config,
              logger=logger
          )

          # Send the email alert if was passed on the cli
          if args.send_alerts:
              check_quality_masterdata.send_email_alert(
                  df=df, args=args, config=config, logger=logger
              )
        if args.group in ('shipment', 'all'):
          # Run the monitoring of shipment data quality
          df = check_quality_shipment.get_snowflake_shipment_table(
              conn_snowflake=conn_snowflake,
              env=args.env,
              # tables=tables,
              config=config,
              logger=logger
          )

          # Send the email alert if was passed on the cli
          if args.send_alerts:
              check_quality_shipment.send_email_alert(
                  df=df, args=args, config=config, logger=logger
              )

        if args.group in ('duplicates', 'all'):
          # Run the monitoring of duplicates data quality
          df = check_quality_duplicates.get_snowflake_duplicates_table(
              conn_snowflake=conn_snowflake,
              env=args.env,
              # tables=tables,
              config=config,
              logger=logger
          )

          df_details = check_quality_duplicates.get_snowflake_duplicates_details_table(
              conn_snowflake=conn_snowflake,
              env=args.env,
              # tables=tables,
              config=config,
              logger=logger
          )

          # Send the email alert if was passed on the cli
          if args.send_alerts:
              check_quality_duplicates.send_email_alert(
                  df=df, df_details=df_details, args=args, config=config, logger=logger
              )
