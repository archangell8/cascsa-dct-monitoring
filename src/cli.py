import argparse
import sys


def cli() -> dict:
    """Create cli for the process.

    Returns:
        dict: Dictionary with the arguments.
    """
    parser = argparse.ArgumentParser(
        description="Monitoring DCT and Snowflake process."
    )

    parser.add_argument(
        '--env',
        required=True,
        choices=['dev', 'prd'],
        help='Enviroment of execution (dev or prd)'
    )

    subparsers = parser.add_subparsers(
        dest='type',
        required=False,
        title='Type of monitoring',
        metavar=''
    )

    ingestion_delay = subparsers.add_parser(
        'ingestion-delay', help='Moitoring snowflake delay ingestions.'
    )

    ingestion_tasks = subparsers.add_parser(
        'ingestion-tasks', help='Moitoring snowflake tasks ingestions.'
    )

    ingestion_audit = subparsers.add_parser(
        'ingestion-audit', help='Moitoring snowflake audit date ingestions.'
    )

    ingestion_kafka = subparsers.add_parser(
        'ingestion-kafka', help='Moitoring snowflake kafka offset ingestions.'
    )

    transform = subparsers.add_parser(
        'transform', help='Moitoring DCT transforms.'
    )


    # INGESTION DELAY
    ingestion_delay.add_argument(
        '--source',
        choices=['jde', 'jde_full', 'pscfl', 'fts', 'rds', 'ctms', 'onet'],
        default='jde',
        help='Source to monitoring (default: jde)'
    )

    ingestion_delay.add_argument(
        '--send-alerts',
        action='store_true',
        help='Send the teams report and email alert.'
    )


    # INGESTION TASKS
    ingestion_tasks.add_argument(
        '--send-alerts',
        action='store_true',
        help='Send the teams report and email alert.'
    )


    # INGESTION AUDIT
    ingestion_audit.add_argument(
        '--source',
        choices=['jde', 'jde_full', 'pscfl', 'fts', 'rds', 'ctms', 'onet'],
        default='jde',
        help='Source to monitoring (default: jde)'
    )

    ingestion_audit.add_argument(
        '--send-alerts',
        action='store_true',
        help='Send the teams report and email alert.'
    )

    # INGESTION KAFKA
    ingestion_kafka.add_argument(
        '--source',
        choices=['jde'],
        default='jde',
        help='Source to monitoring (default: jde)'
    )

    ingestion_kafka.add_argument(
        '--send-alerts',
        action='store_true',
        help='Send the teams report and email alert.'
    )

    # TRANSFORMATION MONITORING
    transform.add_argument(
        '--send-alerts',
        action='store_true',
        help='Send the teams report and email alert.'
    )

    transform.add_argument(
        '--group',
        choices=['masterdata', 'shipment', 'duplicates', 'all'],
        help='Define model or model group to be monitored.'
    )
    return parser.parse_args()
