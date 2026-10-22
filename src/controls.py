import logging
import os
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import yaml
from jinja2 import Template

from src import databases


def setup_logging() -> logging.Logger:
    """Define logger object.

    Returns:
        object: logger.
    """
    logger = logging.getLogger('logger')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger

def get_current_julian_date() -> int:
    """Method to get the current julian date.

    Returns:
        str: Current julian date.
    """
    now = datetime.now()

    julian_date = (now.year - 1900) * 1000 + now.timetuple().tm_yday

    return julian_date

def load_yaml_config(path: str, filename: str) -> dict[str, Any]:
    """Carrega arquivo YAML substituindo variáveis de ambiente"""
    with open(f'{path}/{filename}') as f:
        config = yaml.safe_load(f)
    
    # Expande variáveis de ambiente
    for key, value in config.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, list):
                    config[key][sub_key] = [
                        os.path.expandvars(item) for item in sub_value
                    ]
                else:
                    config[key][sub_key] = os.path.expandvars(str(sub_value))
        elif isinstance(value, str):
            config[key] = os.path.expandvars(value)
    
    return config

def read_query(path: str, filename: str, params: dict[str, Any]) -> str:
    """Method to read a query file and apply a jinja template.

    Args:
        path (str): Directory that file was saved.
        filename (str): Query file.
        query_params (dict): Dict with values to replace on query template.

    Returns:
        str: Query with tempalte applied.
    """
    with open(f'{path}/{filename}', encoding='utf-8') as f:
        sql = f.read()
    
    return Template(sql).render(params)

def validate_arguments(env: str, sources: list) -> None:
    """Valida argumentos da linha de comando"""
    if env not in ['dev', 'prd']:
        raise ValueError("Ambiente deve ser 'dev' ou 'prd'")
    
    valid_sources = ['jde', 'jde_full', 'pscfl', 'fts', 'rds']
    if not all(source in valid_sources for source in sources):
        raise ValueError(f"Fontes inválidas. Opções válidas: {valid_sources}")
    
    return True

def transform_julian_to_gregorian(julian_date: int) -> str:
    """Converte data juliana para formato gregoriano YYYY-MM-DD"""
    julian_str = str(julian_date)
    year = int(julian_str[:3]) + 1900
    day_of_year = int(julian_str[3:])
    gregorian_date = datetime(year, 1, 1) + timedelta(days=day_of_year - 1)
    return gregorian_date.strftime('%Y-%m-%d')

