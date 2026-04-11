"""
Feature store base — generic Hopsworks connection.

Shared across all sport/market feature pipelines. Each pipeline's own
feature_store.py imports get_feature_store() from here and defines its
sport-specific feature group on top.
"""

import os

import hopsworks
from dotenv import load_dotenv


def get_feature_store():
    load_dotenv()
    project = hopsworks.login(
        project=os.getenv("HOPSWORKS_PROJECT"),
        api_key_value=os.getenv("HOPSWORKS_API_KEY"),
    )
    return project.get_feature_store()
