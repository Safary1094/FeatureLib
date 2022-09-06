import datetime
import os
from abc import ABC, abstractmethod
from typing import Dict

import dask.dataframe as dd
import pandas as pd
from loguru import logger


class Engine:
    def __init__(self, tables: Dict[str, dd.DataFrame]):
        self.tables = tables

    def register_table(self, table: dd.DataFrame, name: str) -> None:
        self.tables[name] = table

    def get_table(self, name: str) -> dd.DataFrame:
        return self.tables[name]


class FeatureCalcer(ABC):
    name = '_base'
    keys = None

    def __init__(self, engine: Engine):
        self.engine = engine

    @abstractmethod
    def compute(self):
        pass


class DateFeatureCalcer(FeatureCalcer):
    def __init__(self, date_to: datetime.date, **kwargs):
        self.date_to = date_to
        super().__init__(**kwargs)


class FeatureStore:
    static_features_path = 'data/static_features.parquet'
    events_features_path = 'data/events_features.parquet'

    def __int__(self):
        logger.info('Loading FeatureStore')
        self.events, self.static = self.load()

    def compute_features(self, features_config):
        calcers = list()
        keys = None

        for feature_config in features_config:
            calcer_args = feature_config["args"]
            calcer_args["engine"] = engine

            calcer = create_calcer(feature_config["name"], **calcer_args)
            if keys is None:
                keys = set(calcer.keys)
            elif set(calcer.keys) != keys:
                raise KeyError(f"{calcer.keys}")

            calcers.append(calcer)

        computation_results = []
        for calcer in calcers:
            computation_results.append(calcer.compute())
        result = join_tables(computation_results, on=list(keys), how='outer')

        return result

    def set(self):
        pass

    def get(self):

    def load(self):
        features = []
        for path, name in zip([self.static_features_path, self.static_features_path],
                              ['Static', 'Events']):
            if os.path.exists(path):
                logger.info(f'Reading {path}')
                features.append(pd.read_parquet(path))
            else:
                logger.info(f'File {path} does not exist. {name} features are initialized with empty table')
                features.append(None)

        return features

#################################################################
# CALCER_REFERENCE = {}
#
#
# def register_calcer(calcer_class) -> None:
#     CALCER_REFERENCE[calcer_class.name] = calcer_class
#
#
# def create_calcer(name: str, **kwargs) -> FeatureCalcer:
#     return CALCER_REFERENCE[name](**kwargs)
#
#
# def join_tables(tables: List[dd.DataFrame], on: List[str], how: str) -> dd.DataFrame:
#     result = tables[0]
#     for table in tables[1:]:
#         result = result.merge(table, on=on, how=how)
#     return result
#
#
# def compute_features(engine: Engine, features_config: dict) -> dd.DataFrame:
#     calcers = list()
#     keys = None
#
#     for feature_config in features_config:
#         calcer_args = feature_config["args"]
#         calcer_args["engine"] = engine
#
#         calcer = create_calcer(feature_config["name"], **calcer_args)
#         if keys is None:
#             keys = set(calcer.keys)
#         elif set(calcer.keys) != keys:
#             raise KeyError(f"{calcer.keys}")
#
#         calcers.append(calcer)
#
#     computation_results = []
#     for calcer in calcers:
#         computation_results.append(calcer.compute())
#     result = join_tables(computation_results, on=list(keys), how='outer')
#
#     return result
