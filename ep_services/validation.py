import os
import logging
import json
from datetime import (date, datetime)
from utils import (Nulls, Enum, get_config, get_db_engine, get_error_msg)

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class DataValidation:
    def __init__(self, _target_table, _df_test_data=None):
        self.target_table = _target_table
        self.df_test_data = _df_test_data
        self.sql_assertion_only = True if self.df_test_data is None else False

        self.db_engine = get_db_engine()
        self.log_header = ['Target Table', 'Column Name', 'Test', 'Value Config', 'Severity', 'Validated', 'Failure Data']
        self.validation_logs = []
        self.df_logs = pd.DataFrame()

    def __call__(self, *args, **kwargs):
        self.validate()

    def validate(self):
        try:
            _test = {
                'Freshness': self.__check_freshness__,
                'Unique': self.__check_unique__,
                'Not Null': self.__check_not_null__,
                'Reference Values': self.__check_reference_values__,
                'Accepted Values': self.__check_accepted_values__,
                'SQL-Assertion': self.__check_sql_assertion__
            }

            with self.db_engine.connect() as db_conn:
                # get all validations from configuration for target_table
                _sql_text = "select distinct TargetTable, ColumnName, Test, ValueConfig, Severity " \
                            "from _DataValidationCfg where TargetTable = '{}' " \
                            "order by ColumnName, Test".format(self.target_table)
                df_validation_cfg = pd.read_sql_query(_sql_text, db_conn)

                # get test cases for validation
                if self.sql_assertion_only:
                    # only takes SQL-Assertion tests
                    df_test_cases = df_validation_cfg[df_validation_cfg["Test"] == 'SQL-Assertion']
                else:
                    # takes all available tests based on the df_test_data, but not SQL-Assertion tests
                    _included_columns = self.df_test_data.columns
                    df_test_cases = df_validation_cfg.loc[
                        df_validation_cfg['ColumnName'].isin(_included_columns) |
                        (
                            df_validation_cfg['ColumnName'].isin(Nulls) & (df_validation_cfg["Test"] != 'SQL-Assertion')
                        )
                    ]

                # start running validation
                for idx, testcase in df_test_cases.iterrows():
                    if testcase['Test'] in _test.keys():
                        logger.info("Validating on {}".format(str(testcase.tolist())))
                        _test[testcase['Test']](testcase)
                    else:
                        logger.error("Test '{}' is not supported".format(testcase['Test']))

            self.df_logs = pd.DataFrame(data=self.validation_logs, columns=self.log_header)
        except Exception:
            _error_msg = get_error_msg()
            logger.error(_error_msg)
            raise Exception(_error_msg)

        return self.df_logs

    @staticmethod
    def write_df_to_html(df, log_dir=None, _log_name=None):
        if log_dir is None:
            log_dir = get_config('Logs')['LogDir'] + '/' + date.today().strftime('%d-%m-%Y')
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
        if _log_name is None:
            _log_name = ''
        else:
            _log_name = _log_name + '-'

        log_html = log_dir + '/' + _log_name + datetime.now().strftime('%d-%m-%Y-%H-%M-%S') + '.html'
        with open(log_html, 'w') as f:
            df.to_html(f, index=False)

        return log_html

    def __check_freshness__(self, test_cfg):
        _value_cfg = json.loads(test_cfg['ValueConfig'])
        count = _value_cfg['count']
        period = _value_cfg['period']

        if not pd.core.dtypes.common.is_datetime64_ns_dtype(self.df_test_data[test_cfg['ColumnName']]):
            try:
                self.df_test_data[test_cfg['ColumnName']] = pd.to_datetime(self.df_test_data[test_cfg['ColumnName']], errors='raise')
            except:
                logger.error(f"Data type of {test_cfg['ColumnName']} is not a date/time style. Validation was ignored.")
                return

        if period in ('minute', 'hour', 'day', 'month', 'year'):
            timestamp_max = self.df_test_data[test_cfg['ColumnName']].max()
            # with self.db_engine.connect() as db_conn:
            #     timestamp_current = (pd.read_sql_query('select now() as current_datetime;', db_conn)).loc[0][0]
            timestamp_current = datetime.now()
            timedelta = timestamp_current - timestamp_max
            time_diff = 0.0

            if period == 'minute':
                time_diff = timedelta.total_seconds() / 60
            elif period == 'hour':
                time_diff = timedelta.total_seconds() / 3600
            elif period == 'day':
                # time_diff = timedelta / np.timedelta64(1, 'D')  # timedelta.days
                time_diff = pd.Timedelta(timedelta).to_timedelta64() / pd.Timedelta(np.timedelta64(1, 'D')).to_timedelta64()
            elif period == 'month':
                # time_diff = timedelta / np.timedelta64(1, 'M')
                time_diff = pd.Timedelta(timedelta).to_timedelta64() / pd.Timedelta(np.timedelta64(1, 'M')).to_timedelta64()
            elif period == 'year':
                # time_diff = timedelta / np.timedelta64(1, 'Y')
                time_diff = pd.Timedelta(timedelta).to_timedelta64() / pd.Timedelta(np.timedelta64(1, 'Y')).to_timedelta64()

            if time_diff >= count:
                self.validation_logs.append(
                    test_cfg.tolist() + [Enum.Validation.FAILED, json.dumps(
                        {"latest_timestamp": datetime.strftime(timestamp_max, '%Y-%m-%d %H:%M:%S'),
                         "current_datetime": datetime.strftime(timestamp_current, '%Y-%m-%d %H:%M:%S'),
                         "total_diff": time_diff})])
            else:
                self.validation_logs.append(test_cfg.tolist() + [Enum.Validation.PASSED, None])
        else:
            logger.error("Incorrect validation configuration. "
                         "Period must be one of these values: 'minute', 'hour', 'day', 'month' or 'year'.")

    def __check_unique__(self, test_cfg):
        _columns = []
        if test_cfg['ColumnName']:
            _columns = [test_cfg['ColumnName']]
        elif test_cfg['ValueConfig']:
            _columns = json.loads(test_cfg['ValueConfig'])['columns']

        # pd.Series(self.df_test_data[test_cfg['ColumnName']]).is_unique
        if len(_columns) > 0:
            df = self.df_test_data.groupby(_columns, as_index=False).size()
            df.rename(columns={"size": "found"}, inplace=True)
            df_findings = df.loc[df['found'] > 1]
            if len(df_findings) > 0:
                self.validation_logs.append(test_cfg.tolist() + [Enum.Validation.FAILED,
                                                                 df_findings.head(10).to_json(orient='split',
                                                                                              index=False)])
            else:
                self.validation_logs.append(test_cfg.tolist() + [Enum.Validation.PASSED, None])
        else:
            logger.error("Incorrect validation configuration. Unique check required column(s).")

    def __check_not_null__(self, test_cfg):
        _column = test_cfg['ColumnName']
        df_findings = self.df_test_data.loc[self.df_test_data[_column].isin(Nulls)]
        if len(df_findings) > 0:
            self.validation_logs.append(
                test_cfg.tolist() + [Enum.Validation.FAILED, df_findings.head(10).to_json(orient='split',
                                                                                          index=False)])
        else:
            self.validation_logs.append(test_cfg.tolist() + [Enum.Validation.PASSED, None])

    def __check_reference_values__(self, test_cfg):
        _value_cfg = json.loads(test_cfg['ValueConfig'])
        column, fk_table, fk_column, where_sql = test_cfg['ColumnName'], _value_cfg['table'], _value_cfg['column'], None

        _unique_values = self.df_test_data.loc[~self.df_test_data[column].isin(Nulls)][column].unique()
        where_in_values = tuple(_unique_values)
        if len(where_in_values) == 1:
            # add 1 more value to avoid ',' at the end
            where_in_values = where_in_values + where_in_values

        if 'where_sql' in _value_cfg.keys():
            where_sql = _value_cfg['where_sql']

        if where_sql in Nulls:
            where_sql = "where {} in {}".format(fk_column, where_in_values)
        else:
            where_sql = where_sql + " and {} in {}".format(fk_column, where_in_values)

        _query = "select {} as id from {} {};".format(fk_column, fk_table, where_sql)
        with self.db_engine.connect() as db_conn:
            df_reference_values = pd.read_sql_query(_query, db_conn)
            _unique_reference_values = df_reference_values['id'].unique()

        # get items in the test data but not in referenced fk values
        _findings = np.setdiff1d(_unique_values, _unique_reference_values)
        if len(_findings) > 0:
            self.validation_logs.append(test_cfg.tolist() + [Enum.Validation.FAILED, json.dumps(_findings.tolist())])
        else:
            self.validation_logs.append(test_cfg.tolist() + [Enum.Validation.PASSED, None])

    def __check_accepted_values__(self, test_cfg):
        _column = test_cfg['ColumnName']
        _accepted_values = json.loads(test_cfg['ValueConfig'])
        df_findings = self.df_test_data.loc[~self.df_test_data[_column].isin(_accepted_values)]
        if len(df_findings) > 0:
            self.validation_logs.append(
                test_cfg.tolist() + [Enum.Validation.FAILED, df_findings.head(10).to_json(orient='split', index=False)])
        else:
            self.validation_logs.append(test_cfg.tolist() + [Enum.Validation.PASSED, None])

    def __check_sql_assertion__(self, test_cfg):
        _query = test_cfg['ValueConfig']

        with self.db_engine.connect() as db_conn:
            df_findings = pd.read_sql_query(_query, db_conn)

        if len(df_findings) > 0:
            self.validation_logs.append(
                test_cfg.tolist() + [Enum.Validation.FAILED, df_findings.head(10).to_json(orient='split', index=False)])
        else:
            self.validation_logs.append(test_cfg.tolist() + [Enum.Validation.PASSED, None])


if __name__ == '__main__':
    pass
