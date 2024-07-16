from ep_services import DataValidation

import pandas as pd
from utils import (init_logging, get_db_engine)


if __name__ == '__main__':
    # prepare logging configuration
    init_logging()

    # get data set for validation
    query = """
        SELECT TOP (1000) [Player_Id]
          ,[Player_External_Id]
          ,[Player_Login]
          ,[Player_Password]
          ,[Player_Nick_Name]
          ,[Player_Language]
          ,[Player_Currency]
          ,[Player_Affiliate]
          ,[Player_Verified_Date]
          ,[Player_Register_Date]
          ,[Player_Login_Date]
          ,[Player_First_Name]
          ,[Player_Middle_Name]
          ,[Player_Last_Name]
      FROM [Dim_Players]
    """

    df_test_data = pd.read_sql_query(query, get_db_engine())

    # call data services to test the test data which is targeted to st_project table
    df_validation_logs = DataValidation('Dim_Players', _df_test_data=df_test_data).validate()

    # process the validation output
    df_failure_logs = df_validation_logs[df_validation_logs['Validated'] == 'Failed']

    # send email notification
    if len(df_failure_logs) > 0:
        # write logs as html file
        _log_html = DataValidation.write_df_to_html(df_failure_logs)
        '''
        # publish notification
        from sns import SNS
        msg_id = SNS('Data-Pipeline-Execution').publish_email_message(
            email_subject="[Test Email] Data Validation Failure",
            email_message="The dataset to load into table '{}' contains some data quality issues as per Data Validation"
                          " configurations for '{}' from _DataValidationCfg. Please check the data validation logs for"
                          " further details at {}".format('st_project', 'st_project', _log_html)
        )
        '''
    print('Data validation run successfully')
