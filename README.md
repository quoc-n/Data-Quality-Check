Welcome to Data Quality Checks project!

This Data Quality Check to run on every data pipeline for data accuracy checking based on model configurations. The module can be packed and distributed as a package for different purposes of use.

## Resources: DataValidation module
The module exposes functions to execute data quality checks based on the configured test cases, and able to choose writing the validation
logs to a file. To use this data validation, test cases are needed to configure properly in the table _DataValidationCfg

```
CREATE TABLE [dbo].[_DataValidationCfg](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[TargetTable] [varchar](50) NULL,
	[ColumnName] [varchar](50) NULL,
	[Test] [varchar](50) NULL,
	[ValueConfig] [varchar](512) NULL,
	[Severity] [varchar](10) NULL,
	[Status] [varchar](10) NULL,
	[Desc] [varchar](256) NULL
) ON [PRIMARY]
```

### Sample configurations
![image](https://github.com/user-attachments/assets/e8565964-508f-45f4-9207-63175111985f)


### The following are exposed functions:
- validate: executing all test cases and return a validation log in dataframe
- write_df_to_html: writing the validation logs (dataframe) to an html file

### The following are supported tests in validation process:
```
_test = {
            'Freshness': self.__check_freshness__,
            'Unique': self.__check_unique__,
            'Not Null': self.__check_not_null__,
            'Reference Values': self.__check_reference_values__,
            'Accepted Values': self.__check_accepted_values__,
            'SQL-Assertion': self.__check_sql_assertion__
        }
```
1. Freshness: a test to check the fresshness of a dataset based on a timestamp column (by comparing the most recent record with current timestamp to determine how fresh the dataset is). In other words, freshness is used to define the acceptable amount of time between the most recent record, and now

Example ValueConfig: {"count": 3, "period": "day"}
- count: an integer number
- period: minute, hour, day, month, year

2. Unique: a test to verify that every value in a column contains unique values

Example ValueConfig: null if 1 column check; or multiple columns check: {“columns”: [“tsac_no“, “type”]}

3. Not Null: a test to check that the values for a given column are always present

4. Reference Values: a test to check referential integrity

Example ValueConfig: {"table": "st_project", "column": "project_name",  "where_sql": "where country='Singapore'" }
- table: a table name
- column: a column name on the table to refer
- where_sql: a valid sql where clause

5. Accepted Values: a test to validate whether a set of values within a column is present

Example ValueConfig: ["SALE","RENTAL","ASSIGNMENT","NEWHOME"]

6. SQL-Assertion: a test to execute a sql statement to assert if the validation is correct

Example ValueConfig: select count(*) as cnt from tx_era_stage where tsac_price < 0

For above sql check, we don’t expect to get any transactions which have price < 0, hence if the execution returned records, it will assert as failed


## Packing and using the distributed package 
### distributing package as ep_services
build:
python -m pip install --upgrade build \
python -m build --outdir ./ep_services/package-dist

### installing ep_services
python -m pip install --no-index --find-links="/home/edge/python-crawler/service-packages/" ep_services \
python3 -m pip install --no-index --find-links="D:/EdgeProp/repos/edgeprop-analytics/python-crawler/service-packages/" ep_services

## using ep_services
```
from ep_services import DataValidation

# df_source_data: is a pandas dataframe contains the data that is needed to verify through all configured test cases in the table _DataValidationCfg

df_validation_logs = DataValidation('st_project', _df_test_data=df_source_data).validate()
df_failure_logs = df_validation_logs[df_validation_logs['Validated'] == Enum.Validation.FAILED]

if len(df_failure_logs) > 0:
    df_failure_error_logs = df_failure_logs[df_failure_logs['Severity'] == 'Error']
    if len(df_failure_error_logs) > 0:
        logger.error("Input dataset contains data quality issues which are configured as Error on Severity.")

        # publishing a notification: sending failure notification for the pipeline, and can also handle to not execute the next steps
        pass
    else:
        logger.info("Input dataset contains data quality issues which are configured as Warn on Severity.")
```
