"""
Shared DLT expectation definitions.

Import these in your pipeline files to apply consistent data quality rules.
Usage:
    from pipeline.expectations.common_expectations import VALID_ID
    @dlt.expect(*VALID_ID)
    def my_table(): ...
"""

# Tuples of (constraint_name, sql_expression) for use with @dlt.expect decorators
VALID_ID = ("valid_id", "id IS NOT NULL AND id > 0")
VALID_TIMESTAMP = ("valid_timestamp", "created_at IS NOT NULL")
NO_FUTURE_DATES = ("no_future_dates", "created_at <= current_timestamp()")
VALID_STRING_NAME = ("valid_name", "name IS NOT NULL AND length(trim(name)) > 0")
