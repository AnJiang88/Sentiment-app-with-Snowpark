import string
import json
from xml.dom.minicompat import StringTypes
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import udf, call_udf
from snowflake.snowpark.functions import lit
from datetime import date

# Create Session object
def create_session_object():
    with open('connetion.json') as f:
        connection_parameters = json.load(f)
        session = Session.builder.configs(connection_parameters).create()
        print(session.sql('select current_warehouse(), current_database(), current_schema()').collect())
        return session

session = create_session_object()

# Call the read_unstructured_reviews table function into a dataframe
df = session.table_function("READ_UNSTRUCTURED_REVIEWS", lit("https://gka32533.snowflakecomputing.com/api/files/SUMMIT_DEMO/RAW/RAW_FILES/unstructured%2freviews__0_0_0%2edat"))

# Save the data frame as a table
df.write.mode("overwrite").save_as_table("raw_reviews_sample")