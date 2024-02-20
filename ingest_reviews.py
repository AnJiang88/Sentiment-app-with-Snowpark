import string
import json
from xml.dom.minicompat import StringTypes
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import udf, call_udf
from snowflake.snowpark.functions import lit
from datetime import date

# Create session object
def create_session_object():
    with open('connection.json') as f:
        connection_parameters = json.load(f)
        session = Session.builder.configs(connection_parameters).create()
        print(session.sql('select current_warehouse(), current_database(), current_schema()').collect())
        return session

session = create_session_object()

# Create a UDTF to read unstructured files and extract reviews
@udtf(name = "read_unstructured_reviews", is_permanent = True, stage_location="python_scripts", replace=True, output_schema=StructType([StrctField("pr")]))
class read_reviews_class:
    def process(self, stagefile):
        with _snowflake.open(stagefile) as f:
            data = f.readall().decode('utf-8')
            lines = data.split('\n')
            for line in lines:
                lineStr = line.strip()
                d = lineStr.split("|")
                try:
                    # Read the product_id, the product review and the review date.
                    review_date = date.fromisoformat(d[1])
                    product_id = d[0]
                    product_review = d[2]
                    yield (product_id, product_review, review_date, )
                except:
                    pass