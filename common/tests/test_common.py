import logging
from google.cloud import bigquery
from common.retry_policies import *
from common.config import *

def test_bq_query_retry_logic(caplog):
    caplog.set_level(logging.INFO)
    client = bigquery.Client()

    try:
        client.query(
            query=f"SELECT CAST('string' as FLOAT64)", job_retry=BIGQUERY_RETRY_POLICY
        ).result()
    except Exception as e:
        pass
    
    assert caplog.text.count("Checking for retriability.") == 6
    

def test_helpers():
    assert val_starts_with_g("G-123456789") 
    assert not val_starts_with_g("g-123456789") 
    assert not val_starts_with_g("Hello, world!")

    assert val_greater_or_equal_to_zero(1) 
    assert val_greater_or_equal_to_zero(0) 
    assert not val_greater_or_equal_to_zero(-1)

    assert val_greater_or_equal_to_one(2) 
    assert val_greater_or_equal_to_one(1) 
    assert not val_greater_or_equal_to_one(0)

    assert between_5_and_100(5) 
    assert between_5_and_100(100) 
    assert between_5_and_100(42) 
    assert not between_5_and_100(4) 
    assert not between_5_and_100(101) 

    assert length_less_than_64("Hello, world!") 
    assert not length_less_than_64("Tb0snRBOjmit0MrxosQkmXJ19oJTGR6pEQJzS7Oy0mrrfl79uRw392UhVKvVgzRr") 
    assert not length_less_than_64("H7A6cygR2wG7keEjQzXwarhYKtWtVZZnZURQM3GLCvZlTvegy6xsjgHHd6Vw72UvO")