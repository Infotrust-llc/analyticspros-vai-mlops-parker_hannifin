from google.api_core import exceptions
from google.api_core.retry import Retry
import logging

BQ_RETRYABLE_TYPES = (
    exceptions.BadRequest,  # 400
    exceptions.TooManyRequests,  # 429
    exceptions.InternalServerError,  # 500
    exceptions.BadGateway,  # 502
    exceptions.ServiceUnavailable,  # 503
)


def is_retryable(exc):
    logging.info("Checking for retryability.")
    return isinstance(exc, BQ_RETRYABLE_TYPES)


BIGQUERY_RETRY_POLICY = Retry(
    predicate=is_retryable, initial=5, maximum=120, multiplier=2
)
