"""Common Utilities."""

import logging
import multiprocessing
import os
import re
from datetime import datetime, timedelta, timezone
from random import choice
from string import ascii_letters, digits
from typing import Optional, Union, cast, get_args, get_origin

import requests


from .mail import Emailer, SendGridEmailer


logger = logging.getLogger(__name__)
# logger.addHandler(logging.StreamHandler(sys.stdout))

LOG_QUEUES = {}


def random_string(length: int) -> str:
    """Generate String with length."""
    return "".join(choice(ascii_letters + digits) for _ in range(length))


def utc_datetime(**addons: int) -> datetime:
    """Get current datetime with option to add additional timedelta."""
    return datetime.now(tz=timezone.utc) + timedelta(**addons)


def utc_timestamp(**addons: int) -> int:
    """Get current timestamp with option to add additional timedelta."""
    return int(utc_datetime(**addons).timestamp())


def make_optional(cls: type) -> type:
    """Check if the type hint is Optional."""
    # An Optional type will be represented as Union[type, NoneType]
    if get_origin(cls) is Union and type(None) in get_args(cls):
        return cls
    return cast(type, Optional[cls])


class ElasticConnector:
    """Elastic Connector."""

    @classmethod
    def configure(cls, url: str) -> None:
        """Configure Elastic Connector."""
        cls.url = url
        cls.headers = {
            "Content-Type": "application/json",
            "Authorization": f"ApiKey {os.getenv("ELASTIC_API_KEY")}"
        }

    @classmethod
    def post(cls, url_suffix: str, payload: dict = None) -> dict:
        """Post to Elastic."""
        return requests.post(f"{cls.url}/{url_suffix}", headers=cls.headers, json=payload)


def format_elastic_record(record: dict) -> dict:
    # Strip out color code from message before sending to elastic
    msg = record.getMessage()
    msg = re.sub(r"\033\[[0-9]*m", "", msg)
    ts = "%s.%03d" % (
        logging.Formatter().formatTime(record, "%Y-%m-%dT%H:%M:%S"),
        record.msecs,
    )

    elastic_record = {
        "@timestamp": ts,
        "message": msg,
        "level": record.levelname,
    }
    extra_fields = record.__dict__.get("extra_fields", [])
    elastic_record.update({k: record.__dict__[k] for k in extra_fields})
    return elastic_record


def add_elastic_log_handler(logger_instance: logging.Logger, index: str, under_test: bool = False) -> None:
    has_queue_handler = any(
        isinstance(h, logging.handlers.QueueHandler) for h in logger_instance.handlers
    )
    if not has_queue_handler:
        log_queue = multiprocessing.Queue()
        queue_handler = logging.handlers.QueueHandler(log_queue)
        logger_instance.addHandler(queue_handler)

        def elastic_log_worker(elastic_index: str) -> None:
            # Set up logging in the child process
            logging.basicConfig(level=logging.INFO)
            child_logger = logging.getLogger("elastic_handler_logger")

            # Use logging instead of print for better control
            child_logger.info("Starting elastic logger handler")
            while True:
                try:
                    record = log_queue.get()
                    if record is None:
                        # This is temporary
                        # for debugging purposes
                        from datetime import datetime

                        ElasticConnector.post(
                            f"/{elastic_index}/_doc/",
                            payload={
                                "@timestamp": datetime.now().strftime(
                                    "%Y-%m-%dT%H:%M:%S"
                                ),
                                "message": f"Stopping process for {elastic_index}",
                                "level": "SYSTEM",
                            }
                        )
                        # end of temporary code
                        break
                    elastic_record = format_elastic_record(record)
                    ElasticConnector.post(f"/{elastic_index}/_doc/", payload=elastic_record)
                except Exception as e:
                    child_logger.error("Error in elastic log worker", e)
                    pass

        # if under test, don't spawn the log worker process. Tests will validate two things:
        # 1. logs are added to the log queue
        # 2. format_elastic_record process the log properly and create the record for elastic
        if not under_test:
            worker_proc = multiprocessing.Process(
                target=elastic_log_worker, args=(index,)
            )
            worker_proc.start()

        return log_queue


def start_elastic_logger(index: str) -> None:
    """Start Elastic Logger."""
    if (
        index not in LOG_QUEUES
        and multiprocessing.current_process().name == "MainProcess"
    ):
        LOG_QUEUES[index] = add_elastic_log_handler(
            logger, index, False
        )


__all__ = [
    "Emailer",
    "SendGridEmailer",
    "random_string",
    "utc_datetime",
    "utc_timestamp",
    "logger",
    "ElasticConnector",
    "start_elastic_logger",
]
