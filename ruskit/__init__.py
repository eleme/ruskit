from redis import connection
from redis.exceptions import (ResponseError, ConnectionError)
from .exceptions import (AskError, MovedError, ClusterDownError,
                         UnknownNodeError)

import logging

logging.basicConfig()

REDIS_CUSTOM_EXCEPTION_CLASSES = {
    'ASK': AskError,
    'MOVED': MovedError,
    'CLUSTERDOWN': ClusterDownError,
    # we should add exists error from BaseParser which is ConnectionError
    'ERR': {'max number of clients reached': ConnectionError,
            'Unknown node': UnknownNodeError},
}


def redis_custom_parse_error(self, response):
    "Parse an error response"
    error_code = response.split(' ')[0]
    if error_code in self.EXCEPTION_CLASSES:
        response = response[len(error_code) + 1:]
        exception_class = self.EXCEPTION_CLASSES[error_code]
        if isinstance(exception_class, dict):
            for reason, inner_exception_class in exception_class.items():
                if reason in response:
                    return inner_exception_class(response)
            return ResponseError(response)
        return exception_class(response)
    return ResponseError(response)


connection.BaseParser.EXCEPTION_CLASSES.update(REDIS_CUSTOM_EXCEPTION_CLASSES)
connection.BaseParser.parse_error = redis_custom_parse_error
