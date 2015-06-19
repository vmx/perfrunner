import hashlib
import json
import time
from uuid import uuid4

from logger import logger
import requests


def uhex():
    return uuid4().hex


def pretty_dict(d):
    return json.dumps(d, indent=4, sort_keys=True,
                      default=lambda o: o.__dict__)


def log_phase(phase, settings):
    logger.info('Running {}: {}'.format(phase, pretty_dict(settings)))


def target_hash(*args):
    int_hash = hash(args)
    str_hash = hashlib.md5(hex(int_hash)).hexdigest()
    return str_hash[:6]


def server_group(servers, group_number, i):
    group_id = 1 + i / ((len(servers) + 1) / group_number)
    return 'Group {}'.format(group_id)


def retry(catch=(), iterations=5, wait=10):
    """
    This is a general purpose decorator for retrying a function while
    discarding a tuple of exceptions that the function might throw.

    'catch' is a tuple of exceptions. Passing in a list is also fine.

    'iterations' means number of total attempted calls. 'iterations' is only
    meaningful when >= 2.

    'wait' is wait time between calls.

    Usage:

    import perfrunner.helpers.misc

    @perfrunner.helpers.misc.retry(catch=[RuntimeError, KeyError])
    def hi():
        raise KeyError("Key Errrrr from Hi")

    # or if you want to tune your own iterations and wait

    @perfrunner.helpers.misc.retry(
        catch=[KeyError, TypeError],
        iterations=3, wait=1)
    def hi(who):
        print "hi called"
        return "hi " +  who

    print hi("john")
    # this throws TypeError when 'str' and 'None are concatenated
    print hi(None)
    """
    # in case the user specifies a list of Exceptions instead of a tuple
    catch = tuple(catch)

    def retry_decorator(func):
        def retry_wrapper(*arg, **kwargs):
            for i in xrange(iterations):
                try:
                    result = func(*arg, **kwargs)
                except catch:
                    if i == (iterations - 1):
                        raise
                    else:
                        pass
                else:
                    return result
                time.sleep(wait)
        return retry_wrapper
    return retry_decorator


def download_file_verify(url, filename, sha1):
    """Returns true if the downloaded file matches the given SHA1."""
    _download_file(url, filename)
    return _verify_sha1(filename, sha1)


# From http://stackoverflow.com/a/16696317/935109 (2015-06-17)
def _download_file(url, filename):
    logger.info("Downloading {} as {}".format(url, filename))
    r = requests.get(url, stream=True)
    with open(filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()
    return filename


def _verify_sha1(path, expected_sha1):
    with open(path, 'rb') as f:
        sha1 = hashlib.sha1(f.read()).hexdigest()
    return sha1 == expected_sha1
