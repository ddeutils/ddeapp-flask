import functools


def ignore_unhash(func):
    uncached = func.__wrapped__
    attributes = functools.WRAPPER_ASSIGNMENTS + ("cache_info", "cache_clear")

    @functools.wraps(func, assigned=attributes)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except TypeError as error:
            if "unhashable type" in str(error):
                return uncached(*args, **kwargs)
            raise

    wrapper.__uncached__ = uncached
    return wrapper
