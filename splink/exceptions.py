import warnings


# base class for any type of custom exception
class SplinkException(Exception):
    pass


class EMTrainingException(SplinkException):
    pass


class SplinkDeprecated(DeprecationWarning):
    pass


warnings.simplefilter("always", SplinkDeprecated)
