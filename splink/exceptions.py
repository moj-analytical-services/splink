import warnings


# base class for any type of custom exception
class SplinkException(Exception):
    pass


class EMTrainingException(SplinkException):
    pass


class ComparisonSettingsException(SplinkException):
    def __init__(self, message=""):
        # Add the default message to the beginning of the provided message
        full_message = (
            "The following errors were identified within your "
            "settings object's comparisons"
        )
        if message:
            full_message += ":\n" + message
        super().__init__(full_message)


class SplinkDeprecated(DeprecationWarning):
    pass


class ErrorLogger:
    """A basic error loggger. This function allows you to collate
    errors into a single list and then log them.

    The logged errors default to displaying a SplinkException, though
    this can be changed.
    """

    # In py3.11, this functionality is now covered by
    # https://docs.python.org/3/library/exceptions.html#ExceptionGroup

    def __init__(self):
        self.Q = []
        self.e = []

    @property
    def errors(self):
        return "\n".join(self.Q)

    def append(self, error):
        self.e.append(error)

        if isinstance(error, str):
            self.Q.append(error)
        else:
            # Potentially needs a validation check to
            # ensure an error is passed.
            error_name = error.__class__.__name__

            logged_exception = f"{error_name}: {str(error)}"
            self.Q.append(logged_exception)

    def raise_and_log_all_errors(self, exception=SplinkException, additional_txt=""):
        if self.Q:
            raise exception(f"\n{self.errors}{additional_txt}")


warnings.simplefilter("always", SplinkDeprecated)
