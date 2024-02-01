import warnings
from typing import List, Union


# base class for any type of custom exception
class SplinkException(Exception):
    pass


class InvalidAWSBucketOrDatabase(Exception):
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


class InvalidDialect(SplinkException):
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
        self.error_queue: List[str] = []  # error queue for formatted errors
        # Raw input errors. Used for debugging.
        self.raw_errors: List[Union[str, Exception]] = []

    @property
    def errors(self) -> str:
        """Return concatenated error messages."""
        return "\n".join(self.error_queue)

    def append(self, error: Union[str, Exception]) -> None:
        """Append an error to the error list.

        Args:
            error: An error message string or an Exception instance.
        """
        # Ignore if None
        if error is None:
            return

        self.raw_errors.append(error)

        if isinstance(error, str):
            self.error_queue.append(error)
        elif isinstance(error, Exception):
            error_name = error.__class__.__name__
            logged_exception = f"{error_name}: {str(error)}"
            self.error_queue.append(logged_exception)
        else:
            raise ValueError(
                "The 'error' argument must be a string or an Exception instance."
            )

    def raise_and_log_all_errors(self, exception=SplinkException, additional_txt=""):
        """Raise a custom exception with all logged errors.

        Args:
            exception: The type of exception to raise (default is SplinkException).
            additional_txt: Additional text to append to the error message.
        """
        if self.error_queue:
            raise exception(f"\n{self.errors}{additional_txt}")


warnings.simplefilter("always", SplinkDeprecated)
