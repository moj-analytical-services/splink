from typing import List, Union


def format_text_as_red(text):
    return f"\033[91m{text}\033[0m"


# base class for any type of custom exception
class SplinkException(Exception):
    pass


class InvalidAWSBucketOrDatabase(Exception):
    pass


class EMTrainingException(SplinkException):
    pass


class MissingDependencyException(Exception):
    pass


class ComparisonSettingsException(SplinkException):
    def __init__(self, message=""):
        # Add the default message to the beginning of the provided message
        full_message = "Errors were detected in your settings object's comparisons"

        if message:
            full_message += "\n" + message
        super().__init__(full_message)


class SplinkDeprecated(DeprecationWarning):
    pass


class InvalidSplinkInput(SplinkException):
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

        return "\n\n".join([f"{e}" for e in self.error_queue])

    def log_error(self, error: Union[list, str, Exception]) -> None:
        """
        Log an error or a list of errors.

        Args:
            error: An error message, an Exception instance, or a list
                containing strings or exceptions.
        """
        if isinstance(error, (list, tuple)):
            for e in error:
                self._log_single_error(e)
        else:
            self._log_single_error(error)

    def _log_single_error(self, error: Union[str, Exception]) -> None:
        """Log a single error message or Exception."""
        if error is None:
            return

        self.raw_errors.append(error)
        error_str = self._format_error(error)
        self.error_queue.append(error_str)

    def _format_error(self, error: Union[str, Exception]) -> str:
        """Format an error message or Exception for logging."""
        if isinstance(error, Exception):
            return f"{format_text_as_red(error.__class__.__name__)}: {error}"
        elif isinstance(error, str):
            return f"{error}"
        else:
            raise ValueError("Error must be a string or an Exception instance.")

    def raise_and_log_all_errors(self, exception=SplinkException, additional_txt=""):
        """Raise a custom exception with all logged errors.

        Args:
            exception: The type of exception to raise (default is SplinkException).
            additional_txt: Additional text to append to the error message.
        """
        if self.error_queue:
            error_message = f"\n{self.errors}\n{additional_txt}"
            raise exception(error_message)
