## Understanding and debugging Splink's computations

Splink contains tooling to help developers understand the underlying computations, how caching and pipelining is working, and debug problems.

There are two main mechanisms: `_debug_mode`, and setting different logging levels

## Debug mode

You can turn on debug mode by setting `linker._debug_mode = True`.

This has the following effects:

- Each step of Splink's calculations are executed in turn. That is, [pipelining](https://moj-analytical-services.github.io/splink/dev_guides/caching.html) is switched off.
- The SQL statements being executed by Splink are displayed
- The results of the SQL statements are displayed in tabular format

This is probably the best way to understand each step of the calculations being performed by Splink - because a lot of the implementation gets 'hidden' within pipelines for performance reasons.

Note that enabling debug mode will dramatically reduce Splink's performance!

## Logging

Splink has a range of logging modes that output information about what Splink is doing at different levels of verbosity.

Unlike debug mode, logging doesn't affect the performance of Splink.

### Logging levels

You can set the logging level with code like `logging.getLogger("splink").setLevel(desired_level)` although **see notes below about gotchas**.

The logging levels in Splink are:

- `logging.INFO` (`20`): This outputs user facing messages about the training status of Splink models
- `15`: Outputs additional information about time taken and parameter estimation
- `logging.DEBUG` (`10`): Outputs information about the names of the SQL statements executed
- `logging.DEBUG` (`7`): Outputs information about the names of the components of the SQL pipelines
- `logging.DEBUG` (`5`): Outputs the SQL statements themselves

### How to control logging

Note that by default Splink sets the [logging level to `INFO` on initialisation](https://github.com/moj-analytical-services/splink/blob/44304126acf3a3292810f1bc209f644e3691ee3a/splink/linker.py#L135)

#### With basic logging

```python
import logging
linker = Linker(df, settings, db_api, set_up_basic_logging=False)

# This must come AFTER the linker is intialised, because the logging level
# will be set to INFO
logging.getLogger("splink").setLevel(logging.DEBUG)
```

#### Without basic logging

```python

# This code can be anywhere since set_up_basic_logging is False
import logging
logging.basicConfig(format="%(message)s")
splink_logger = logging.getLogger("splink")
splink_logger.setLevel(logging.INFO)

linker = Linker(df, settings, db_api, set_up_basic_logging=False)
```
