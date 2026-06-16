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

You can set the logging level when creating a linker, or by calling
`splink.logging.enable(desired_level)`.

The logging levels in Splink are:

- `logging.INFO` (`20`): This outputs user facing messages about the training status of Splink models
- `15`: Outputs additional information about time taken and parameter estimation
- `logging.DEBUG` (`10`): Outputs information about the names of the SQL statements executed
- `7`: Outputs information about the names of the components of the SQL pipelines
- `5`: Outputs the SQL statements themselves

### How to control logging

By default Splink configures its own `splink` logger at `logging.INFO`, without
calling `logging.basicConfig()` or changing the root logger for the Python process.
This means normal users see useful progress messages, while applications can still
control their own logging setup.

#### Configure when creating a linker

```python
import logging

linker = Linker(df, settings, log_level=logging.DEBUG)
```

Pass `log_level=None` if you do not want Splink to configure logging:

```python
linker = Linker(df, settings, log_level=None)
```

#### Configure outside linker construction

```python
import logging
import splink.logging

splink.logging.enable(logging.INFO)

linker = Linker(df, settings)
```

#### Use application logging

If your application has already configured logging, Splink will use that existing
configuration instead of adding its own handler:

```python
import logging

logging.basicConfig(format="%(message)s")
logging.getLogger("splink").setLevel(logging.INFO)

linker = Linker(df, settings)
```
