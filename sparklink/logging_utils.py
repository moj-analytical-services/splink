import logging

sqlparse_exists = True
try:
    import sqlparse
except ImportError:
    sqlparse_exists = False
from textwrap import dedent

def format_sql(sql):
    if sqlparse_exists:
        return sqlparse.format(sql, reindent=True, keyword_case='upper')
    else:
        return dedent(sql)


def log_sql(sql, logger, level='INFO'):

    level = level.upper()
    sql = format_sql(sql)

    # Is the logger a Spark logger or a Python logger?
    if type(logger).__name__ == 'JavaObject':
        if level == 'INFO':
            logger.info(sql)
        elif level == 'DEBUG':
            logger.debug(sql)
        elif level == 'WARNING':
            logger.warning(sql)
        elif level == 'ERROR':
            logger.error(sql)

    if type(logger).__name__ == 'Logger':
        logger.log(getattr(logging, level),  sql)