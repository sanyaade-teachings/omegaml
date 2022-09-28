import logging

logger = logging.getLogger(__name__)


def hello(**kwargs):
    logger.info('hellworld log output')
    return "hello from helloworld", kwargs


def run(om, *args, **kwargs):
    """
    the script API execution entry point
    :return: result
    """
    import pandas as pd
    df = pd.DataFrame({
        'a': list(range(0, int(1e3 + 1))),
        'b': list(range(0, int(1e3 + 1)))
    })
    store = om.datasets
    store.put(df, 'mydata-xlarge', append=False, chunksize=100)
    result = hello(**kwargs)
    return result
