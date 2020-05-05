import logging
from importlib import reload # https://stackoverflow.com/a/53553516



def logger(logging):
    """
    Used for logging in cmd line.
    Args:
        my_name (str): person's name
    Returns:
        None
    """
    reload(logging) # https://stackoverflow.com/a/53553516
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s: %(asctime)s %(process)d %(message)s',
                        #datefmt='%d/%m/%Y %I:%M:%S %p',
                        # filename=constants.LOG_FILE_LOCATION, filemode='a')
                        )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter('%(levelname)s: %(asctime)s %(process)d %(message)s'))
    logging.getLogger().addHandler(console)