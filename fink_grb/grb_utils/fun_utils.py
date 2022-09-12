    
    
def return_verbose_level(config, logger):
    """
    Get the verbose level from the config file and return it.
    """
    try:
        logs = config["ADMIN"]["verbose"] == 'True'
    except Exception as e:
        logger.error("Config entry not found \n\t {}\n\tsetting verbose to True by default".format(e))
        logs = True

    return logs