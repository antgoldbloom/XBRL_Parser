
def print_and_log(logging,message,warning=False):
    print(message)
    if warning:
        logging.warning(message)
    else:
        logging.info(message)
