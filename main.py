import logging
from source.core.application import Application

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s][%(levelname)s][%(name)s]:\n  %(message)s'
)

if __name__ == '__main__':
    try:
        app = Application("configs/config.yaml")
        app.run()
    except Exception as error:
        logging.exception(error)
