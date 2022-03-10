import logging
import os


class Logger:
    def __init__(self, type):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.formatter = logging.Formatter('%(asctime)s : %(filename)s : %(funcName)s : %(levelname)s : %(message)s')
        self.basepath = os.path.dirname(__file__) + '/..'
        if type == 'Proxies':
            self.file_handler = logging.FileHandler(os.path.join(self.basepath, 'log_data/proxies.log'))
        elif type == 'Scraping':
            self.file_handler = logging.FileHandler(os.path.join(self.basepath, 'log_data/scraping.log'))
        self.file_handler.setLevel(logging.DEBUG)
        self.file_handler.setFormatter(self.formatter)
        self.logger.addHandler(self.file_handler)