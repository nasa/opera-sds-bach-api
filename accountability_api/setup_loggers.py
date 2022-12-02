import logging
import os
from logging.handlers import TimedRotatingFileHandler

from accountability_api.singleton_base import Singleton
from accountability_api.configuration_obj import ConfigurationObj
from gunicorn.glogging import Logger as GloggingLogger


class GunicornLogger(GloggingLogger):
    """
    Ref: https://stackoverflow.com/a/49122240
    """

    def __init__(self, cfg):
        self._logger_config = GetLoggerDetails()
        super().__init__(cfg)
        pass

    def __generate_handler(self):
        return TimedRotatingFileHandler(
            self._logger_config.log_file_path,
            when="h",
            interval=self._logger_config.log_interval_hr,
            backupCount=self._logger_config.log_backup_count,
            encoding=None,
            delay=False,
            utc=False,
            atTime=None,
        )

    def setup(self, cfg):
        super().setup(cfg)
        fh = self.__generate_handler()
        fh.setLevel(self.access_log.level)
        fh.setFormatter(self._logger_config.log_format)
        self.access_log.addHandler(fh)

        # enables writing to the log file specified in class GetLoggerDetails
        fh2 = self.__generate_handler()
        fh2.setLevel(self.error_log.level)
        fh2.setFormatter(self._logger_config.log_format)
        self.error_log.addHandler(fh2)
        return


class LocalLog:
    def setup_log(self):
        logger_config = GetLoggerDetails()
        logging.captureWarnings(True)
        logging.basicConfig(level=logger_config.log_level)

        fh = TimedRotatingFileHandler(
            logger_config.log_file_path,
            when="h",
            interval=logger_config.log_interval_hr,
            backupCount=logger_config.log_backup_count,
            encoding=None,
            delay=False,
            utc=False,
            atTime=None,
        )
        fh.setLevel(logger_config.log_level)
        fh.setFormatter(logger_config.log_format)

        logging.getLogger().setLevel(logger_config.log_level)
        logging.getLogger().addHandler(fh)

        logging.getLogger("werkzeug").setLevel(logger_config.log_level)
        logging.getLogger("werkzeug").addHandler(fh)

        logging.getLogger("py.warnings").addHandler(fh)
        return

    pass


class GetLoggerDetails(metaclass=Singleton):
    def __init__(self):
        config = ConfigurationObj()
        self.log_level = logging.getLevelName(
            config.get_item("LOG_LEVEL", profile="LOGGING", default="INFO")
        )
        self.log_interval_hr = logging.getLevelName(
            config.get_item("LOG_INTERVAL_HOUR", profile="LOGGING", default="12")
        )
        self.log_backup_count = logging.getLevelName(
            config.get_item("LOG_BACKUP_COUNT", profile="LOGGING", default="30")
        )
        self.log_file_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "..",  # ~/sciflo/ops/bach-api (project root)
            "..",  # ~/sciflo/ops/
            "..",  # ~/sciflo/
            "log",
            "bach-api.log",
        )
        try:
            self.log_interval_hr = int(self.log_interval_hr)
        except:
            self.log_interval_hr = 12
        try:
            self.log_backup_count = int(self.log_backup_count)
        except:
            self.log_backup_count = 30
        self.log_format = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
        pass

    pass
