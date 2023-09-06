import logging
import logging.config
from pathlib import Path
import time

config = {
    "log": {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"simple": {"format": "%(message)s"}, "datetime": {"format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"}},
        "handlers": {
            "console": {"class": "logging.StreamHandler", "level": "DEBUG", "formatter": "simple", "stream": "ext://sys.stdout"},
            "info_file_handler": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "INFO",
                "formatter": "datetime",
                "filename": "PJKILLER_{}.log".format(time.strftime("%Y%m%d%H%M%S", time.localtime())),
                "maxBytes": 10485760,
                "backupCount": 20,
                "encoding": "utf8",
            },
        },
        "root": {"level": "INFO", "handlers": ["console", "info_file_handler"]},
    }
}

def setup_logging(save_dir: Path, default_level=logging.INFO):
    """Setup logging configuration"""
    if "log" in config:
        for _, handler in config["log"]["handlers"].items():
            if "filename" in handler:
                handler["filename"] = str(save_dir / handler["filename"])
        logging.config.dictConfig(config["log"])
    else:
        print("Warning: logging configuration file is not found.")
        logging.basicConfig(level=default_level)


def get_logger(name, save_dir, verbosity=2):
    log_levels = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    assert verbosity in log_levels, "verbosity option {} is invalid. Valid options are {}.".format(verbosity, log_levels.keys())
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    setup_logging(Path(save_dir))
    logger = logging.getLogger(name)
    logger.setLevel(log_levels[verbosity])

    return logger
