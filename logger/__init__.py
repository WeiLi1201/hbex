import os
import logging
from logging import config as log_conf


__all__ = [
    'hbtrade_logger', 'tradesignal_logger', 'cmpfeature_logger', 'tradedeal_logger'
]

log_dir = os.path.dirname(os.path.dirname(__file__))+'/logs'
if not os.path.exists(log_dir):
    os.mkdir(log_dir)

log_path = os.path.join(log_dir, 'hbex.log')
hbtrade_path = os.path.join(log_dir, 'hbtrade.log')
tradesignal_path = os.path.join(log_dir, 'tradesignal.log')
cmpfeature_path = os.path.join(log_dir, 'cmpfeature.log')
tradedeal_path = os.path.join(log_dir, 'tradedeal.log')

log_config = {
    'version': 1.0,
    'formatters': {
        'detail': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'datefmt': "%Y-%m-%d %H:%M:%S"
        },
        'simple': {
            'format': '%(name)s - %(levelname)s - %(message)s',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'detail'
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'maxBytes': 1024 * 1024 * 5,
            'backupCount': 10,
            'filename': log_path,
            'level': 'INFO',
            'formatter': 'detail',
            'encoding': 'utf-8',
        },
        'hbtrade_logger': {
            'class': 'logging.handlers.RotatingFileHandler',
            'maxBytes': 1024 * 1024 * 5,
            'backupCount': 10,
            'filename': hbtrade_path,
            'level': 'INFO',
            'formatter': 'detail',
            'encoding': 'utf-8',
        },
        'tradesignal_logger': {
            'class': 'logging.handlers.RotatingFileHandler',
            'maxBytes': 1024 * 1024 * 5,
            'backupCount': 10,
            'filename': tradesignal_path,
            'level': 'INFO',
            'formatter': 'detail',
            'encoding': 'utf-8',
        },
        'cmpfeature_logger': {
            'class': 'logging.handlers.RotatingFileHandler',
            'maxBytes': 1024 * 1024 * 5,
            'backupCount': 10,
            'filename': cmpfeature_path,
            'level': 'INFO',
            'formatter': 'detail',
            'encoding': 'utf-8',
        },
        'tradedeal_logger': {
            'class': 'logging.handlers.RotatingFileHandler',
            'maxBytes': 1024 * 1024 * 5,
            'backupCount': 10,
            'filename': tradedeal_path,
            'level': 'INFO',
            'formatter': 'detail',
            'encoding': 'utf-8',

        }

    },
    'loggers': {
        'hbtrade_logger': {
            'handlers': ['hbtrade_logger', 'console'],
            'level': 'DEBUG',
        },
        'tradesignal_logger': {
            'handlers': ['tradesignal_logger', 'console'],
            'level': 'DEBUG',
        },
        'cmpfeature_logger': {
            'handlers': ['cmpfeature_logger', 'console'],
            'level': 'DEBUG',
        },
        'tradedeal_logger': {
            'handlers': ['tradedeal_logger', 'console'],
            'level': 'DEBUG',
        }
    }
}

log_conf.dictConfig(log_config)

hbtrade_logger = logging.getLogger('hbtrade_logger')
tradesignal_logger = logging.getLogger('tradesignal_logger')
cmpfeature_logger = logging.getLogger('cmpfeature_logger')
tradedeal_logger = logging.getLogger('tradedeal_logger')

