import os
import json
import logging
import traceback
from datetime import date, datetime

from cryptography.fernet import Fernet
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine

import pandas as pd
import numpy as np
Nulls = [None, np.NaN, np.nan, pd.NaT, '', 'None', 'NaN', 'nan', 'NaT']


def get_config(tag=None):
    with open('config.json') as f:
        data = json.load(f)
    if tag:
        return data[tag]

    return data


def init_logging():
    log_dir = get_config('Logs')['LogDir'] + '/' + date.today().strftime('%d-%m-%Y')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    _log_file = log_dir + '/' + datetime.now().strftime('%d-%m-%Y-%H-%M-%S') + '.log'
    logging.basicConfig(filename=_log_file, level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    return _log_file


def get_error_msg():
    # traceback.print_exc(file=sys.stdout)
    _error_msg = str(traceback.format_exc(limit=2))[:4000]

    return _error_msg


def read_sql_text(_file_path):
    with open(_file_path) as f:
        return f.read()


def get_db_engine():
    config = get_config()
    db_cfg = config['Connections']['AleaccBI']

    db_engine = create_engine(
        "mssql+pymssql://{}:{}@{}:{}/{}".format(
            db_cfg['UserName'],
            db_cfg['Password'],
            db_cfg['Server'],
            db_cfg['Port'],
            db_cfg['Database']
        )
    )

    return db_engine


def get_property_db_engine():
    config = get_config()

    db_cfg = config['Connections']['Property']
    if config['SSH_Tunnel']:
        sandbox_cfg = config['Sandbox']
        sandbox = SSHTunnelForwarder(
            ssh_address_or_host=(sandbox_cfg['Server'], sandbox_cfg['Port']),
            ssh_config_file=sandbox_cfg['SSH_Config'],
            # ssh_private_key_password='',
            remote_bind_address=(db_cfg['Server'], db_cfg['Port'])
        )
        sandbox.start()

        db_password = db_cfg['Password']
        if db_cfg['EncryptedKey']:
            db_password = Crypto.decrypt(db_password, db_cfg['EncryptedKey'])

        local_port = str(sandbox.local_bind_port)
        db_engine = create_engine('mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'
                                  .format(db_cfg['UserName'], db_password, '127.0.0.1', local_port, db_cfg['Database']),
                                  pool_recycle=1)

        return db_engine
    else:
        db_password = db_cfg['Password']
        if db_cfg['EncryptedKey']:
            db_password = Crypto.decrypt(db_password, db_cfg['EncryptedKey'])

        db_engine = create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.format(db_cfg['UserName'], db_password, db_cfg['Server'],
                                                                          db_cfg['Port'], db_cfg['Database']))

        return db_engine


class Enum:
    class Validation:
        PASSED = 'Passed'
        FAILED = 'Failed'


class Crypto:
    # Reference: https://cryptography.io/en/latest/fernet/
    @staticmethod
    def create_key():
        return (Fernet.generate_key()).decode()

    @staticmethod
    def encrypt(data, key):
        fer = Fernet(key)
        return (fer.encrypt(data=data.encode())).decode()

    @staticmethod
    def decrypt(encrypted_data, key):
        fer = Fernet(key)
        return (fer.decrypt(token=encrypted_data.encode())).decode()


if __name__ == '__main__':
    _key = Crypto.create_key()
    encrypted = Crypto.encrypt('your-password-to-encrypt', key=_key)
    decrypted = Crypto.decrypt(encrypted, key=_key)

    print("Your randomly generated key: {}".format(_key))
    print("Your encrypted password: {}".format(encrypted))
    # print("Your decrypted password: {}".format(decrypted))
