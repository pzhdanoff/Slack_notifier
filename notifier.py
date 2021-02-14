#!/usr/bin/env python3

from datetime import datetime, timedelta
from configparser import ConfigParser
from dotenv import load_dotenv
from slack import WebClient
from threading import Thread
from pathlib import Path
from time import sleep
import psycopg2 as pg
import requests
import os

config = ConfigParser()
config.read('conf.ini')
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)
client = WebClient(token=os.environ['SLACK_BOT_TOKEN'])


class GetDB:

    def __init__(self, database):
        self.__host = config['database']['db_host']
        self.__port = config['database']['db_port']
        self.__user = config['database']['db_user']
        self.__passwd = config['database']['db_password']
        self.__database = database

    def connection(self):
        connection = pg.connect(
                dbname=self.__database,
                user=self.__user,
                password=self.__passwd,
                host=self.__host,
                port=self.__port
        )
        return connection

    @staticmethod
    def coreProcessedSelect() -> str:
        select = f"""select document_id, core_processing_end, action_id
                     from mdlp_meta.outcome_documents where doc_status in ('CORE_PROCESSED_DOCUMENT')
                     and doc_date::date = current_date and action_id not in (10511);"""
        return select

    @staticmethod
    def coreProcessingSelect() -> str:
        select = """select document_id, core_processing_start, action_id
                    from mdlp_meta.outcome_documents where doc_status in ('CORE_PROCESSING_DOCUMENT')
                    and doc_date::date = current_date and action_id not in (10511);"""
        return select

    @staticmethod
    def uploadingSelect() -> str:
        select = """select document_id, doc_date, action_id from mdlp_meta.outcome_documents where doc_status in ('UPLOADING_DOCUMENT')
                    and doc_date::date = current_date and link not ilike '%/opt/gluster%';"""
        return select

    @staticmethod
    def fourErrorDesc():
        select = """select document_id, doc_date, action_id
                    from mdlp_meta.outcome_documents
                    where doc_business_error_desc ilike '%Некорректная операция (операция не может быть выполнена для указанных реквизитов)%'
                    and doc_date::date = current_date;"""
        return select


def message(role, color, status, data, dataCount) -> dict:
    return {
        "color": color,
        "text": f":boom: {role} Документы в статусе \'{status}\' более 1 часа ({dataCount} шт.)!\n",
        "attachments": [
            {
                "color": color,
                "text": data
            }
        ]
    }


def attachBuilder(fetchArray: list, idArray: list):
    for row in fetchArray:
        if row[1] < (datetime.now() - timedelta(hours=1)):
            idArray.append(f'xml_doc_id: {row[0]}, action_id: {row[2]}\n')


if __name__ == '__main__':
    db = GetDB('mdlp_db')
    while True:
        coreProcessedIdArray = []
        coreProcessingIdArray = []
        uploadingArray = []
        fourErrorArray = []
        connect = db.connection()
        curs = connect.cursor()
        try:
            curs.execute(db.coreProcessedSelect())
            attachBuilder(curs.fetchall(), coreProcessedIdArray)
            if len(coreProcessedIdArray) != 0:
                requests.post(os.environ['HOOK_URL'], json=message(
                        '[ALERT]',
                        '#FF0000',
                        'CORE_PROCESSED_DOCUMENT',
                        ''.join(coreProcessedIdArray), len(coreProcessedIdArray))
                              )
            curs.execute(db.coreProcessingSelect())
            attachBuilder(curs.fetchall(), coreProcessingIdArray)
            if len(coreProcessingIdArray) != 0:
                requests.post(os.environ['HOOK_URL'], json=message(
                        '[ALERT]',
                        '#FF0000',
                        'CORE_PROCESSING_DOCUMENT',
                        ''.join(coreProcessingIdArray), len(coreProcessingIdArray))
                              )
            curs.execute(db.uploadingSelect())
            attachBuilder(curs.fetchall(), uploadingArray)
            if len(uploadingArray) != 0:
                requests.post(os.environ['HOOK_URL'], json=message(
                        '[ALERT]',
                        '#FF0000',
                        'UPLOADING_DOCUMENT',
                        ''.join(uploadingArray), len(uploadingArray))
                              )
            attachBuilder(curs.fetchall(), fourErrorArray)
            if len(fourErrorArray) != 0:
                requests.post(os.environ['HOOK_URL'], json=message(
                        '[ALERT]',
                        '#FF0000',
                        'Ошибка 4',
                        ''.join(fourErrorArray), len(fourErrorArray))
                              )
        except Exception as err:
            print(err)
        finally:
            curs.close()
            connect.close()
        sleep(900)
