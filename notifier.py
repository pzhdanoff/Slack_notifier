#!/usr/bin/env python3

from datetime import datetime, timedelta
from configparser import ConfigParser
from dotenv import load_dotenv
from slack import WebClient
from threading import Thread
from pathlib import Path
from queue import Queue
from time import sleep
import psycopg2 as pg
import requests
import sys
import os

config = ConfigParser()
config.read('conf.ini')
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)
client = WebClient(token=os.environ['SLACK_BOT_TOKEN'])
docQueue = Queue()
identifiersArray = set()


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
    def searchDocSelect(dateFmt, status, searchOption) -> str:
        select = f"""select document_id, {dateFmt}, action_id
                         from mdlp_meta.outcome_documents
                         where doc_status in ('{status}')
                         and (doc_date::date between current_date - 1 and current_date) {searchOption};"""
        return select

    @staticmethod
    def fourErrorDesc() -> str:
        select = """select document_id, doc_date, action_id
                    from mdlp_meta.outcome_documents
                    where doc_business_error_desc ilike '%Некорректная операция (операция не может быть выполнена для указанных реквизитов)%'
                    and action_id not in (702)
                    and (doc_date::date between current_date - 1 and current_date);"""
        return select

    @staticmethod
    def resolvSelect(docId):
        select = f"""select document_id, doc_status
                     from mdlp_meta.outcome_documents
                     where document_id = '{docId}'
                     and doc_status in ('PROCESSED_DOCUMENT', 'FAILED_RESULT_READY')
                     and (doc_business_error_desc not ilike '%Некорректная операция (операция не может быть выполнена для указанных реквизитов)%'
                     or doc_business_error_desc is null);"""
        return select


def message(message, color, data) -> dict:
    return {
        "color": color,
        "text": message,
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
            if row[2]:
                idArray.append(f'xml_doc_id: {row[0]}, action_id: {row[2]}\n')
            else:
                idArray.append(f'xml_doc_id: {row[0]}\n')
            docQueue.put(row[0])


def alerting():
    db = GetDB('mdlp_db')
    while True:
        failedArray = []
        fourErrorArray = []
        uploadingArray = []
        processingArray = []
        coreProcessedIdArray = []
        coreProcessingIdArray = []
        connect = db.connection()
        curs = connect.cursor()
        try:
            curs.execute(db.fourErrorDesc())
            attachBuilder(curs.fetchall(), fourErrorArray)
            if len(fourErrorArray) != 0:
                requests.post(os.environ['HOOK_URL'], json=message(
                        f":boom: [ALERT] Обнаружены документы с \'code: 4\'"
                        f" в количестве {len(fourErrorArray)} шт.\n",
                        '#FF0000',
                        ''.join(fourErrorArray))
                              )
            curs.execute(db.searchDocSelect('doc_date', 'FAILED', 'and action_id not in (511)'))
            attachBuilder(curs.fetchall(), failedArray)
            if len(failedArray) != 0 and len(failedArray) <= 100:
                requests.post(os.environ['HOOK_URL'], json=message(
                        f":boom: [ALERT] Обнаружены документы в статусе \'FAILED\'"
                        f" более 1 часа в количестве {len(failedArray)} шт.\n",
                        '#FF0000',
                        ''.join(failedArray))
                              )
            curs.execute(
                db.searchDocSelect('core_processing_end', 'CORE_PROCESSED_DOCUMENT', 'and action_id not in (511)'))
            attachBuilder(curs.fetchall(), coreProcessedIdArray)
            if len(coreProcessedIdArray) != 0:
                requests.post(os.environ['HOOK_URL'], json=message(
                        f":boom: [ALERT] Обнаружены документы в статусе \'CORE_PROCESSED_DOCUMENT\'"
                        f" более 1 часа в количестве {len(coreProcessedIdArray)} шт.\n",
                        '#FF0000',
                        ''.join(coreProcessedIdArray))
                              )
            curs.execute(
                db.searchDocSelect('core_processing_start', 'CORE_PROCESSING_DOCUMENT', 'and action_id not in (511)'))
            attachBuilder(curs.fetchall(), coreProcessingIdArray)
            if len(coreProcessingIdArray) != 0:
                requests.post(os.environ['HOOK_URL'], json=message(
                        f":boom: [ALERT] Обнаружены документы в статусе \'CORE_PROCESSING_DOCUMENT\'"
                        f" более 1 часа в количестве {len(coreProcessingIdArray)} шт.\n",
                        '#FF0000',
                        ''.join(coreProcessingIdArray))
                              )
            curs.execute(db.searchDocSelect('doc_date', 'UPLOADING_DOCUMENT', "and link not ilike '%/opt/gluster%'"))
            attachBuilder(curs.fetchall(), uploadingArray)
            if len(uploadingArray) != 0:
                requests.post(os.environ['HOOK_URL'], json=message(
                        f":boom: [ALERT] Обнаружены документы в статусе \'UPLOADING_DOCUMENT\'"
                        f" более 1 часа в количестве {len(uploadingArray)} шт.\n",
                        '#FF0000',
                        ''.join(uploadingArray))
                              )
            curs.execute(db.searchDocSelect('doc_date', 'PROCESSING_DOCUMENT', 'and action_id not in (511)'))
            attachBuilder(curs.fetchall(), processingArray)
            if len(processingArray) != 0:
                requests.post(os.environ['HOOK_URL'], json=message(
                        f":boom: [ALERT] Обнаружены документы в статусе \'PROCESSING_DOCUMENT\'"
                        f" более 1 часа в количестве {len(processingArray)} шт.\n",
                        '#FF0000',
                        ''.join(processingArray))
                              )
            if not docQueue.empty():
                requests.post(os.environ['HOOK_URL'], json={
                    "color": '#FF0000',
                    "text": f':boom: [ALERT] Всего обнаружено документов: {docQueue.qsize()}'
                }
                              )
        except Exception:
            sys.exit(1)
        finally:
            curs.close()
            connect.close()
        sleep(1800)


def resolving():
    global identifiersArray
    dataBase = GetDB('mdlp_db')
    while True:
        if not docQueue.empty():
            for item in range(docQueue.qsize()):
                identifiersArray.add(docQueue.get())
                docQueue.task_done()
        if len(identifiersArray) != 0:
            connect = dataBase.connection()
            curs = connect.cursor()
            resolvArray = []
            try:
                for item in list(identifiersArray):
                    curs.execute(dataBase.resolvSelect(item))
                    row = curs.fetchone()
                    if row:
                        resolvArray.append(f'xml_doc_id: {row[0]}\n')
                        identifiersArray.remove(item)
            except Exception:
                sys.exit(1)
            finally:
                curs.close()
                connect.close()
            if len(resolvArray) > 0:
                requests.post(os.environ['HOOK_URL'], json=message(
                        f':white_check_mark: [RESOLVED] Обработано документов {len(resolvArray)}.'
                        f' Осталось обработать {len(identifiersArray)}',
                        '#40ff00',
                        ''.join(resolvArray))
                              )
        sleep(300)


if __name__ == '__main__':
    thread_1 = Thread(target=alerting)
    thread_2 = Thread(target=resolving)
    thread_1.start()
    thread_2.start()
    thread_1.join()
    thread_2.join()
