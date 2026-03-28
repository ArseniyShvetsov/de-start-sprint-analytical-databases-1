from airflow import DAG
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
import boto3
import os
import logging
from typing import List, Dict

# Настройка логирования
logger = logging.getLogger(__name__)

# Список файлов для скачивания
FILES_TO_DOWNLOAD = [
    {'key': 'group_log.csv', 'filename': 'group_log.csv'}  
]

DOWNLOAD_PATH = '/data'  
CONNECTION_ID = 'yandex_s3'  # ID подключения в Airflow

@dag(
    dag_id='project_download_s3_files',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': 30,
    },
    description='Скачивание файла group_log.csv из Yandex Object Storage',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['s3', 'download', 'manual', 'verification', 'project']
)
def download_all_files():
    
    @task
    def setup_directory():
        """Создание директории для скачивания файлов"""
        try:
            if not os.path.exists(DOWNLOAD_PATH):
                os.makedirs(DOWNLOAD_PATH, exist_ok=True)
                logger.info(f"Создана директория: {DOWNLOAD_PATH}")
            else:
                logger.info(f"Директория уже существует: {DOWNLOAD_PATH}")
            
            if os.access(DOWNLOAD_PATH, os.W_OK):
                logger.info(f"Есть права на запись в {DOWNLOAD_PATH}")
            else:
                raise PermissionError(f"Нет прав на запись в {DOWNLOAD_PATH}")
            
            import shutil
            free_space = shutil.disk_usage(DOWNLOAD_PATH).free
            free_space_mb = free_space / (1024 * 1024)
            logger.info(f"Свободно места: {free_space_mb:.2f} МБ")
            
            return f"Директория {DOWNLOAD_PATH} готова"
            
        except Exception as e:
            logger.error(f"Ошибка при настройке директории: {str(e)}")
            raise
    
    @task
    def download_file(file_info: Dict):
        """Скачивание одного файла из S3 хранилища"""
        file_key = file_info['key']
        file_name = file_info['filename']
        full_path = os.path.join(DOWNLOAD_PATH, file_name)
        
        try:
            logger.info(f"Начинаю скачивание файла: {file_key}")
            
            # Получаем credentials из Connection Airflow
            conn = BaseHook.get_connection(CONNECTION_ID)
            
            logger.info(f"Использую подключение: {CONNECTION_ID}")
            logger.info(f"Login: {conn.login}")
            
            # Инициализируем S3 клиент
            session = boto3.session.Session()
            s3_client = session.client(
                service_name='s3',
                endpoint_url='https://storage.yandexcloud.net',
                aws_access_key_id=conn.login,
                aws_secret_access_key=conn.password,
            )
            
            # Получаем информацию о файле перед скачиванием
            try:
                head_response = s3_client.head_object(
                    Bucket='sprint6',
                    Key=file_key
                )
                file_size_s3 = head_response['ContentLength']
                logger.info(f"Размер файла в S3: {file_size_s3} байт")
            except Exception as e:
                logger.warning(f"Не удалось получить информацию о файле {file_key}: {str(e)}")
                file_size_s3 = None
            
            # Скачиваем файл
            s3_client.download_file(
                Bucket='sprint6',
                Key=file_key,
                Filename=full_path
            )
            
            logger.info(f"Файл успешно скачан: {full_path}")
            
            # Проверяем, что файл скачался
            if os.path.exists(full_path):
                file_size_local = os.path.getsize(full_path)
                logger.info(f"Размер локального файла: {file_size_local} байт")
                
                if file_size_s3 and file_size_local != file_size_s3:
                    logger.warning(f"Размеры не совпадают! S3: {file_size_s3}, Local: {file_size_local}")
                
                return {
                    'file_name': file_name,
                    'path': full_path,
                    'size': file_size_local,
                    'status': 'success'
                }
            else:
                raise FileNotFoundError(f"Файл {full_path} не найден после скачивания")
                
        except Exception as e:
            logger.error(f"Ошибка при скачивании файла {file_key}: {str(e)}")
            raise
    
    @task
    def verify_file(file_info: Dict):
        """Проверка скачанного файла (читает 10 строк)"""
        file_name = file_info['file_name']
        file_path = file_info['path']
        file_size = file_info['size']
        
        try:
            logger.info(f"Начинаю проверку файла: {file_name}")
            
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Файл {file_path} не существует")
            
            if file_size == 0:
                raise ValueError(f"Файл {file_name} пустой")
            
            # Пробуем разные кодировки
            encodings = ['utf-8', 'cp1251', 'latin-1']
            file_content = []
            used_encoding = None
            
            for encoding in encodings:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        for i, line in enumerate(f):
                            if i < 10:
                                file_content.append(line.strip())
                            else:
                                break
                        used_encoding = encoding
                        logger.info(f"Файл {file_name} успешно прочитан в кодировке {encoding}")
                        break
                except UnicodeDecodeError:
                    continue
            
            if used_encoding is None:
                raise ValueError(f"Не удалось определить кодировку для файла {file_name}")
            
            # Логируем информацию о файле
            logger.info(f"=== Информация о файле {file_name} ===")
            logger.info(f"Путь: {file_path}")
            logger.info(f"Размер: {file_size} байт ({file_size / 1024:.2f} КБ)")
            logger.info(f"Кодировка: {used_encoding}")
            logger.info(f"Первые {len(file_content)} строк:")
            
            for i, line in enumerate(file_content, 1):
                logger.info(f"  {i}: {line[:200]}{'...' if len(line) > 200 else ''}")
            
            # Проверяем заголовки CSV
            if file_content and ',' in file_content[0]:
                headers = file_content[0].split(',')
                logger.info(f"Заголовки CSV: {headers[:5]}...")  # Показываем первые 5 заголовков
            
            logger.info(f"=== Проверка файла {file_name} завершена ===")
            
            return {
                'file_name': file_name,
                'size': file_size,
                'encoding': used_encoding,
                'first_10_lines': file_content,
                'status': 'verified'
            }
            
        except Exception as e:
            logger.error(f"Ошибка при проверке файла {file_name}: {str(e)}")
            raise
    
    @task
    def verify_all_files(files_info: List[Dict]):
        """Сводная проверка всех скачанных файлов"""
        logger.info("=" * 50)
        logger.info("Сводный отчет по всем файлам:")
        logger.info("=" * 50)
        
        total_files = len(files_info)
        total_size = 0
        
        for file_info in files_info:
            logger.info(f"Файл: {file_info['file_name']}")
            logger.info(f"  Размер: {file_info['size']} байт")
            logger.info(f"  Кодировка: {file_info.get('encoding', 'unknown')}")
            logger.info(f"  Статус: {file_info['status']}")
            total_size += file_info['size']
        
        logger.info(f"Всего файлов: {total_files}")
        logger.info(f"Общий размер: {total_size} байт ({total_size / (1024*1024):.2f} МБ)")
        logger.info("=" * 50)
        
        return {
            'total_files': total_files,
            'total_size': total_size,
            'files': files_info
        }
    
    # Создаем задачи
    setup_task = setup_directory()
    
    # Скачиваем все файлы
    download_tasks = []
    verify_tasks = []
    
    for file_info in FILES_TO_DOWNLOAD:
        download_task = download_file(file_info)
        verify_task = verify_file(download_task)
        
        download_tasks.append(download_task)
        verify_tasks.append(verify_task)
        
        setup_task >> download_task >> verify_task
    
    # Финальная проверка всех файлов
    final_verify = verify_all_files(verify_tasks)
    verify_tasks >> final_verify


# Создаем экземпляр DAG
dag = download_all_files()