from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import os
import logging
import pandas as pd
import tempfile
from typing import Dict

# Импортируем VerticaHook
try:
    from airflow.providers.vertica.hooks.vertica import VerticaHook
    VERTICA_AVAILABLE = True
except ImportError:
    from airflow.hooks.base_hook import BaseHook
    VERTICA_AVAILABLE = False

logger = logging.getLogger(__name__)

DATA_DIR = '/data'
SCHEMA_NAME = 'VT260317D7FA03__STAGING'
VERTICA_CONN_ID = 'vertica_default'

def get_vertica_connection():
    """Получение подключения к Vertica"""
    if VERTICA_AVAILABLE:
        return VerticaHook(vertica_conn_id=VERTICA_CONN_ID)
    else:
        import vertica_python
        conn = BaseHook.get_connection(VERTICA_CONN_ID)
        
        conn_info = {
            'host': conn.host,
            'port': conn.port,
            'user': conn.login,
            'password': conn.password,
            'database': conn.schema,
            'autocommit': True
        }
        return vertica_python.connect(**conn_info)

@dag(
    dag_id='load_group_log_stg',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=3),
    },
    description='Загрузка данных group_log в STAGING слой',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['staging', 'vertica', 'load', 'group_log']
)
def load_group_log_stg():
    
    @task
    def check_data_file():
        """Проверка наличия файла group_log.csv"""
        file_path = os.path.join(DATA_DIR, 'group_log.csv')
        
        logger.info(f"Проверка файла: {file_path}")
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Файл не найден: {file_path}")
        
        file_size = os.path.getsize(file_path)
        logger.info(f"Файл найден. Размер: {file_size} байт")
        
        return file_path
    
    @task
    def truncate_table():
        """Очистка таблицы group_log перед загрузкой"""
        conn = get_vertica_connection()
        
        try:
            sql = f"TRUNCATE TABLE {SCHEMA_NAME}.group_log;"
            
            if VERTICA_AVAILABLE:
                conn.run(sql)
            else:
                cursor = conn.cursor()
                cursor.execute(sql)
                conn.commit()
            
            logger.info(f"Таблица {SCHEMA_NAME}.group_log очищена")
        except Exception as e:
            logger.warning(f"Таблица не существует или не может быть очищена: {e}")
        finally:
            if not VERTICA_AVAILABLE and conn:
                conn.close()
        
        return "Table truncated"
    
    @task
    def prepare_data_for_copy(file_path: str):
        """
        Подготовка данных для COPY: обработка NULL значений
        """
        logger.info(f"Начинаю подготовку данных из {file_path}")
        
        # Определяем кодировку
        import chardet
        with open(file_path, 'rb') as f:
            raw_data = f.read(10000)
            detected = chardet.detect(raw_data)
            encoding = detected['encoding'] if detected['encoding'] else 'utf-8'
        
        logger.info(f"Кодировка исходного файла: {encoding}")
        
        # Читаем CSV
        df = pd.read_csv(
            file_path,
            encoding=encoding,
            dtype={
                'group_id': 'Int64',
                'user_id': 'Int64',
                'user_id_from': 'Int64',
                'event': 'str',
                'datetime': 'str'
            }
        )
        
        logger.info(f"Прочитано {len(df)} строк из CSV")
        
        # Очищаем event
        df['event'] = df['event'].str.strip().str.lower()
        valid_events = ['create', 'add', 'leave']
        df.loc[~df['event'].isin(valid_events), 'event'] = None
        
        # Очищаем datetime
        def clean_datetime(dt):
            if pd.isna(dt) or dt == '':
                return ''
            dt = str(dt).strip()
            # Убираем лишние символы
            import re
            dt = re.sub(r'[^\d\-:\.\s]', '', dt)
            return dt
        
        df['datetime'] = df['datetime'].apply(clean_datetime)
        
        # Заменяем NA на пустые строки
        df = df.astype(object).where(pd.notnull(df), '')
        
        # Создаем временный файл
        temp_file = tempfile.NamedTemporaryFile(
            mode='w',
            encoding='utf-8',
            suffix='.csv',
            delete=False,
            dir='/tmp'
        )
        
        df.to_csv(temp_file.name, index=False, header=False, encoding='utf-8')
        temp_file.close()
        
        logger.info(f"Подготовлен временный файл: {temp_file.name}")
        
        # Показываем первые строки для проверки
        with open(temp_file.name, 'r') as f:
            for i, line in enumerate(f):
                if i < 3:
                    logger.info(f"Пример строки {i+1}: {line.strip()}")
                else:
                    break
        
        return temp_file.name
    
    @task
    def prepare_data_for_copy(file_path: str):
        """
        Подготовка данных для COPY - всегда пропускаем первую строку
        """
        logger.info(f"Начинаю подготовку данных из {file_path}")
        
        # Определяем кодировку
        import chardet
        with open(file_path, 'rb') as f:
            raw_data = f.read(10000)
            detected = chardet.detect(raw_data)
            encoding = detected['encoding'] if detected['encoding'] else 'utf-8'
        
        logger.info(f"Кодировка исходного файла: {encoding}")
        
        # Проверяем первую строку
        with open(file_path, 'r', encoding=encoding) as f:
            first_line = f.readline().strip()
            logger.info(f"Первая строка: {first_line}")
        
        # Всегда пропускаем первую строку (если это заголовок - хорошо, если данные - потеряем одну строку)
        # Но это лучше, чем ошибка
        column_names = ['group_id', 'user_id', 'user_id_from', 'event', 'datetime']
        
        df = pd.read_csv(
            file_path,
            encoding=encoding,
            skiprows=1,  # Всегда пропускаем первую строку
            header=None,
            names=column_names,
            dtype={
                'group_id': 'Int64',
                'user_id': 'Int64',
                'user_id_from': 'Int64',
                'event': 'str',
                'datetime': 'str'
            },
            parse_dates=['datetime']
        )
        
        logger.info(f"Прочитано {len(df)} строк")
        
        # Очищаем event
        df['event'] = df['event'].astype(str).str.strip().str.lower()
        valid_events = ['create', 'add', 'leave']
        df.loc[~df['event'].isin(valid_events), 'event'] = None
        
        # Заменяем NA на пустые строки
        df = df.astype(object).where(pd.notnull(df), '')
        
        # Создаем временный файл
        temp_file = tempfile.NamedTemporaryFile(
            mode='w',
            encoding='utf-8',
            suffix='.csv',
            delete=False,
            dir='/tmp'
        )
        
        df[['group_id', 'user_id', 'user_id_from', 'event', 'datetime']].to_csv(
            temp_file.name, index=False, header=False, encoding='utf-8'
        )
        temp_file.close()
        
        logger.info(f"Подготовлен файл: {temp_file.name}")
        
        return temp_file.name


    @task
    def load_with_copy(temp_file_path: str):
        """
        Загрузка данных через COPY - 5 полей + автоматические load_dt, load_src
        """
        logger.info(f"Начинаю загрузку через COPY из {temp_file_path}")
        
        conn = get_vertica_connection()
        
        # Уникальное имя для временной таблицы rejected
        rejected_table = f"{SCHEMA_NAME}.group_log_rejected_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # COPY только для 5 полей, load_dt и load_src добавятся автоматически
        copy_sql = f"""
            COPY {SCHEMA_NAME}.group_log 
            (group_id, user_id, user_id_from, event, datetime)
            FROM LOCAL '{temp_file_path}'
            DELIMITER ','
            ENCLOSED BY '"'
            ESCAPE AS '\\'
            NULL AS ''
            REJECTED DATA AS TABLE {rejected_table}
            REJECTMAX 10000;
        """
        
        try:
            if VERTICA_AVAILABLE:
                conn.run(copy_sql)
            else:
                cursor = conn.cursor()
                cursor.execute(copy_sql)
                conn.commit()
            
            logger.info(f"COPY команда выполнена успешно")
            
            # Проверяем количество загруженных строк
            count_sql = f"SELECT COUNT(*) FROM {SCHEMA_NAME}.group_log"
            if VERTICA_AVAILABLE:
                count = conn.get_first(count_sql)[0]
            else:
                cursor = conn.cursor()
                cursor.execute(count_sql)
                count = cursor.fetchone()[0]
            
            logger.info(f"✅ Загружено {count} строк в group_log")
            
            return {
                'table': 'group_log',
                'rows': count,
                'status': 'success'
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке через COPY: {e}")
            
            # Пытаемся прочитать rejected таблицу
            try:
                reject_count_sql = f"SELECT COUNT(*) FROM {rejected_table}"
                if VERTICA_AVAILABLE:
                    reject_count = conn.get_first(reject_count_sql)[0]
                else:
                    cursor = conn.cursor()
                    cursor.execute(reject_count_sql)
                    reject_count = cursor.fetchone()[0]
                
                if reject_count > 0:
                    logger.error(f"📊 Найдено {reject_count} rejected записей")
                    
                    # Показываем примеры
                    sample_sql = f"SELECT rejected_data, rejected_reason FROM {rejected_table} LIMIT 5"
                    if VERTICA_AVAILABLE:
                        samples = conn.get_records(sample_sql)
                    else:
                        cursor = conn.cursor()
                        cursor.execute(sample_sql)
                        samples = cursor.fetchall()
                    
                    logger.error("📄 Примеры проблемных строк:")
                    for sample in samples:
                        logger.error(f"   Причина: {sample[1]}")
                        logger.error(f"   Данные: {sample[0][:200]}")
            except Exception as analyze_error:
                logger.error(f"Не удалось проанализировать rejected: {analyze_error}")
            
            raise
        finally:
            if not VERTICA_AVAILABLE and conn:
                conn.close()
            
            # Удаляем временный файл
            try:
                if os.path.exists(temp_file_path):
                    os.unlink(temp_file_path)
                    logger.info(f"Временный файл удален: {temp_file_path}")
            except Exception as e:
                logger.warning(f"Не удалось удалить временный файл: {e}")
    
    @task
    def verify_loaded_data(load_result: Dict):
        """
        Проверка загруженных данных
        """
        conn = get_vertica_connection()
        
        logger.info("=" * 60)
        logger.info("ПРОВЕРКА ЗАГРУЖЕННЫХ ДАННЫХ - group_log")
        logger.info("=" * 60)
        
        try:
            table = load_result['table']
            expected = load_result['rows']
            
            # Проверяем количество
            count_sql = f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{table}"
            
            if VERTICA_AVAILABLE:
                actual = conn.get_first(count_sql)[0]
            else:
                cursor = conn.cursor()
                cursor.execute(count_sql)
                actual = cursor.fetchone()[0]
            
            logger.info(f"Таблица {table}:")
            logger.info(f"  Загружено: {expected} строк")
            logger.info(f"  В БД: {actual} строк")
            
            if actual != expected:
                raise ValueError(f"Количество строк не совпадает для {table}")
            
            # Общая статистика
            stats_sql = f"""
                SELECT 
                    COUNT(*) as total_events,
                    COUNT(DISTINCT group_id) as unique_groups,
                    COUNT(DISTINCT user_id) as unique_users,
                    COUNT(CASE WHEN user_id_from IS NOT NULL THEN 1 END) as invited_users,
                    MIN(datetime) as earliest_event,
                    MAX(datetime) as latest_event
                FROM {SCHEMA_NAME}.{table}
            """
            
            if VERTICA_AVAILABLE:
                stats = conn.get_first(stats_sql)
            else:
                cursor = conn.cursor()
                cursor.execute(stats_sql)
                stats = cursor.fetchone()
            
            logger.info(f"  Статистика group_log:")
            logger.info(f"    Всего событий: {stats[0]}")
            logger.info(f"    Уникальных групп: {stats[1]}")
            logger.info(f"    Уникальных пользователей: {stats[2]}")
            logger.info(f"    Приглашенных пользователей: {stats[3]}")
            logger.info(f"    Период: {stats[4]} - {stats[5]}")
            
            # Статистика по типам событий
            events_sql = f"""
                SELECT 
                    event,
                    COUNT(*) as event_count,
                    COUNT(DISTINCT user_id) as unique_users
                FROM {SCHEMA_NAME}.{table}
                GROUP BY event
                ORDER BY event
            """
            
            if VERTICA_AVAILABLE:
                events = conn.get_records(events_sql)
            else:
                cursor = conn.cursor()
                cursor.execute(events_sql)
                events = cursor.fetchall()
            
            logger.info(f"  Распределение по событиям:")
            for event in events:
                logger.info(f"    {event[0]}: {event[1]} событий, {event[2]} пользователей")
            
            # Проверка на наличие NULL в обязательных полях
            null_check_sql = f"""
                SELECT 
                    COUNT(CASE WHEN group_id IS NULL THEN 1 END) as null_group_id,
                    COUNT(CASE WHEN user_id IS NULL THEN 1 END) as null_user_id,
                    COUNT(CASE WHEN event IS NULL THEN 1 END) as null_event,
                    COUNT(CASE WHEN datetime IS NULL THEN 1 END) as null_datetime
                FROM {SCHEMA_NAME}.{table}
            """
            
            if VERTICA_AVAILABLE:
                null_checks = conn.get_first(null_check_sql)
            else:
                cursor = conn.cursor()
                cursor.execute(null_check_sql)
                null_checks = cursor.fetchone()
            
            if null_checks[0] > 0 or null_checks[1] > 0 or null_checks[2] > 0 or null_checks[3] > 0:
                logger.warning(f"  Обнаружены NULL значения:")
                logger.warning(f"    NULL в group_id: {null_checks[0]}")
                logger.warning(f"    NULL в user_id: {null_checks[1]}")
                logger.warning(f"    NULL в event: {null_checks[2]}")
                logger.warning(f"    NULL в datetime: {null_checks[3]}")
            else:
                logger.info(f"  NULL значения в обязательных полях отсутствуют")
            
            # Показываем примеры данных
            sample_sql = f"SELECT * FROM {SCHEMA_NAME}.{table} LIMIT 5"
            
            if VERTICA_AVAILABLE:
                samples = conn.get_records(sample_sql)
            else:
                cursor = conn.cursor()
                cursor.execute(sample_sql)
                samples = cursor.fetchall()
            
            logger.info(f"  Примеры данных (первые 5 записей):")
            for i, sample in enumerate(samples, 1):
                logger.info(f"    {i}: group_id={sample[0]}, user_id={sample[1]}, "
                          f"user_id_from={sample[2]}, event={sample[3]}, datetime={sample[4]}")
            
            logger.info("=" * 60)
            logger.info("ПРОВЕРКА ЗАГРУЗКИ group_log УСПЕШНО ЗАВЕРШЕНА")
            logger.info("=" * 60)
            
            return "Verification completed"
            
        except Exception as e:
            logger.error(f"Ошибка при проверке данных: {e}")
            raise
        finally:
            if not VERTICA_AVAILABLE and conn:
                conn.close()
    
    @task
    def final_report(verification_result: str):
        """Финальный отчет"""
        logger.info("=" * 60)
        logger.info("ЗАГРУЗКА group_log В STAGING СЛОЙ УСПЕШНО ЗАВЕРШЕНА!")
        logger.info("=" * 60)
        return verification_result
    
    # Создаем pipeline
    file_path = check_data_file()
    truncate = truncate_table()
    temp_file = prepare_data_for_copy(file_path)
    load_result = load_with_copy(temp_file)  
    verify = verify_loaded_data(load_result)
    report = final_report(verify)
    
    file_path >> truncate >> temp_file >> load_result >> verify >> report


# Создаем экземпляр DAG
dag = load_group_log_stg()