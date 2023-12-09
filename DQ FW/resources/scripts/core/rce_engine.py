from pathlib import Path
import time

from resources.scripts.core.rce_connection import ConnectionDB
from resources.scripts.core.rce_temp_tables import TempTablesExec
from resources.scripts.core.rce_checks import ChecksExec
from resources.scripts.core.rce_details import DetailsExec
from resources.scripts.functions.rc_functions import write_textbox_log, handle_query_err


class ChecksEngine:
    """Ядро механизма, который занимается запуском скриптов проверок."""

    def __init__(self, username, password, dbms, host, service_name, port, database, charset, check_on_date, branch,
                 retries_on_error, details_to_excel, results_to_excel, log_tk_object, dict_temp_table_sql_queries,
                 dict_sql_checks_queries, dict_sql_details, file_format, file_header, file_timestamp, file_encoding):

        self.username, self.password, self.dbms, self.host = username, password, dbms, host
        self.service_name, self.port, self.database, self.charset = service_name, port, database, charset
        self.dict_temp_table_sql_queries, self.dict_sql_checks_queries = dict_temp_table_sql_queries, dict_sql_checks_queries
        self.dict_sql_details, self.check_on_date, self.branch = dict_sql_details, check_on_date, branch
        self.log_tk_object, self.retries_on_error = log_tk_object, retries_on_error
        self.details_to_excel, self.results_to_excel, self.connection_flag = details_to_excel, results_to_excel, 0
        self.file_format, self.file_header, self.file_encoding,  = file_format, file_header, file_encoding
        self.file_timestamp = file_timestamp

    def connect(self, conn_retries=3):
        # Создаем объект подключения к БД.
        conn = ConnectionDB(dbms=self.dbms, username=self.username, password=self.password, host=self.host,
                            service_name=self.service_name, port=self.port, database=self.database,
                            charset=self.charset, log_tk_object=self.log_tk_object)

        connection = conn.connect(conn_retries)
        if connection:
            self.connection_flag = 1
            return connection


    def disconnect(self, connection):
        # Закрываем подключение к БД.
        connection.close()
        self.connection_flag = 0
        write_textbox_log(self.log_tk_object, msg=f'Закрыл подключение к БД.')


    def execute_sql_temp_tables(self, connection):
        """Функция для выполнения временных таблиц."""

        if self.dict_temp_table_sql_queries:
            for key in self.dict_temp_table_sql_queries.keys():  # Ключ - полный адрес файла. Значение - SQL запрос в нем.
                sql_split = self.dict_temp_table_sql_queries[key].split(';')
                filename = Path(key).resolve().stem
                retries = self.retries_on_error

                while retries:
                    retries -= 1
                    try:
                        et = TempTablesExec(self.dbms, connection, sql_split, self.log_tk_object, filename)
                        et.execute_temp_tables()
                        break

                    except Exception as ex:
                        write_textbox_log(self.log_tk_object, msg=f'Возникло исключение {str(ex).strip()}.', sqlfile=filename)
                        q_err = handle_query_err(ex, filename)

                        if q_err == 1 and retries > 0: # Если ошибка обрабатываемая и не требует переподключения.
                            write_textbox_log(self.log_tk_object, msg=f'Попробую повторно выполнить запрос через 15 секунд.', sqlfile=filename)
                            time.sleep(15)
                        elif q_err == 0: # Необрабатываемая ошибка.
                            write_textbox_log(self.log_tk_object, msg=f'Перехожу к следующему файлу.', sqlfile=filename)
                            break
                        elif q_err == 3: # Ошибка обрабатываемая и требует переподключения.
                            write_textbox_log(self.log_tk_object, msg=f'Попробую повторно подключиться через 15 секунд.', sqlfile=filename)
                            time.sleep(15)
                            connection = self.connect(conn_retries=3)
                        elif q_err == 5: # Ошибка обрабатываемая и требует переподключения с увеличенным интервалом ожидания.
                            write_textbox_log(self.log_tk_object, msg=f'Попробую повторно подключиться через 5 минут.', sqlfile=filename)
                            time.sleep(300)
                            connection = self.connect(conn_retries=3)
                        elif q_err == 4: # Игнорируем ошибку и работаем дальше.
                            break
                        else:
                            write_textbox_log(self.log_tk_object, msg=f'Перехожу к следующей проверке.', sqlfile=filename)


    def execute_sql_checks(self, connection):
        """Функция для выполнения основных проверок."""
        if self.dict_sql_checks_queries:
            for key in self.dict_sql_checks_queries.keys():  # Ключ - полный адрес файла. Значение - SQL запрос в нем.
                sql_split = self.dict_sql_checks_queries[key].split(';')
                filename = Path(key).resolve().stem
                retries = self.retries_on_error

                while retries:
                    retries -= 1
                    try:
                        ec = ChecksExec(self.dbms, connection, filename, sql_split, self.check_on_date,
                                        self.branch, self.details_to_excel, self.results_to_excel,
                                        self.log_tk_object, self.file_format, self.file_header,
                                        self.file_timestamp, self.file_encoding)
                        ec.execute_sql_checks()
                        break

                    except Exception as ex:
                        write_textbox_log(self.log_tk_object, msg=f'Возникло исключение {str(ex).strip()}.', sqlfile=filename)
                        q_err = handle_query_err(ex, filename)

                        if q_err == 1 and retries > 0: # Если ошибка обрабатываемая и не требует переподключения.
                            write_textbox_log(self.log_tk_object, msg=f'Попробую повторно выполнить запрос через 15 секунд.', sqlfile=filename)
                            time.sleep(15)
                        elif q_err == 0: # Необрабатываемая ошибка.
                            write_textbox_log(self.log_tk_object, msg=f'Перехожу к следующему файлу с проверками.', sqlfile=filename)
                            break
                        elif q_err == 3: # Ошибка обрабатываемая и требует переподключения.
                            write_textbox_log(self.log_tk_object, msg=f'Попробую повторно подключиться через 15 секунд.', sqlfile=filename)
                            time.sleep(15)
                            connection = self.connect(conn_retries=3)
                        elif q_err == 5: # Ошибка обрабатываемая и требует переподключения с увеличенным интервалом ожидания.
                            write_textbox_log(self.log_tk_object, msg=f'Попробую повторно подключиться через 5 минут.', sqlfile=filename)
                            time.sleep(300)
                            connection = self.connect(conn_retries=3)
                        elif q_err == 4: # Игнорируем ошибку и работаем дальше.
                            break
                        else:
                            write_textbox_log(self.log_tk_object, msg=f'Перехожу к следующему файлу с проверками.', sqlfile=filename)


    def execute_sql_details(self, connection):
        """Функция для выгрузки детализаций."""
        if self.dict_sql_details:
            for key in self.dict_sql_details.keys():  # Ключ - полный адрес файла. Значение - SQL запрос в нем.
                sql_split = self.dict_sql_details[key].split(';')
                filename = Path(key).resolve().stem
                retries = self.retries_on_error

                while retries:
                    retries -= 1
                    try:
                        de = DetailsExec(self.dbms, connection, filename, sql_split, self.check_on_date, self.branch,
                                         self.log_tk_object, self.file_format, self.file_header, self.file_timestamp, self.file_encoding)
                        de.execute()
                        break

                    except Exception as ex:
                        write_textbox_log(self.log_tk_object, msg=f'Возникло исключение {str(ex).strip()}.', sqlfile=filename)
                        q_err = handle_query_err(ex, filename)

                        if q_err == 1 and retries > 0: # Если ошибка обрабатываемая и не требует переподключения.
                            write_textbox_log(self.log_tk_object, msg=f'Попробую повторно выполнить запрос через 15 секунд.', sqlfile=filename)
                            time.sleep(15)
                        elif q_err == 0: # Необрабатываемая ошибка.
                            write_textbox_log(self.log_tk_object, msg=f'Перехожу к следующему файлу с детализациями.', sqlfile=filename)
                            break
                        elif q_err == 3: # Ошибка обрабатываемая и требует переподключения.
                            write_textbox_log(self.log_tk_object, msg=f'Попробую повторно подключиться через 15 секунд.', sqlfile=filename)
                            time.sleep(15)
                            connection = self.connect(conn_retries=3)
                        elif q_err == 5: # Ошибка обрабатываемая и требует переподключения с увеличенным интервалом ожидания.
                            write_textbox_log(self.log_tk_object, msg=f'Попробую повторно подключиться через 5 минут.', sqlfile=filename)
                            time.sleep(300)
                            connection = self.connect(conn_retries=3)
                        elif q_err == 4: # Игнорируем ошибку и работаем дальше.
                            break
                        else:
                            write_textbox_log(self.log_tk_object, msg=f'Перехожу к следующему файлу с детализациями.', sqlfile=filename)


    def execute(self, connection):
        """Метод для основного скелета выполнения запросов."""
        # 1. Выполняем запросы для создания временных таблиц.
        self.execute_sql_temp_tables(connection)

        # 2. Выполняем запросы проверок.
        self.execute_sql_checks(connection)

        # 3. Выполняем запросы детализаций.
        self.execute_sql_details(connection)

        write_textbox_log(self.log_tk_object, msg=f'Выполнение запросов из всех файлов завершено.')

        # 4. Закрываем подключение по окончанию работы.
        self.disconnect(connection)