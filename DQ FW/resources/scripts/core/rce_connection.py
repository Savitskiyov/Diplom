import teradata as td
import cx_Oracle as ora
import psycopg2 as pgl
import jaydebeapi as jdba
from subprocess import Popen, PIPE
import jpype
import glob
import time
import os


from resources.scripts.functions.rc_functions import write_textbox_log, handle_query_err

class ConnectionDB:
    """Класс для создания подключения к БД."""

    def __init__(self, username, password, dbms, host, service_name, port, database, charset, log_tk_object):
        self.username, self.password, self.dbms, self.host = username, password, dbms, host
        self.service_name, self.port, self.database = service_name, port, database
        self.charset, self.log_tk_object = charset, log_tk_object


    def td_connect(self, host, username, password, log_tk_object, charset='UTF8'):
        """Функция для подключения к БД Teradata."""
        driver = "Teradata Database ODBC Driver 16.20"
        odbclibpath = "./distrib/ODBC Teradata"
        udaexec = td.UdaExec(appName='test', version='1.0', logConsole=False, configureLogging=False, odbcLibPath=odbclibpath)
        conn = udaexec.connect(method='odbc', system=host, username=username, password=password, driver=driver, authentication='LDAP', charset=charset, autoCommit=True, queryTimeOut=1200, USEREGIONALSETTINGS='N')
        write_textbox_log(log_tk_object, f'Подключение к Teradata установлено.')
        return conn


    def orcl_connect(self, host, port, service_name, username, password, log_tk_object):
        """Функция для подключения к БД Oracle."""
        dsn_tns = ora.makedsn(host=host, port=port, service_name=service_name )
        conn = ora.connect(user=username, password=password, dsn=dsn_tns)
        conn.autocommit = True
        write_textbox_log(log_tk_object, 'Подключение к Oracle установлено.')
        return conn


    def pgl_connect(self, host, port, database, username, password, log_tk_object):
        """Функция для подключения к БД PostgreSQL."""
        conn = pgl.connect(dbname=database, user=username, password=password, host=host, port=port)
        conn.autocommit = True
        write_textbox_log(log_tk_object, 'Подключение к PostgreSQL установлено.')
        return conn


    def hd_connect(self, host, port, username, password, log_tk_object):
        '''Функция для подключения к БД Hadoop.'''

        kinit_args = [r'C:\JVM\bin\kinit.exe', '{}'.format(username)]
        try:
            write_textbox_log(log_tk_object, 'Запускаю JVM.')
            kinit = Popen(kinit_args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
            kinit.communicate(input='{}\n'.format('{}'.format(password)).encode('utf-8'))
            os.environ['JAVA_HOME'] = r'C:\JVM'
            jar_files = glob.glob(r'C:\JVM\drivers\ImpalaHive\2.5.43\*.jar')
            url = f'jdbc:impala://{host}:{port}/default;AuthMech=1;KrbHostFQDN={host};KrbServiceName=impala;ssl=0'
            write_textbox_log(log_tk_object, 'Создаю подключение.')
            conn = jdba.connect(jclassname=r'com.cloudera.hive.jdbc41.HS2Driver', url=url, jars=jar_files)
            write_textbox_log(log_tk_object, 'Подключение к Hadoop установлено.')
            return conn

        except Exception as ex:
            write_textbox_log(self.log_tk_object, msg=f'Возникло исключение {str(ex).strip()}.')


    def connect(self, conn_retries=3):
        """Метод для подключения."""
        write_textbox_log(self.log_tk_object, msg=f'Устаналиваю подключение к БД.')
        while conn_retries:
            conn_retries -= 1
            try:
                if self.dbms == 'Teradata':
                    conn = self.td_connect(self.host, self.username, self.password, self.log_tk_object, self.charset)
                elif self.dbms == 'Oracle':
                    conn = self.orcl_connect(self.host, self.port, self.service_name, self.username, self.password, self.log_tk_object)
                elif self.dbms == 'PostgreSQL':
                    conn = self.pgl_connect(self.host, self.port, self.database, self.username, self.password, self.log_tk_object)
                elif self.dbms == 'Hadoop':
                    conn = self.hd_connect(self.host, self.port, self.username, self.password, self.log_tk_object)
                return conn

            except Exception as ex:
                write_textbox_log(self.log_tk_object, msg=f'Возникло исключение {str(ex).strip()}.')
                q_err = handle_query_err(ex)

                if q_err == 0:
                    write_textbox_log(self.log_tk_object, msg=f'Прекращаю попытки подключения.')
                    return None
                elif q_err == 3 and conn_retries > 0:
                    write_textbox_log(self.log_tk_object, msg=f'Попробую повторно подключиться через 15 секунд.')
                    time.sleep(15)
                elif q_err == 5 and conn_retries > 0:  # Ошибка обрабатываемая и требует переподключения с увеличенным интервалом ожидания.
                    write_textbox_log(self.log_tk_object, msg=f'Попробую повторно подключиться через 5 минут.')
                    time.sleep(300)