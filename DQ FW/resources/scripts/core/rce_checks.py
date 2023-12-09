import pandas as pd

from resources.scripts.functions.rc_functions import write_textbox_log, create_result_folders, save_df_to_file

class ChecksExec:
    """Класс для запуска временных таблиц."""

    def __init__(self, dbms, conn, filename, sql_split, check_on_date, branch, details_to_excel, results_to_excel,
                 log_tk_object, file_format, file_header,  file_timestamp, file_encoding):
        self.dbms, self.conn, self.sql_split, self.check_on_date  = dbms, conn, sql_split, check_on_date
        self.branch, self.log_tk_object = branch, log_tk_object
        self.details_to_excel, self.results_to_excel = details_to_excel, results_to_excel
        self.filename, self.file_format, self.file_header,   = filename, file_format, file_header
        self.file_timestamp, self.file_encoding = file_timestamp, file_encoding


    def execute_sql_checks(self):
        """Функция для выполнения SQL запросов - выполняются по одному."""
        write_textbox_log(self.log_tk_object, 'Начинаю выполнение скриптов проверок из файла.', self.filename)
        i = 0
        for s in self.sql_split:
            i += 1
            if 0 < len(s) <= 5:
                write_textbox_log(self.log_tk_object, f'Запрос {i} содержит 5 символов или менее, пропускаю.', self.filename)
            elif len(s) > 5:
                write_textbox_log(self.log_tk_object, msg=f'Выполняю запрос {i}.', sqlfile=self.filename)

                # Если метки не выбраны пользователем; нет метки в скрипте - просто выполняем запрос.
                if 'RESULTS_TO_EXCEL' not in s.upper() and 'DETAILS_TO_EXCEL' not in s.upper():
                    if self.dbms == 'Teradata':
                        self.conn.execute(s)
                    elif self.dbms in ('Oracle', 'PostgreSQL', 'Hadoop'):
                        cur = self.conn.cursor()
                        cur.execute(s)
                        cur.close()
                    write_textbox_log(self.log_tk_object, msg=f'Выполнение запроса {i} завершено.', sqlfile=self.filename)

                # Проверяем опциональную метку RESULTS_TO_EXCEL.
                elif self.results_to_excel is True and 'RESULTS_TO_EXCEL' in s.upper():
                    folder = create_result_folders('results_to_excel', self.branch, self.log_tk_object, self.dbms)
                    df = pd.read_sql(s, self.conn)
                    save_df_to_file(df, folder, self.filename, self.branch, self.check_on_date, self.file_format, self.file_header, self.file_timestamp, self.file_encoding)
                    write_textbox_log(self.log_tk_object, msg=f'Выполнение запроса {i} завершено.', sqlfile=self.filename)

                # Проверяем опциональную метку DETAILS_TO_EXCEL.
                elif self.details_to_excel is True and 'DETAILS_TO_EXCEL' in s.upper():
                    folder = create_result_folders(self.filename, self.branch, self.log_tk_object, self.dbms)
                    df = pd.read_sql(s, self.conn)
                    save_df_to_file(df, folder, self.filename, self.branch, self.check_on_date, self.file_format, self.file_header, self.file_timestamp, self.file_encoding)
                    write_textbox_log(self.log_tk_object, msg=f'Выполнение запроса {i} завершено.', sqlfile=self.filename)

                # Если опция RESULTS_TO_EXCEL не выбрана и метка есть в скрипте - пропускаем скрипт.
                elif self.results_to_excel is False and 'RESULTS_TO_EXCEL' in s.upper():
                    write_textbox_log(self.log_tk_object, f'Пропускаю метку RESULTS_TO_EXCEL (не отмечено).', self.filename)

                # Если опция не выбрана и метка есть в скрипте - пропускаем скрипт.
                elif self.details_to_excel is False and 'DETAILS_TO_EXCEL' in s.upper():
                    write_textbox_log(self.log_tk_object, f'Пропускаю метку DETAILS_TO_EXCEL (не отмечено).', self.filename)

        write_textbox_log(self.log_tk_object, msg=f'Выполнение скриптов из файла завершено.', sqlfile=self.filename)