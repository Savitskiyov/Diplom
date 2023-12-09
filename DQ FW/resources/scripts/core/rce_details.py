import pandas as pd

from resources.scripts.functions.rc_functions import write_textbox_log, create_result_folders, save_df_to_file

class DetailsExec:
    """Класс для запуска временных таблиц."""

    def __init__(self, dbms, conn, filename, sql_split, check_on_date, branch, log_tk_object,
                 file_format, file_header, file_timestamp, file_encoding):
        self.dbms, self.conn, self.sql_split, self.check_on_date, self.branch = dbms, conn, sql_split, check_on_date, branch
        self.log_tk_object, self.filename, self.file_format = log_tk_object, filename, file_format
        self.file_timestamp, self.file_encoding, self.file_header = file_timestamp, file_encoding, file_header

    def execute(self):
        """Функция для выполнения SQL запросов - выполняются по одному."""
        # Запросы для создания временных таблиц и для проверок просто запускаем. Детализацию выгружаем.
        write_textbox_log(self.log_tk_object, 'Начинаю выполнение скриптов детализаций из файла.', self.filename)
        i = 0
        for s in self.sql_split:
            i += 1
            if 0 < len(s) <= 5:
                write_textbox_log(self.log_tk_object, f'Запрос {i} содержит 5 символов или менее, пропускаю.', self.filename)
            elif len(s) > 5:
                write_textbox_log(self.log_tk_object, msg=f'Выполняю выгрузку детализации {i}.', sqlfile=self.filename)
                folder = create_result_folders(self.filename, self.branch, self.log_tk_object, self.dbms)
                df = pd.read_sql(s, self.conn)
                save_df_to_file(df, folder, self.filename, self.branch, self.check_on_date, self.file_format, self.file_header, self.file_timestamp, self.file_encoding)
                write_textbox_log(self.log_tk_object, msg=f'Выполнение запроса {i} завершено.', sqlfile=self.filename)

        write_textbox_log(self.log_tk_object, msg=f'Выгрузка детализаций из файла завершена.', sqlfile=self.filename)