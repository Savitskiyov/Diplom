from resources.scripts.functions.rc_functions import write_textbox_log

class TempTablesExec:
    """Класс для запуска временных таблиц."""

    def __init__(self, dbms, conn, sql_split, log_tk_object, filename):
        self.dbms, self.conn, self.sql_split,  = dbms, conn, sql_split
        self.log_tk_object, self.filename = log_tk_object, filename

    def sql_td_execute_temp_tables(self):
        """Функция для создания временных таблиц - должны быть выполнены вместе."""
        write_textbox_log(self.log_tk_object, 'Начинаю выполнение скриптов временных таблиц из файла.', self.filename)
        i = 1
        for s in self.sql_split:
            if 0 < len(s) <= 5:
                write_textbox_log(self.log_tk_object, f'Запрос содержит 5 символов или менее, пропускаю.', self.filename)
            elif len(s) > 5:
                write_textbox_log(self.log_tk_object, f'Выполняю запрос {i}.', self.filename)
                self.conn.execute(s)
                i += 1
                write_textbox_log(self.log_tk_object, f'Выполнение запроса {i} завершено.', self.filename)


    def sql_orcl_pgsql_hadoop_execute_temp_tables(self):
        """Функция для создания временных таблиц - должны быть выполнены вместе."""
        write_textbox_log(self.log_tk_object, 'Начинаю выполнение скриптов временных таблиц из файла.', self.filename)
        i = 0
        for s in self.sql_split:
            i += 1
            if 0 < len(s) <= 5:
                write_textbox_log(self.log_tk_object, f'Запрос {i} содержит 5 символов или менее, пропускаю.', self.filename)
            elif len(s) > 5:
                write_textbox_log(self.log_tk_object, f'Выполняю запрос {i}.', self.filename)
                cur = self.conn.cursor()
                cur.execute(s)
                cur.close()
                write_textbox_log(self.log_tk_object, f'Выполнение запроса {i} завершено.', self.filename)


    def execute_temp_tables(self):
        """Функция для запуска выполнения временных таблиц - в зависимости от БД."""
        if self.dbms == 'Teradata':
            self.sql_td_execute_temp_tables()
        elif self.dbms in ('Oracle', 'PostgreSQL', 'Hadoop'):
            self.sql_orcl_pgsql_hadoop_execute_temp_tables()