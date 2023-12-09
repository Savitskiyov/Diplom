from dateutil.relativedelta import relativedelta
from datetime import datetime, date
from tkinter import filedialog, messagebox
import subprocess
import traceback
import pandas as pd
import pathlib
import time
import sys
import os


def get_current_datetime():
    """Функция возвращает актуальную дату и время в формате текстовой строки."""
    dttm = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    return dttm


def get_current_short_time():
    """Функция возвращает сокращенную дату и время в формате текстовой строки."""
    dttm = datetime.now().strftime("%d.%m %H:%M:%S")
    return dttm


def show_readme_file():
    """Функция для отображения справки."""
    subprocess.Popen(fr'notepad.exe readme.txt')


def delete_tab(tab):
    """Функция для удаления вкладки tkinter"""
    for item in tab.winfo_children(): # Уничтожаем вкладку
        if str(item) == tab.select():
            item.destroy()
            return


def get_file_path(title, multiple=True, ext=None, initial_path=r'./'):
    """Функция для получения адреса выбранного файла/файлов."""
    if multiple:
        files_path = filedialog.askopenfilenames(title=title, filetypes=ext)
    else:
        files_path = filedialog.askopenfilename(title=title, filetypes=ext)
    if not files_path:
        return [initial_path]
    return files_path


def open_excel(files_path):
    """Функция для загрузки файлов Excel. Возвращает список датафреймов."""
    df_list = []
    if len(files_path) > 0:
        for file in files_path: # Вычитываем каждый файл из списка.
            df = pd.read_excel(file)
            df_list.append(df)
    return df_list


def open_explorer(path):
    """Функция для открытия проводника по указанному пути."""
    subprocess.Popen(fr'explorer "{path}"')


def get_path():
    """Возвращает абсолютный путь текущего файла."""
    path = str(pathlib.Path().resolve())
    return path


def show_askyesnocancel_msgbox(title, msg):
    """Вывод окна askyesnocancel messagebox."""
    reply = messagebox.askyesnocancel(title, msg)
    return reply


def create_time_series(db):
    """Функция для создания временного ряда: с первого дня предыдущего месяца до текущего дня."""
    today, six_months_ago, dttm_format = datetime.today(), date.today() - relativedelta(months=12), '%Y-%m-%d %H:%M:%S'
    time_series = pd.date_range(six_months_ago, today, freq='D').sort_values()

    if db == 'Teradata':
        dttm_format = '%Y-%m-%d %H:%M:%S'
    elif db in ('Oracle', 'PostgreSQL'):
        dttm_format = '%d.%m.%Y %H:%M:%S'

    list_time_series = []
    for el in time_series:
        list_time_series.append("'" + el.strftime(dttm_format) + "'")

    return list_time_series


def replace_sql_variables(dict_variables, dict_sql_queries):
    """Функция для замены переменных в скриптах."""
    # Подставляем переменные в каждый из запросов.
    new_sql_queries = {}
    for k in dict_sql_queries.keys():
        sql = dict_sql_queries[k]
        for key, value in dict_variables.items():
            sql = sql.replace(key, value)
        new_sql_queries[k] = sql
    return new_sql_queries


def choose_sql_files(title, msg, type, dict, ext=None):
    """Функция для загрузки скриптов. Пользователь выбирает файлы.
    Функция возвращает словарь, который содержит список адресов файлов."""
    # Определяем, была ли передана фильтрация по каким-либо форматам.
    if ext is not None:
        files = filedialog.askopenfilenames(title=title, filetypes=ext)
    else:
        files = filedialog.askopenfilenames(title=title)

    # Наполняем словарь: ключ - абсолютный адрес файла, значение - тип проверки.
    if len(files) > 0:
        for file in files:  # Вычитываем каждый файл из списка.
            dict[type].append(file)

    # Удалим дубли из списка и сортируем файлы в списке.
    no_doubles_list = sorted(list(set(dict[type])))
    dict[type] = no_doubles_list
    return dict


def read_sql_files(lst_temp_tables, lst_checks, lst_sql_details):
    """Функция для загрузки скриптов.
    Пользователь выбирает файлы. Названия файлов (ключ) и скрипты (значение) в них наполняют словарь."""

    for stype in (lst_temp_tables, lst_checks, lst_sql_details):
        for file in stype: # Вычитываем каждый файл из списка.
            # Наполняем словарь: ключ - название файла, значение - sql запрос.
            try:
                sql_query = ''
                with open(file, encoding='utf-8') as f_in: # Пытаемся вычитать файл в UTF-8
                        for line in f_in:
                            sql_query += line
            except UnicodeDecodeError:
                sql_query = ''
                with open(file) as f_in: # В случае исключения даем возможность подобрать кодировку самостоятельно.
                        for line in f_in:
                            sql_query += line
            except FileNotFoundError:
                messagebox.showerror(title='Ошибка открытия файла', message=f'Не удалось прочитать файл \n{file}.')
            stype[file] = sql_query

    # Возвращаем словарь с запросами SQL.
    return (lst_temp_tables, lst_checks, lst_sql_details)


def create_result_folders(filename, branch, log_object, dbms):
    """Функция для создания подпапок в папке results."""
    dt = datetime.now().strftime("%Y.%m.%d")
    user = os.getlogin()

    if dbms == 'Teradata':
        db = 'TD'
    elif dbms == 'Oracle':
        db = 'ORA'
    elif dbms == 'PostgreSQL':
        db = 'PG'
    elif dbms == 'Hadoop':
        db = 'HDP'

    directory = f'''./results'''
    if not os.path.exists(directory):
        os.makedirs(directory)
        write_textbox_log(log_object, f'Создал папку {directory}.')

    directory = f'''./results/{db}'''
    if not os.path.exists(directory):
        os.makedirs(directory)
        write_textbox_log(log_object, f'Создал папку {directory}.')

    directory = f'''./results/{db}/{dt}'''
    if not os.path.exists(directory):
        os.makedirs(directory)
        write_textbox_log(log_object, f'Создал папку {directory}.')

    directory = f'''./results/{db}/{dt}/{filename}'''
    if not os.path.exists(directory):
        os.makedirs(directory)
        write_textbox_log(log_object, f'Создал папку {directory}.')

    directory = f'''./results/{db}/{dt}/{filename}/{user}'''
    if not os.path.exists(directory):
        os.makedirs(directory)
        write_textbox_log(log_object, f'Создал папку {directory}.')

    if branch != 'None':
        branch = branch.replace("'", '')
        directory = f'''./results/{db}/{dt}/{filename}/{user}/{branch}'''
        if not os.path.exists(directory):
            os.makedirs(directory)
            write_textbox_log(log_object, f'Создал папку {directory}.')

    return directory


def disable_tk_widgets(frame, widget_state='normal'):
    """Функция для отключения виджетов."""
    for w in frame.winfo_children():
        if w.widgetName.lower() in ['entry', 'tk_optionmenu', 'listbox', 'checkbutton']:
            w.config(state=widget_state)


def use_countdown(countdown_dttm, log_object):
    """Функция для отсрочки запуска проверок."""
    datetime_target, datetime_current = datetime.strptime(countdown_dttm, "%d.%m.%Y %H:%M:%S"), datetime.now()
    num_of_secs = (datetime_target - datetime_current).total_seconds()
    if num_of_secs > 0:
        interval = num_of_secs / 10
        msg = f' Выбрана отсрочка, запуск в {countdown_dttm}. Начинаю обратный отсчет каждые 1/10 времени.'
        write_textbox_log(log_object, msg)
        while num_of_secs >= 1:
            left_dttm = time.strftime("%H:%M:%S", time.gmtime(num_of_secs))
            write_textbox_log(log_object, f'До запуска осталось {left_dttm}.')
            num_of_secs -= interval
            time.sleep(interval)
        write_textbox_log(log_object, f'''Отсчет закончен.''')


def save_df_to_file(df, folder, filename, branch, check_on_date, file_format='xlsx', file_header=True, file_timestamp=True, file_encoding='unicode'):
    """Функция для выгрузки данных из датафрейма в текстовый файл."""
    """Удаляет датафрейм после выгрузки данных в текстовый файл."""
    dttm = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    lines = len(df.index)

    if branch == 'None':
        branch_ = ''
    else:
        branch_ = f'''_{branch.replace("'", '')}'''

    if file_header == 'да':
        file_header = True
    elif file_header == 'нет':
        file_header = False

    if file_timestamp == 'да':
        filename = f'{folder}/{filename}{branch_}_{check_on_date}_{dttm}_{lines}'
    elif file_timestamp == 'нет':
        filename = f'{folder}/{filename}{branch_}_{check_on_date}_{lines}'

    if file_format == 'xlsx':
        df.to_excel(f'{filename}.xlsx', encoding=file_encoding, engine='xlsxwriter', index=False, header=file_header)
        del df

    elif file_format == 'txt':
        df.to_csv(f'{filename}.txt', encoding=file_encoding, index=False, header=file_header, sep='\t', mode='a')
        del df

    elif file_format == 'csv':
        df.to_csv(f'{filename}.csv', encoding=file_encoding, index=False, header=file_header, sep=';', mode='a')
        del df

    elif file_format == 'json':
        df.to_json(f'{filename}.json', orient='split')
        del df

    elif file_format == 'xml':
        df.to_xml(f'{filename}.xml')
        del df


def write_textbox_log(log_object, msg, sqlfile=''):
    """Функция для записи сообщения в лог - текстовый объект Textbox Tkinter."""
    try:
        dttm = get_current_datetime()
        string = dttm + '\t' + sqlfile + '\t'+ msg + '\n'
        log_object.insert('end', str(string))
    except Exception as ex:
        handle_query_err(ex, sqlfile)


def save_text_to_file(txt_log, txt_entry, type):
    """Функция для сохранения текстового значения из лога в файл. Принимает объект - текстовое поле."""
    dttm = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")

    if type == 'log':
        file = str(pathlib.Path().resolve()) + f'\\logs\\{os.getlogin()}_{dttm}.csv'
        write_textbox_log(txt_log, f'Сохраняю текстовый лог в файл {file}.')
        txt_value = txt_log.get('1.0', 'end')
        with open(file, 'w') as f:
            f.write(txt_value)

    if type == 'scripts':
        file = str(pathlib.Path().resolve()) + f'\\templates\\{os.getlogin()}_{dttm}.txt'
        write_textbox_log(txt_log, f'Сохраняю набор скриптов в файл {file}.')
        txt_value = txt_entry.get('1.0', 'end')
        with open(file, 'w') as f:
            f.write(txt_value)


def load_file_to_entry(txt_entry):
    """Функция для загрузки сохраненного набора скриптов в текстовый виджет."""

    file = filedialog.askopenfilename(title='Выберите файл *.txt с набором скриптов для запуска.',
                                      filetypes=[("Text file", ".txt")],
                                      initialdir='./templates/')
    if file:
        try:
            text_content = ''
            with open(file, encoding='utf-8') as f:  # Пытаемся вычитать файл в UTF-8
                for line in f:
                    text_content += line
        except UnicodeDecodeError:
            text_content = ''
            with open(file) as f:  # В случае исключения даем возможность подобрать кодировку самостоятельно.
                for line in f:
                    text_content += line
        except FileNotFoundError:
            messagebox.showerror(title='Ошибка открытия файла', message=f'Не удалось прочитать файл \n{file}.')

        txt_entry.delete(1.0, 'end')  # Чистим виджет перед загрузкой в него данных
        txt_entry.insert('end', text_content.strip() + '\n')


def listbox_fill(config, lst_branch, lst_dttm, lst_randvar1, lst_randvar2, lst_randvar3, txt_scripts):
    """Функция для очистки/перезаполнения списков listbox и Text."""

    # Чистим Listbox
    for l in (lst_dttm, lst_randvar1, lst_randvar2, lst_randvar3, lst_branch):
        l.delete(0, 'end')

    txt_scripts.delete(1.0, 'end') # Чистим виджет, который содержит скрипты для запуска.

    # Перезаполняем Listbox временным рядом
    for el in create_time_series(db=config['dbms']): # Перезаполняем Listbox временным рядом
        lst_dttm.insert(0, el)

    # Перезаполняем Listbox значениями из конфига
    vars_list = [[lst_randvar1, config['var4']], [lst_randvar2, config['var5']], [lst_randvar3, config['var6']], [lst_branch, config['branches']]]
    for el in vars_list:
        for l in el[1]:
            el[0].insert(0, l)


def get_entries_values(ent_login, ent_password, var_host, var_db, var_port, var_service_name, var_charset,
                       var_retries_on_error, ent_countdown, var_file_format, var_file_header, var_timestamp, var_file_encoding):
    """Функция для получения значений полей ввода ENTRY."""

    # Получаем параметры из полей ввода.
    username, password, host, database = ent_login.get(), ent_password.get(), var_host.get(), var_db.get()
    port, service_name = var_port.get(), var_service_name.get()
    charset, retries_on_error, countdown_dttm  = var_charset.get(), int(var_retries_on_error.get()), ent_countdown.get()
    file_format, file_header, file_encoding = var_file_format.get(), var_file_header.get(), var_file_encoding.get()
    file_timestamp = var_timestamp.get()

    dict_entries_values = {}
    dict_entries_values = {'username': username, 'password': password, 'host': host,  'database': database,
                           'port': port, 'service_name': service_name, 'charset': charset,
                           'retries_on_error': retries_on_error, 'countdown_dttm': countdown_dttm,
                           'file_format': file_format, 'file_header': file_header,
                           'file_timestamp': file_timestamp, 'file_encoding': file_encoding}
    return dict_entries_values


def get_list_values(var_lst_dttm_on_off, lst_dttm, var_lst_branch_on_off, lst_branch,
                    var_lst_randvar1_on_off, lst_randvar1_val, var_lst_randvar2_on_off, lst_randvar2_val,
                    var_lst_randvar3_on_off, lst_randvar3_val):
    """Функция для получения списков значений."""

    dttm_list, branch_list, randvar1_list, randvar2_list, randvar3_list = {}, {}, {}, {}, {}

    # Наполняем списки выбранных/указанных пользователем переменных.
    if var_lst_dttm_on_off.get():
        dttm_list = [lst_dttm.get(i) for i in lst_dttm.curselection()]
    else:
        dttm_list = ["'5999-12-31 00:00:00'"]

    if var_lst_branch_on_off.get():
        branch_list = [lst_branch.get(i) for i in lst_branch.curselection()]
    else:
        branch_list = ['None']

    if var_lst_randvar1_on_off.get():
        randvar1_list = [lst_randvar1_val.get(i) for i in lst_randvar1_val.curselection()]
    else:
        randvar1_list = ['None']

    if var_lst_randvar2_on_off.get():
        randvar2_list = [lst_randvar2_val.get(i) for i in lst_randvar2_val.curselection()]
    else:
        randvar2_list = ['None']

    if var_lst_randvar3_on_off.get():
        randvar3_list = [lst_randvar3_val.get(i) for i in lst_randvar3_val.curselection()]
    else:
        randvar3_list = ['None']

    for list_ in [branch_list, randvar1_list, randvar2_list, randvar3_list]:
        list_[:] = [i for i in list_ if len(i) >= 3]  # Чистим списки от полей Text (от коротких строк).

    dict_entries_values = {}
    dict_entries_values = {'dttm_list': dttm_list, 'branch_list': branch_list, 'randvar1_list': randvar1_list,
                           'randvar2_list': randvar2_list, 'randvar3_list': randvar3_list}
    return dict_entries_values


def checkout_variables(username, password, host, service_name, dttm_list, branch_list, randvar1_list, randvar2_list,
                       randvar3_list):
    """Функция для проверки всех полей ввода пользователя перед подключением."""
    # Если не выбран ни один тип скриптов.
    if (len(dttm_list) == 0 or len(branch_list) == 0 or len(randvar1_list) == 0 or len(randvar2_list) == 0 or len(randvar3_list) == 0):
        return  'Не выбраны значения одной из переменных!!'
    elif username == '':
        return 'Не введен параметр Login!'
    elif password == '':
        return  'Не введен параметр Password!'
    elif host == '':
        return 'Не введен параметр Host!'
    elif service_name == '':
        return 'Не введен параметр Service_name'
    else:
        return 'OK'


def handle_query_err(exception, filename=''):
    '''
        Функция парсит текст ошибки, который возвращает БД.
        1 - продолжить попытки выполнения
        0 - прекратить выполнение скриптов из текущего файла
        3 - установить повторное подключение и продолжить попытки выполнения.
        4 - продолжить работу
        5 - установить повторное подключение с ожиданием в 5 минут и продолжить попытки выполнения.
    '''

    write_error_log(exception, filename) # Пишем ошибку в файл ./logs/errors/

    if type(exception).__name__ in ('TclError', 'RuntimeError'): # Прекращаем выполнение потока с проверками.
        sys.exit()

    msg = traceback.format_exc().lower()
    # 0. писок необрабатываемых исключений, когда нужно перейти к следующему файлу.
    stop_list = ['does not exist',
                 'keyword not found',
                 'invalid session mode',
                 'not all variables bound',
                 'invalid identifier',
                 'not a valid month',
                 'invalid username',
                 'the userid, password or account is invalid',
                 'fatal']

    for m in stop_list:
        if m in msg:
            return 0

    # 3. Список исключений, когда необходимо повторно подключиться.
    reconnect_list = ['was aborted',
                      'internal error',
                      'connection reset by peer',
                      'session is not logged on',
                      'internal error (exception)',
                      'connection already closed',
                      "object has no attribute 'cursor'",
                      "cannot be performed on a closed cursor"]

    for m in reconnect_list:
        if m in msg:
            return 3

    # 4. Список исключений, где идет пропуск выполнения.
    pass_list = ['object is not iterable']

    for m in pass_list:
        if m in msg:
            return 4

    # 5. Список исключений, когда необходимо повторно подключиться, но необходимо увеличить интервал ожидания.
    reconnect_wait_list = ['the database system is starting up',
                           'server closed the connection unexpectedly',
                           'connection refused']

    for m in reconnect_wait_list:
        if m in msg:
            return 5

    # Исключения, при которых необходима остановка выполнения запросов.
    shutdown_list = ['access violation writing']

    for m in shutdown_list:
        if m in msg:
            sys.exit()

    return 1


def write_error_log(exception, sql_filename=''):
    """Функция для создания файла-лога с описанием ошибки."""
    with open(f'./logs/errors/error_log_{datetime.now().strftime("%d.%m.%Y")}.csv', 'a+') as f:
        user = os.getlogin()
        ex_dttm = get_current_datetime()
        ex_type = type(exception).__name__
        ex_msg = str(exception).replace('\n', ' ')
        ex_file = __file__
        ex_line = 'line:' + str(exception.__traceback__.tb_lineno)
        t = '|'
        line = ex_dttm + t + user + t + sql_filename + t + ex_type + t + ex_msg + t +  ex_file + t + ex_line + t + '\n'
        f.write(line)