from tkinter import Label, Entry, Frame, messagebox, Checkbutton, Button
from tkinter import BooleanVar, Listbox, Scrollbar, OptionMenu, StringVar, Text
from itertools import product
from threading import Thread
from datetime import datetime
import json
import os

from resources.scripts.core.rce_engine import ChecksEngine
from resources.scripts.functions.rc_functions import use_countdown, choose_sql_files, read_sql_files
from resources.scripts.functions.rc_functions import replace_sql_variables, write_textbox_log, disable_tk_widgets
from resources.scripts.functions.rc_functions import save_text_to_file, load_file_to_entry
from resources.scripts.functions.rc_functions import listbox_fill, get_entries_values
from resources.scripts.functions.rc_functions import get_list_values, checkout_variables
from resources.scripts.functions.rc_functions import get_current_datetime, delete_tab
from resources.scripts.functions.rc_functions import show_askyesnocancel_msgbox, get_current_short_time
from resources.scripts.config.rc_config import Configuration


def create_check_page(notebook, window_title, bg_color, username):
    """Функция для построения вкладки для выбора и запуска проверок."""
    frame = Frame(notebook, bg=bg_color)
    frame.grid()

    config = Configuration()
    tab_names = [notebook.tab(i, option='text') for i in notebook.tabs()] # Список имен вкладок до добавления новой.
    notebook.add(frame, text= window_title + ' ' + get_current_short_time())

    notebook.select(notebook.index(len(tab_names))) # Выбираем
    CheckPage(frame, notebook, bg_color, username, config.get(window_title))


class CheckPage:
    """Класс для создания вкладки с запуском проверок."""

    def __init__(self, frame, notebook, bg_color, username, config):
        self.root, self.notebook, self.config = frame, notebook, config

        def choose_scripts(txt_scripts, sql_type):
            """Функция для обработки события - нажатие кнопки "Выбрать файлы".
            Заполняет текстовый виджет на вкладке со списком проверок. Из этого виджета файлы поступают в обработку."""

            ### Словари с запросами. Ключ - полный адрес файла. Значение - SQL запрос в нем.
            dict_sql_queries, dict_sql_queries_ = {}, {sql_type:[]}

            txt_sql_type = ''
            if sql_type == 'temp':
                txt_sql_type = 'Скрипты временных таблиц'
            elif sql_type == 'checks':
                txt_sql_type = 'Скрипты проверок'
            elif sql_type == 'details':
                txt_sql_type = 'Скрипты детализаций'

            ##### Позволяем пользователю выбрать скрипты.
            try:
                while 1:
                    title = f'{txt_sql_type}. Выберите *.sql файлы.'
                    msg = f'{txt_sql_type}. Выбор файлов.'
                    dict_sql_queries_ = choose_sql_files(title=title, msg=msg, type=sql_type, dict=dict_sql_queries_)
                    dict_sql_queries.update(dict_sql_queries_)
                    title = f'{txt_sql_type}. Желаете выбрать дополнительные файлы?'
                    reply = show_askyesnocancel_msgbox(msg, title)
                    if reply is None:
                        raise StopIteration
                    elif not reply:
                        break
            except StopIteration:
                return None

            # Заполняем текстовый виджет.
            for key, value in dict_sql_queries.items():
                for file in value:
                    txt_scripts.insert('end', key + ';' + file + '\n')


        def btn_execute_press():
            """Функция для обработки события - нажатие кнопки "Выполнить"."""

            ##### Получаем значение полей ввода - там, где пользователь выбирает только 1 вариант.
            dict_entries_values = get_entries_values(ent_login, ent_password, var_host, var_db, var_port,
                                                     var_service_name,var_charset, var_retries_on_error,ent_countdown,
                                                     var_file_format, var_file_header, var_timestamp, var_file_encoding)

            ##### Получаем списки значений полей ввода - там, где пользователь может выбрать несколько вариантов.
            dict_list_values = get_list_values(var_lst_dttm_on_off, lst_dttm, var_lst_branch_on_off, lst_branch,
                                               var_lst_randvar1_on_off,  lst_randvar1_val, var_lst_randvar2_on_off,
                                               lst_randvar2_val, var_lst_randvar3_on_off, lst_randvar3_val)

            lst_temp_tables, lst_checks, lst_sql_details = {}, {}, {}

            ##### Извлекаем содержимое текстового виджета со скриптами.
            script_files = txt_scripts.get('1.0', 'end-1c').split('\n')
            for script_file in script_files:
                if len(script_file) > 3:
                    scr = script_file.split(';')
                    if scr[0] == 'temp':
                        lst_temp_tables[scr[1]] = ''
                    elif scr[0] == 'checks':
                        lst_checks[scr[1]] = ''
                    elif scr[0] == 'details':
                        lst_sql_details[scr[1]] = ''


            var_sql = read_sql_files(lst_temp_tables, lst_checks, lst_sql_details)
            dict_temp_table_sql_queries, dict_sql_checks_queries, dict_sql_details = var_sql[0], var_sql[1], var_sql[2]

            # Отдаем объекты на проверку. Если проходят, то работаем дальше.
            res = checkout_variables(dict_entries_values['username'],
                                     dict_entries_values['password'],
                                     dict_entries_values['host'],
                                     dict_entries_values['service_name'],
                                     dict_list_values['dttm_list'],
                                     dict_list_values['branch_list'],
                                     dict_list_values['randvar1_list'],
                                     dict_list_values['randvar2_list'],
                                     dict_list_values['randvar3_list']
                                     )
            if res != 'OK':
                messagebox.showerror(title='Ошибка', message=res)
            else:
                ln = 0
                for d in (dict_temp_table_sql_queries, dict_sql_checks_queries, dict_sql_details):
                    if d is not None:
                        ln += len(d) # Считаем общее число скриптов.

                reply = messagebox.askquestion('Запуск скриптов', f'Всего выбрано {ln} файлов со скриптами. Выполнить?')
                if reply == 'yes': # Ждем подтверждения от пользователя.
                    use_countdown(dict_entries_values['countdown_dttm'], txt_log) # Отсрочка старта

                    # Идем циклом по временному ряду, если даты начала и окончания откличаются.
                    for i,  k, d, t, s in product(dict_list_values['dttm_list'],
                                                  dict_list_values['branch_list'],
                                                  dict_list_values['randvar1_list'],
                                                  dict_list_values['randvar2_list'],
                                                  dict_list_values['randvar3_list']):
                        write_textbox_log(txt_log, msg=f'Всего {ln} файлов, начинаю работу на дату: {i[1:11]}, ' + \
                            f'{ent_branch_name.get()}: {k}, ' + \
                            f'{ent_randvar1_name.get()}: {d}, ' + \
                            f'{ent_randvar2_name.get()}: {t}, ' + \
                            f'{ent_randvar3_name.get()}: {s}.')

                        # Словарь со всеми значениями переменных.
                        dict_variables = {ent_dttm_name.get(): i,
                                          ent_dttm_name.get()[:-2]: i[:11] + "'",
                                          ent_branch_name.get(): k,
                                          ent_randvar1_name.get(): d,
                                          ent_randvar2_name.get(): t,
                                          ent_randvar3_name.get(): s}

                        # Подставляем в скрипты переменные.
                        new_dict_temp_table_sql_queries= replace_sql_variables(dict_variables=dict_variables, dict_sql_queries=dict_temp_table_sql_queries)
                        new_dict_sql_checks_queries = replace_sql_variables(dict_variables=dict_variables, dict_sql_queries=dict_sql_checks_queries)
                        new_dict_sql_details = replace_sql_variables(dict_variables=dict_variables, dict_sql_queries=dict_sql_details)
                        check_on_date = i[1:11]

                        # Блокируем виджеты.
                        disable_tk_widgets(frame, widget_state='disabled')
                        btn_execute.config(state='disabled')
                        btn_reset.config(state='disabled')

                        ce = ChecksEngine(username=dict_entries_values['username'],
                                          password=dict_entries_values['password'],
                                          dbms=config['dbms'],
                                          host=dict_entries_values['host'],
                                          service_name=dict_entries_values['service_name'],
                                          port=dict_entries_values['port'],
                                          database=dict_entries_values['database'],
                                          charset=dict_entries_values['charset'],
                                          check_on_date=check_on_date,
                                          branch=k,
                                          retries_on_error=dict_entries_values['retries_on_error'],
                                          details_to_excel=var_sql_details_to_excel.get(),
                                          results_to_excel=var_sql_results_to_excel.get(),
                                          log_tk_object=txt_log,
                                          dict_temp_table_sql_queries=new_dict_temp_table_sql_queries,
                                          dict_sql_checks_queries=new_dict_sql_checks_queries,
                                          dict_sql_details=new_dict_sql_details,
                                          file_format=dict_entries_values['file_format'],
                                          file_header=dict_entries_values['file_header'],
                                          file_timestamp=dict_entries_values['file_timestamp'],
                                          file_encoding=dict_entries_values['file_encoding']
                                          )

                        connection = ce.connect()

                        ###### Кнопка "Закрыть подключение".
                        #btn_close_conn = Button(frame, text='Закрыть подключение', bg=bg_color, command=lambda: Thread(target=ce.disconnect(connection)).start())
                        #btn_close_conn.grid(row=30, column=0, stick='we')

                        if ce.connection_flag == 1 and connection:
                            ce.execute(connection)
                        else:
                           break

                    # Разблокируем виджеты.
                    disable_tk_widgets(frame, widget_state='normal')
                    btn_execute.config(state='normal')
                    btn_reset.config(state='normal')
                    #btn_close_conn.destroy()


        variable_names = config['variable_names'] # Получаем список названий используемых переменных

        ##### Реквизиты пользователя.
        Label(frame, text='Параметры подключения', bg='#eddcd2', fg='black').grid(row=0, column=0, sticky='WE')

        # Отступ 3 столбец.
        Label(frame, text=' ', bg=bg_color, fg='grey').grid(row=41, column=2, sticky='WE')

        # Поле для ввода логина пользователя.
        Label(frame, text='Login', bg=bg_color, fg='black').grid(row=1, column=0, sticky='W')
        ent_login = Entry(frame)
        ent_login.insert(0, username)
        ent_login.grid(row=1, column=1, sticky='WE')

        # Поле для ввода пароля пользователя.
        Label(frame, text='Password', bg=bg_color, fg='black').grid(row=2, column=0, sticky='W')
        ent_password = Entry(frame, show='*')
        ent_password.grid(row=2, column=1, sticky='WE')

        # Поле для ввода номера порта
        Label(frame, text='Port', bg=bg_color, fg='black').grid(row=3, column=0, sticky='W')
        var_port = StringVar(frame)
        var_port.set(config['port'][0])
        ent_port = OptionMenu(frame, var_port, *config['port'])
        ent_port.configure(anchor='w', bg=bg_color, highlightbackground=bg_color)
        ent_port.grid(row=3, column=1, sticky='WE')

        # Набор символов
        Label(frame, text='Charset', bg=bg_color, fg='black').grid(row=4, column=0, sticky='W')
        var_charset = StringVar(frame)
        var_charset.set(config['charset'][0])
        ent_charset = OptionMenu(frame, var_charset, *config['charset'])
        ent_charset.configure(anchor='w', bg=bg_color, highlightbackground=bg_color)
        ent_charset.grid(row=4, column=1, sticky='WE')

        # Поле для ввода названия хоста
        Label(frame, text='Host', bg=bg_color, fg='black').grid(row=5, column=0, sticky='W')
        var_host = StringVar(frame)
        var_host.set(config['host'][0])
        ent_host = OptionMenu(frame, var_host, *config['host'])
        ent_host.configure(anchor='w', bg=bg_color, highlightbackground=bg_color)
        ent_host.grid(row=5, column=1, sticky='WE')

        # Поле для ввода названия service_name
        var_service_name = StringVar(frame)
        var_service_name.set(config['service_name'][0])
        if config['dbms'] == 'Oracle':
            lbl_service_name = Label(frame, text='Service name', bg=bg_color, fg='black')
            lbl_service_name.grid(row=6, column=0, sticky='W')
            ent_service_name = OptionMenu(frame, var_service_name, *config['service_name'])
            ent_service_name.configure(anchor='w', bg=bg_color, highlightbackground=bg_color)
            ent_service_name.grid(row=6, column=1, sticky='WE')
        else:
            lbl_service_name = Label(frame, text='', bg=bg_color)
            lbl_service_name.grid(row=6, column=0, sticky='W')

        # Поле для ввода названия базы данных
        var_db = StringVar(frame)
        var_db.set(config['database'][0])
        if config['dbms'] == 'PostgreSQL':
            lbl_db = Label(frame, text='Database', bg=bg_color, fg='black')
            lbl_db.grid(row=6, column=0, sticky='W')
            ent_db = OptionMenu(frame, var_db, *config['database'])
            ent_db.configure(anchor='w', bg=bg_color, highlightbackground=bg_color)
            ent_db.grid(row=6, column=1, sticky='WE')


        ##### Переменные
        Label(frame, text='Переменные подстановки', bg='#ddbea9', fg='black').grid(row=0, column=3, columnspan=5, sticky='WE')

        ### Список ?DTTM
        ent_dttm_name = Entry(frame, width=15)
        ent_dttm_name.insert(0, f'''{variable_names["var1"]}''')
        ent_dttm_name.grid(row=1, column=4, sticky='WE')

        # Скроллбар DTTM вертикальный
        scrl_y_dttm = Scrollbar(frame, bg=bg_color, orient='vertical')
        scrl_y_dttm.grid(row=2, column=5, rowspan=7, sticky='NS')
        # Скроллбар DTTM горизонтальный
        scrl_x_dttm  = Scrollbar(frame, bg=bg_color, orient='horizontal')
        scrl_x_dttm.grid(row=8, column=3, columnspan=2, sticky='WE')

        lst_dttm = Listbox(frame, selectmode='multiple', exportselection=0, height=8)
        lst_dttm.config(xscrollcommand = scrl_x_dttm.set, yscrollcommand = scrl_y_dttm.set)
        scrl_y_dttm.config(command=lst_dttm.yview)
        scrl_x_dttm.config(command=lst_dttm.xview)
        lst_dttm.grid(row=2, column=3, rowspan=6, columnspan=2, sticky='NSWE')

        # Чекбокс для включения/отключения
        var_lst_dttm_on_off = BooleanVar()
        cbx_lst_dttm_on_off = Checkbutton(frame, text='', variable=var_lst_dttm_on_off, bg=bg_color)
        cbx_lst_dttm_on_off.grid(row=1, column=3, sticky='w')
        cbx_lst_dttm_on_off.select()

        # Отступ 6 столбец.
        Label(frame, text=' ', bg=bg_color).grid(row=41, column=6, sticky='WE')

        ### ?BRANCH
        ent_branch_name = Entry(frame, width=15)
        ent_branch_name.insert(0, f'''{variable_names["var3"]}''')
        ent_branch_name.grid(row=1, column=8, sticky='WE')

        # Скроллбар BRANCH вертикальный
        scrl_y_branch = Scrollbar(frame, bg=bg_color, orient='vertical')
        scrl_y_branch.grid(row=2, column=9, rowspan=7, sticky='NS')
        # Скроллбар BRANCH горизонтальный
        scrl_x_branch = Scrollbar(frame, bg=bg_color, orient='horizontal')
        scrl_x_branch.grid(row=8, column=7, columnspan=2, sticky='WE')

        lst_branch = Listbox(frame, selectmode='multiple', exportselection=0, height=8)
        lst_branch.config(xscrollcommand = scrl_x_branch.set, yscrollcommand = scrl_y_branch.set)
        scrl_y_branch.config(command=lst_branch.yview)
        scrl_x_branch.config(command=lst_branch.xview)
        lst_branch.grid(row=2, column=7, rowspan=6, columnspan=2, sticky='NSWE')

        # Чекбокс для включения/отключения
        var_lst_branch_on_off = BooleanVar()
        cbx_lst_branch_on_off = Checkbutton(frame, text='', variable=var_lst_branch_on_off, bg=bg_color)
        cbx_lst_branch_on_off.grid(row=1, column=7, sticky='w')

        # Отступ 10 столбец.
        Label(frame, text=' ', bg=bg_color).grid(row=41, column=10, sticky='WE')

        ### RANDVAR1
        ent_randvar1_name = Entry(frame, width=15)
        ent_randvar1_name.insert(0, f'''{variable_names["var4"]}''')
        ent_randvar1_name.grid(row=1, column=12, sticky='WE')
        # Скроллбар RANDVAR1 вертикальный
        scrl_y_randvar1 = Scrollbar(frame, bg=bg_color, orient='vertical')
        scrl_y_randvar1.grid(row=2, column=13, rowspan=7, sticky='NS')
        # Скроллбар RANDVAR1 горизонтальный
        scrl_x_randvar1 = Scrollbar(frame, bg=bg_color, orient='horizontal')
        scrl_x_randvar1.grid(row=8, column=11, columnspan=2, sticky='WE')
        lst_randvar1_val = Listbox(frame, selectmode='multiple', exportselection=0, height=8)
        lst_randvar1_val.config(xscrollcommand = scrl_x_randvar1.set, yscrollcommand = scrl_y_randvar1.set)
        scrl_y_randvar1.config(command=lst_randvar1_val.yview)
        scrl_x_randvar1.config(command=lst_randvar1_val.xview)
        lst_randvar1_val.grid(row=2, column=11, sticky='NSWE',rowspan=6, columnspan=2)
        # Чекбокс для включения/отключения
        var_lst_randvar1_on_off = BooleanVar()
        cbx_lst_randvar1_on_off = Checkbutton(frame, text='', variable=var_lst_randvar1_on_off, bg=bg_color)
        cbx_lst_randvar1_on_off.grid(row=1, column=11, sticky='w')

        # Отступ 14 столбец.
        Label(frame, text=' ', bg=bg_color).grid(row=41, column=14, sticky='WE')

        ### RANDVAR2
        ent_randvar2_name = Entry(frame, width=15)
        ent_randvar2_name.insert(0, f'''{variable_names["var5"]}''')
        ent_randvar2_name.grid(row=1, column=16, sticky='WE')
        # Скроллбар RANDVAR2 вертикальный
        scrl_y_randvar2 = Scrollbar(frame, bg=bg_color, orient='vertical')
        scrl_y_randvar2.grid(row=2, column=17, rowspan=7, sticky='NS')
        # Скроллбар RANDVAR2 горизонтальный
        scrl_x_randvar2 = Scrollbar(frame, bg=bg_color, orient='horizontal')
        scrl_x_randvar2.grid(row=8, column=15, columnspan=2, sticky='WE')
        lst_randvar2_val = Listbox(frame, selectmode='multiple', exportselection=0, height=8)
        lst_randvar2_val.config(xscrollcommand=scrl_x_randvar2.set, yscrollcommand=scrl_y_randvar2.set)
        scrl_y_randvar2.config(command=lst_randvar2_val.yview)
        scrl_x_randvar2.config(command=lst_randvar2_val.xview)
        lst_randvar2_val.grid(row=2, column=15, sticky='NSWE',rowspan=6, columnspan=2)
        # Чекбокс для включения/отключения
        var_lst_randvar2_on_off = BooleanVar()
        cbx_lst_randvar2_on_off = Checkbutton(frame, text='', variable=var_lst_randvar2_on_off, bg=bg_color)
        cbx_lst_randvar2_on_off.grid(row=1, column=15, sticky='w')

        # Отступ 18 столбец.
        Label(frame, text=' ', bg=bg_color).grid(row=41, column=18, sticky='WE')

        ### RANDVAR3
        ent_randvar3_name = Entry(frame, width=15)
        ent_randvar3_name.insert(0, f'''{variable_names["var6"]}''')
        ent_randvar3_name.grid(row=1, column=20, sticky='WE')
        # Скроллбар RANDVAR3 вертикальный
        scrl_y_randvar3 = Scrollbar(frame, bg=bg_color, orient='vertical')
        scrl_y_randvar3.grid(row=2, column=21, rowspan=7, sticky='NS')
        # Скроллбар RANDVAR3 горизонтальный
        scrl_x_randvar3 = Scrollbar(frame, bg=bg_color, orient='horizontal')
        scrl_x_randvar3.grid(row=8, column=19, columnspan=2, sticky='WE')
        lst_randvar3_val = Listbox(frame, selectmode='multiple', exportselection=0, height=8)
        lst_randvar3_val.config(xscrollcommand=scrl_x_randvar3.set, yscrollcommand=scrl_y_randvar3.set)
        scrl_y_randvar3.config(command=lst_randvar3_val.yview)
        scrl_x_randvar3.config(command=lst_randvar3_val.xview)
        lst_randvar3_val.grid(row=2, column=19, sticky='NSWE',rowspan=6, columnspan=2)
        # Чекбокс для включения/отключения
        var_lst_randvar3_on_off = BooleanVar()
        cbx_lst_randvar3_on_off = Checkbutton(frame, text='', variable=var_lst_randvar3_on_off, bg=bg_color)
        cbx_lst_randvar3_on_off.grid(row=1, column=19, sticky='w')

        ##### Выбор скриптов
        Label(frame, text='Выбранные файлы со скриптами', bg='#a5a58d', fg='black').grid(row=9, column=3, columnspan=5, sticky='WE')

        ## Окно просмотра скриптов
        scrl_y_scripts = Scrollbar(frame, bg=bg_color, orient='vertical')
        scrl_y_scripts.grid(row=10, column=21, rowspan=6, sticky='NS')
        scrl_x_scripts = Scrollbar(frame, bg=bg_color, orient='horizontal')
        scrl_x_scripts.grid(row=16, column=3, columnspan=18, sticky='WE')
        txt_scripts = Text(frame, wrap='none', bg='white', fg='black', height=8, font=('Calibri', 9), xscrollcommand=scrl_x_scripts.set, yscrollcommand=scrl_y_scripts.set)
        scrl_x_scripts.config(command=txt_scripts.xview)
        scrl_y_scripts.config(command=txt_scripts.yview)
        txt_scripts.grid(row=10, column=3, rowspan=6, columnspan=18, sticky='nswe')

        # Заполняем все listbox!
        listbox_fill(config, lst_branch, lst_dttm, lst_randvar1_val, lst_randvar2_val, lst_randvar3_val, txt_scripts)


        ##### Скрипты
        Label(frame, text='Выбор скриптов *.sql для запуска', bg='#bcd4d7', fg='black').grid(row=9, column=0, sticky='WE')

        ##### Кнопка для выбора скриптов создания временных таблиц.
        Label(frame, text='Приоритет #1', bg=bg_color, fg='black').grid(row=10, column=0, sticky='w')
        Button(frame, text=' Временные таблицы', bg=bg_color, command=lambda: choose_scripts(txt_scripts, 'temp')).grid(row=10, column=1, stick='we')

        ##### Кнопка для выбора скриптов проверок.
        Label(frame, text='Приоритет #2', bg=bg_color, fg='black').grid(row=11, column=0, sticky='w')
        Button(frame, text='Проверки', bg=bg_color, command=lambda: choose_scripts(txt_scripts, 'checks')).grid(row=11, column=1, stick='we')

        ##### Кнопка для выбора скриптов получения детализации (выгружаются в Excel).
        Label(frame, text='Приоритет #3', bg=bg_color, fg='black').grid(row=12, column=0, sticky='w')
        Button(frame, text='Детализация', bg=bg_color, command=lambda: choose_scripts(txt_scripts, 'details')).grid(row=12, column=1, stick='we')

        ##### Шаблоны скриптов
        Label(frame, text='Готовые наборы скриптов', bg='#A7C7E7', fg='black').grid(row=13, column=0, sticky='WE')

        ###### Кнопка "Сохранить шаблон".
        Button(frame, text='Сохранить набор', bg=bg_color, command=lambda: save_text_to_file(txt_log, txt_scripts, 'scripts')).grid(row=14, column=1, stick='we')

        ###### Кнопка "Открыть шаблон".
        Button(frame, text='Открыть набор', bg=bg_color, command=lambda: load_file_to_entry(txt_scripts)).grid(row=15, column=1, stick='we')

        #####  Отсрочка
        Label(frame, text='Отсрочка запуска скриптов', bg='#eadade', fg='black').grid(row=17, column=0, sticky='WE')
        Label(frame, text='Запуск в ДД.ММ.ГГГГ чч:мм:сс:', bg=bg_color, fg='black').grid(row=18, column=0, sticky='WE')
        ent_countdown = Entry(frame)
        ent_countdown.insert(0, get_current_datetime())
        ent_countdown.grid(row=18, column=1, sticky='WE')

        # Количество повторений для обрабатываемой ошибки.
        Label(frame, text='Обработка исключений', bg='#a695b7', fg='black').grid(row=19, column=0, sticky='WE')
        Label(frame, text='Кол-во повторных попыток', bg=bg_color, fg='black').grid(row=20, column=0, sticky='WE')
        var_retries_on_error = StringVar(frame)
        var_retries_on_error.set('3')
        ent_retries_on_error = OptionMenu(frame, var_retries_on_error, *['1', '2', '3', '4', '5'])
        ent_retries_on_error.configure(anchor='w', bg=bg_color, highlightbackground=bg_color)
        ent_retries_on_error.grid(row=20, column=1, sticky='WE')

        ##### Чекбокс для выбора скриптов получения детализации (выгружаются в Excel).
        Label(frame, text='Выгрузка результатов в файл', bg='#eed7a1', fg='black').grid(row=21, column=0, sticky='WE')
        var_sql_details_to_excel = BooleanVar()
        cbx_sql_details_to_excel = Checkbutton(frame, text='Метка DETAILS_TO_EXCEL', variable=var_sql_details_to_excel, bg=bg_color)
        cbx_sql_details_to_excel.grid(row=22, column=0, sticky='w')
        var_sql_results_to_excel = BooleanVar()
        cbx_sql_results_to_excel = Checkbutton(frame, text='Метка RESULTS_TO_EXCEL', variable=var_sql_results_to_excel, bg=bg_color)
        cbx_sql_results_to_excel.grid(row=22, column=1, sticky='w')

        # Поле выбора формата сохранения текстового файла
        Label(frame, text='Формат сохраняемого файла', bg=bg_color, fg='black').grid(row=23, column=0, sticky='W')
        var_file_format = StringVar(frame)
        var_file_format.set(config['file_format'][0])
        ent_file_format = OptionMenu(frame, var_file_format, *config['file_format'])
        ent_file_format.configure(anchor='w', bg=bg_color, highlightbackground=bg_color)
        ent_file_format.grid(row=23, column=1, sticky='WE')

        # Выбор - выгружать ли "шапку"
        Label(frame, text='Выгружать заголовок в файле', bg=bg_color, fg='black').grid(row=24, column=0, sticky='W')
        var_file_header = StringVar(frame)
        var_file_header.set(config['file_header'][0])
        ent_file_header = OptionMenu(frame, var_file_header, *config['file_header'])
        ent_file_header.configure(anchor='w', bg=bg_color, highlightbackground=bg_color)
        ent_file_header.grid(row=24, column=1, sticky='WE')

        # Выбор - набор символов
        Label(frame, text='Набор символов', bg=bg_color, fg='black').grid(row=25, column=0, sticky='W')
        var_file_encoding = StringVar(frame)
        var_file_encoding.set(config['file_encoding'][0])
        ent_file_encoding = OptionMenu(frame, var_file_encoding, *config['file_encoding'])
        ent_file_encoding.configure(anchor='w', bg=bg_color, highlightbackground=bg_color)
        ent_file_encoding.grid(row=25, column=1, sticky='WE')

        # Выбор - указывать ли в названии файла метку времени.
        Label(frame, text='Отметка времени в названии', bg=bg_color, fg='black').grid(row=26, column=0, sticky='W')
        var_timestamp = StringVar(frame)
        var_timestamp.set(config['file_timestamp'][0])
        ent_timestamp = OptionMenu(frame, var_timestamp, *config['file_timestamp'])
        ent_timestamp.configure(anchor='w', bg=bg_color, highlightbackground=bg_color)
        ent_timestamp.grid(row=26, column=1, sticky='WE')


        ##### Лог выполнения запросов + скроллбары.
        Label(frame, text='Лог выполнения запросов', bg='#b2d6cd', fg='Black').grid(row=17, column=3, columnspan=5, sticky='WE')
        scrl_y_log = Scrollbar(frame, bg=bg_color, orient='vertical')
        scrl_y_log.grid(row=18, column=21, rowspan=10, sticky='NS')
        scrl_x_log = Scrollbar(frame, bg=bg_color, orient='horizontal')
        scrl_x_log.grid(row=28, column=3, columnspan=18, sticky='WE')
        txt_log = Text(frame, wrap='none', bg='white', fg='black', height=8, font=('Calibri', 9), xscrollcommand=scrl_x_log.set, yscrollcommand=scrl_y_log.set)
        scrl_x_log.config(command=txt_log.xview)
        scrl_y_log.config(command=txt_log.yview)
        txt_log.grid(row=18, column=3, rowspan=10, columnspan=18, sticky='nswe')

        ###### Кнопка "Сбросить"
        btn_reset = Button(frame, text='Сбросить', bg=bg_color, command=lambda:listbox_fill(config, lst_branch, lst_dttm, lst_randvar1_val, lst_randvar2_val, lst_randvar3_val, txt_scripts))
        btn_reset.grid(row=17, column=20, stick='we')

        ###### Кнопка "Закрыть"
        Button(frame, text='Закрыть', bg=bg_color, command=lambda: delete_tab(notebook)).grid(row=0, column=20, stick='we')

        Label(frame, text='', bg=bg_color, fg='black').grid(row=30, column=0, sticky='WE')

        ###### Кнопка "Сохранить лог".
        btn_save_log = Button(frame, text='Сохранить лог', bg=bg_color, command=lambda: save_text_to_file(txt_log, txt_scripts, 'log'))
        btn_save_log.grid(row=30, column=20, stick='we')

        ###### Кнопка "Выполнить" - вызываем функцию в новом потоке!
        btn_execute = Button(frame, text='Выполнить', bg=bg_color, command=lambda: Thread(target=btn_execute_press).start())
        btn_execute.grid(row=30, column=1, stick='we')


def create_config_page(notebook, bg_color):
    """Функция для построения вкладки для выбора и запуска проверок."""
    frame = Frame(notebook, bg=bg_color)
    frame.grid()

    tab_names = [notebook.tab(i, option='text') for i in notebook.tabs()]  # Список имен вкладок до добавления новой.
    notebook.add(frame, text='Конфигурация')

    notebook.select(notebook.index(len(tab_names)))  # Выбираем
    ConfigPage(frame, notebook, bg_color)


class ConfigPage:
    """Построение вкладки для внесения параметров через графический интерфейс."""

    def __init__(self, frame, notebook, bg_color):
        self.root, self.notebook = frame, notebook

        def read_config():
            """Функция для загрузки актуальной конфигурации в интерфейс программы."""
            config = Configuration()  # Получаем конфигурацию из файла.

            for w in frame.winfo_children():
                if w.widgetName.lower() == 'text':
                    w.delete('1.0', 'end')

            # Заполняем виджеты с настройками.
            for el in config.get('Teradata')['host']: tbx_td_host.insert('end', el + '\n')
            for el in config.get('Teradata')['port']: tbx_td_port.insert('end', el + '\n')
            for el in config.get('Teradata')['database']: tbx_td_database.insert('end', el + '\n')
            for el in config.get('Teradata')['service_name']: tbx_td_service_name.insert('end', el + '\n')
            for el in config.get('Teradata')['charset']: tbx_td_charset.insert('end', el + '\n')
            for el in config.get('Teradata')['branches']: tbx_td_branches.insert('end', el + '\n')
            for el in config.get('Teradata')['var4']: tbx_td_var4.insert('end', el + '\n')
            for el in config.get('Teradata')['var5']: tbx_td_var5.insert('end', el + '\n')
            for el in config.get('Teradata')['var6']: tbx_td_var6.insert('end', el + '\n')

            for el in config.get('Oracle')['host']: tbx_ora_host.insert('end', el + '\n')
            for el in config.get('Oracle')['port']: tbx_ora_port.insert('end', el + '\n')
            for el in config.get('Oracle')['database']: tbx_ora_database.insert('end', el + '\n')
            for el in config.get('Oracle')['service_name']: tbx_ora_service_name.insert('end', el + '\n')
            for el in config.get('Oracle')['charset']: tbx_ora_charset.insert('end', el + '\n')
            for el in config.get('Oracle')['branches']: tbx_ora_branches.insert('end', el + '\n')
            for el in config.get('Oracle')['var4']: tbx_ora_var4.insert('end', el + '\n')
            for el in config.get('Oracle')['var5']: tbx_ora_var5.insert('end', el + '\n')
            for el in config.get('Oracle')['var6']: tbx_ora_var6.insert('end', el + '\n')

            for el in config.get('PostgreSQL')['host']: tbx_pg_host.insert('end', el + '\n')
            for el in config.get('PostgreSQL')['port']: tbx_pg_port.insert('end', el + '\n')
            for el in config.get('PostgreSQL')['database']: tbx_pg_database.insert('end', el + '\n')
            for el in config.get('PostgreSQL')['service_name']: tbx_pg_service_name.insert('end', el + '\n')
            for el in config.get('PostgreSQL')['charset']: tbx_pg_charset.insert('end', el + '\n')
            for el in config.get('PostgreSQL')['branches']: tbx_pg_branches.insert('end', el + '\n')
            for el in config.get('PostgreSQL')['var4']: tbx_pg_var4.insert('end', el + '\n')
            for el in config.get('PostgreSQL')['var5']: tbx_pg_var5.insert('end', el + '\n')
            for el in config.get('PostgreSQL')['var6']: tbx_pg_var6.insert('end', el + '\n')

            for el in config.get('Hadoop')['host']: tbx_hd_host.insert('end', el + '\n')
            for el in config.get('Hadoop')['port']: tbx_hd_port.insert('end', el + '\n')
            for el in config.get('Hadoop')['database']: tbx_hd_database.insert('end', el + '\n')
            for el in config.get('Hadoop')['service_name']: tbx_hd_service_name.insert('end', el + '\n')
            for el in config.get('Hadoop')['charset']: tbx_hd_charset.insert('end', el + '\n')
            for el in config.get('Hadoop')['branches']: tbx_hd_branches.insert('end', el + '\n')
            for el in config.get('Hadoop')['var4']: tbx_hd_var4.insert('end', el + '\n')
            for el in config.get('Hadoop')['var5']: tbx_hd_var5.insert('end', el + '\n')
            for el in config.get('Hadoop')['var6']: tbx_hd_var6.insert('end', el + '\n')


        def save_config():
            """Функция для сохранения настроек конфигурации из интерфейса программы в файл json."""
            dttm = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
            user = os.getlogin()

            # Создаем бэкап текущих настроек.
            with open(r'./resources/scripts/config/config.json', 'r') as jsonfrom, open \
                        (f'./resources/scripts/config/config_{user}_{dttm}.json', 'w') as jsonto:
                data = json.load(jsonfrom)
                json.dump(data, jsonto)

            with open(r'./resources/scripts/config/config.json', 'w') as jsonfile:
                # Teradata
                td_host_data = tbx_td_host.get('1.0', 'end-1c').split('\n')
                data["Teradata"]["host"] = [x for x in td_host_data if x]

                td_port_data = tbx_td_port.get('1.0', 'end-1c').split('\n')
                data["Teradata"]["port"] = [x for x in td_port_data if x]

                td_database_data = tbx_td_database.get('1.0', 'end-1c').split('\n')
                data["Teradata"]["database"] = [x for x in td_database_data if x]

                td_service_name_data = tbx_td_service_name.get('1.0', 'end-1c').split('\n')
                data["Teradata"]["service_name"] = [x for x in td_service_name_data if x]

                td_charset_data = tbx_td_charset.get('1.0', 'end-1c').split('\n')
                data["Teradata"]["charset"] = [x for x in td_charset_data if x]

                td_branches_data = tbx_td_branches.get('1.0', 'end-1c').split('\n')
                data["Teradata"]["branches"] = [x for x in td_branches_data if x]

                td_var4_data = tbx_td_var4.get('1.0', 'end-1c').split('\n')
                data["Teradata"]["var4"] = [x for x in td_var4_data if x]

                td_var5_data = tbx_td_var5.get('1.0', 'end-1c').split('\n')
                data["Teradata"]["var5"] = [x for x in td_var5_data if x]

                td_var6_data = tbx_td_var6.get('1.0', 'end-1c').split('\n')
                data["Teradata"]["var6"] = [x for x in td_var6_data if x]

                # Oracle
                ora_host_data = tbx_ora_host.get('1.0', 'end-1c').split('\n')
                data["Oracle"]["host"] = [x for x in ora_host_data if x]

                ora_port_data = tbx_ora_port.get('1.0', 'end-1c').split('\n')
                data["Oracle"]["port"] = [x for x in ora_port_data if x]

                ora_database_data = tbx_ora_database.get('1.0', 'end-1c').split('\n')
                data["Oracle"]["database"] = [x for x in ora_database_data if x]

                ora_service_name_data = tbx_ora_service_name.get('1.0', 'end-1c').split('\n')
                data["Oracle"]["service_name"] = [x for x in ora_service_name_data if x]

                ora_charset_data = tbx_ora_charset.get('1.0', 'end-1c').split('\n')
                data["Oracle"]["charset"] = [x for x in ora_charset_data if x]

                ora_branches_data = tbx_ora_branches.get('1.0', 'end-1c').split('\n')
                data["Oracle"]["branches"] = [x for x in ora_branches_data if x]

                ora_var4_data = tbx_ora_var4.get('1.0', 'end-1c').split('\n')
                data["Oracle"]["var4"] = [x for x in ora_var4_data if x]

                ora_var5_data = tbx_ora_var5.get('1.0', 'end-1c').split('\n')
                data["Oracle"]["var5"] = [x for x in ora_var5_data if x]

                ora_var6_data = tbx_ora_var6.get('1.0', 'end-1c').split('\n')
                data["Oracle"]["var6"] = [x for x in ora_var6_data if x]

                # PostgreSQL
                pg_host_data = tbx_pg_host.get('1.0', 'end-1c').split('\n')
                data["PostgreSQL"]["host"] = [x for x in pg_host_data if x]

                pg_port_data = tbx_pg_port.get('1.0', 'end-1c').split('\n')
                data["PostgreSQL"]["port"] = [x for x in pg_port_data if x]

                pg_database_data = tbx_pg_database.get('1.0', 'end-1c').split('\n')
                data["PostgreSQL"]["database"] = [x for x in pg_database_data if x]

                pg_service_name_data = tbx_pg_service_name.get('1.0', 'end-1c').split('\n')
                data["PostgreSQL"]["service_name"] = [x for x in pg_service_name_data if x]

                pg_charset_data = tbx_pg_charset.get('1.0', 'end-1c').split('\n')
                data["PostgreSQL"]["charset"] = [x for x in pg_charset_data if x]

                pg_branches_data = tbx_pg_branches.get('1.0', 'end-1c').split('\n')
                data["PostgreSQL"]["branches"] = [x for x in pg_branches_data if x]

                pg_var4_data = tbx_pg_var4.get('1.0', 'end-1c').split('\n')
                data["PostgreSQL"]["var4"] = [x for x in pg_var4_data if x]

                pg_var5_data = tbx_pg_var5.get('1.0', 'end-1c').split('\n')
                data["PostgreSQL"]["var5"] = [x for x in pg_var5_data if x]

                pg_var6_data = tbx_pg_var6.get('1.0', 'end-1c').split('\n')
                data["PostgreSQL"]["var6"] = [x for x in pg_var6_data if x]

                # Hadoop
                hd_host_data = tbx_hd_host.get('1.0', 'end-1c').split('\n')
                data["Hadoop"]["host"] = [x for x in hd_host_data if x]

                hd_port_data = tbx_hd_port.get('1.0', 'end-1c').split('\n')
                data["Hadoop"]["port"] = [x for x in hd_port_data if x]

                hd_database_data = tbx_hd_database.get('1.0', 'end-1c').split('\n')
                data["Hadoop"]["database"] = [x for x in hd_database_data if x]

                hd_service_name_data = tbx_hd_service_name.get('1.0', 'end-1c').split('\n')
                data["Hadoop"]["service_name"] = [x for x in hd_service_name_data if x]

                hd_charset_data = tbx_hd_charset.get('1.0', 'end-1c').split('\n')
                data["Hadoop"]["charset"] = [x for x in hd_charset_data if x]

                hd_branches_data = tbx_hd_branches.get('1.0', 'end-1c').split('\n')
                data["Hadoop"]["branches"] = [x for x in hd_branches_data if x]

                hd_var4_data = tbx_hd_var4.get('1.0', 'end-1c').split('\n')
                data["Hadoop"]["var4"] = [x for x in hd_var4_data if x]

                hd_var5_data = tbx_hd_var5.get('1.0', 'end-1c').split('\n')
                data["Hadoop"]["var5"] = [x for x in hd_var5_data if x]

                hd_var6_data = tbx_hd_var6.get('1.0', 'end-1c').split('\n')
                data["Hadoop"]["var6"] = [x for x in hd_var6_data if x]

                json.dump(data, jsonfile)

        ###### Строим виджеты
        # Лейблы для выравнивания.
        for c in range(0, 32):
            Label(frame, bg=bg_color, text='').grid(row=c, column=35, stick='W')

        # Заголовки названий атрибутов
        a = 1
        for header in ['host', 'port', 'database', 'service_name', 'charset', 'branches', 'var4', 'var5', 'var6']:
            Label(frame, text=header).grid(row=0, column=a, stick='W')
            a += 1

        #  Заголовки названий БД
        i = 1
        for db in ('Teradata', 'Oracle', 'PostgreSQL', 'Hadoop'):
            Label(frame, text=db).grid(row=i, column=0, stick='W')
            i += 7

        tbx_td_host = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_td_host.grid(row=1, column=1, rowspan=8, sticky='NSWE')
        tbx_td_port = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_td_port.grid(row=1, column=2, rowspan=8, sticky='NSWE')
        tbx_td_database = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_td_database.grid(row=1, column=3, rowspan=8, sticky='NSWE')
        tbx_td_service_name = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_td_service_name.grid(row=1, column=4, rowspan=8, sticky='NSWE')
        tbx_td_charset = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_td_charset.grid(row=1, column=5, rowspan=8, sticky='NSWE')
        tbx_td_branches = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_td_branches.grid(row=1, column=6, rowspan=8, sticky='NSWE')
        tbx_td_var4 = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_td_var4.grid(row=1, column=7, rowspan=8, sticky='NSWE')
        tbx_td_var5 = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_td_var5.grid(row=1, column=8, rowspan=8, sticky='NSWE')
        tbx_td_var6 = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_td_var6.grid(row=1, column=9, rowspan=8, sticky='NSWE')

        tbx_ora_host = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_ora_host.grid(row=8, column=1, rowspan=8, sticky='NSWE')
        tbx_ora_port = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_ora_port.grid(row=8, column=2, rowspan=8, sticky='NSWE')
        tbx_ora_database = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_ora_database.grid(row=8, column=3, rowspan=8, sticky='NSWE')
        tbx_ora_service_name = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_ora_service_name.grid(row=8, column=4, rowspan=8, sticky='NSWE')
        tbx_ora_charset = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_ora_charset.grid(row=8, column=5, rowspan=8, sticky='NSWE')
        tbx_ora_branches = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_ora_branches.grid(row=8, column=6, rowspan=8, sticky='NSWE')
        tbx_ora_var4 = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_ora_var4.grid(row=8, column=7, rowspan=8, sticky='NSWE')
        tbx_ora_var5 = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_ora_var5.grid(row=8, column=8, rowspan=8, sticky='NSWE')
        tbx_ora_var6 = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_ora_var6.grid(row=8, column=9, rowspan=8, sticky='NSWE')

        tbx_pg_host = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_pg_host.grid(row=15, column=1, rowspan=8, sticky='NSWE')
        tbx_pg_port = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_pg_port.grid(row=15, column=2, rowspan=8, sticky='NSWE')
        tbx_pg_database = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_pg_database.grid(row=15, column=3, rowspan=8, sticky='NSWE')
        tbx_pg_service_name = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_pg_service_name.grid(row=15, column=4, rowspan=8, sticky='NSWE')
        tbx_pg_charset = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_pg_charset.grid(row=15, column=5, rowspan=8, sticky='NSWE')
        tbx_pg_branches = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_pg_branches.grid(row=15, column=6, rowspan=8, sticky='NSWE')
        tbx_pg_var4 = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_pg_var4.grid(row=15, column=7, rowspan=8, sticky='NSWE')
        tbx_pg_var5 = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_pg_var5.grid(row=15, column=8, rowspan=8, sticky='NSWE')
        tbx_pg_var6 = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_pg_var6.grid(row=15, column=9, rowspan=8, sticky='NSWE')

        tbx_hd_host = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_hd_host.grid(row=22, column=1, rowspan=8, sticky='NSWE')
        tbx_hd_port = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_hd_port.grid(row=22, column=2, rowspan=8, sticky='NSWE')
        tbx_hd_database = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_hd_database.grid(row=22, column=3, rowspan=8, sticky='NSWE')
        tbx_hd_service_name = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_hd_service_name.grid(row=22, column=4, rowspan=8, sticky='NSWE')
        tbx_hd_charset = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_hd_charset.grid(row=22, column=5, rowspan=8, sticky='NSWE')
        tbx_hd_branches = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_hd_branches.grid(row=22, column=6, rowspan=8, sticky='NSWE')
        tbx_hd_var4 = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_hd_var4.grid(row=22, column=7, rowspan=8, sticky='NSWE')
        tbx_hd_var5 = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_hd_var5.grid(row=22, column=8, rowspan=8, sticky='NSWE')
        tbx_hd_var6 = Text(frame, wrap='none', height=5, width=20, font=("Helvetica", 9))
        tbx_hd_var6.grid(row=22, column=9, rowspan=8, sticky='NSWE')

        # Кнопка "Закрыть"
        Button(frame, text='Закрыть', command=lambda: delete_tab(notebook)).grid(row=0, column=10, stick='we')

        # Кнопка "Прочитать"
        Button(frame, text='Прочитать', command=read_config).grid(row=1, column=10, stick='we')

        # Кнопка "Сохранить"
        Button(frame, text='Сохранить', command=save_config).grid(row=2, column=10, stick='we')


def create_options_page(notebook, bg_color):
    """Функция для построения вкладки для выбора и запуска проверок."""
    frame = Frame(notebook, bg=bg_color)
    frame.grid()

    tab_names = [notebook.tab(i, option='text') for i in notebook.tabs()]  # Список имен вкладок до добавления новой.
    notebook.add(frame, text='Настройки')

    notebook.select(notebook.index(len(tab_names)))  # Выбираем
    OptionsPage(frame, notebook, bg_color)


class OptionsPage:
    """Построение вкладки для внесения параметров через графический интерфейс."""

    def __init__(self, frame, notebook, bg_color):
        self.frame, self.notebook = frame, notebook
        self.bg_color = bg_color
        self.default_font_size = 8
        self.current_font_size = 8

        def increase_font_size(self, lbl_font_size):
            self.current_font_size +=1
            self.frame.option_add("*Font", f'Halvetica {self.current_font_size}')
            lbl_font_size.config(text=f'{self.current_font_size}')

        def decrease_font_size(self, lbl_font_size):
            self.current_font_size -=1
            self.frame.option_add("*Font", f'Halvetica {self.current_font_size}')
            lbl_font_size.config(text=f'{self.current_font_size}')

        Label(self.frame, text='Изменения вступят в силу после повторного открытия вкладки.', fg='black',
              bg=self.bg_color).grid(row=0, column=0,columnspan=9, sticky='WE')

        ###### Кнопки уменьшения и увеличения размера шрифта.
        Label(self.frame, text='Размер шрифта интерфейса', fg='black', bg=self.bg_color).grid(row=1, column=0, sticky='WE')
        lbl_font_size = Label(self.frame, text=f'{self.current_font_size}', fg='black', bg=self.bg_color)
        lbl_font_size.grid(row=1, column=1, sticky='WE')
        Button(self.frame, text='+', bg=self.bg_color, command=lambda: increase_font_size(self, lbl_font_size)).grid(row=1, column=2, stick='we')
        Button(self.frame, text='-', bg=self.bg_color, command=lambda: decrease_font_size(self, lbl_font_size)).grid(row=1, column=3, stick='we')