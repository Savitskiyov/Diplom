from tkinter import Tk, ttk, Frame, Label, Button
from PIL import ImageTk, Image
import os

from resources.scripts.app.app_tab_gui import create_check_page, create_config_page, create_options_page
from resources.scripts.functions.rc_functions import show_readme_file, open_explorer, get_path


def main():
    """Функция строит окно главного приложения."""
    # Создаем главное приложение.
    main_color = '#eeebe6'

    root = Tk()
    root.title('RC Test runner')
    root.resizable(width=True, height=True)
    root.option_add("*Font", 'Halvetica 8')
    root.configure(background=main_color)

    s = ttk.Style()
    s.configure('TNotebook', tabposition='nw', background=main_color)

    notebook = ttk.Notebook(root)
    notebook.grid(row=0, column=1, rowspan=100)

    #Боковые кнопки
    build_side_buttons(root, notebook, main_color)

    # Основная вкладка - Логотип ВТБ и кластера.
    menu_tab = Frame(notebook, bg=main_color)
    notebook.add(menu_tab, text='Лого')
    img_vtb_logo_object = Image.open(r'./resources/images/VTB_logo_ru_cream.png')
    img_vtb_logo = ImageTk.PhotoImage(img_vtb_logo_object)
    lbl_menu_tab_logo = Label(menu_tab, image=img_vtb_logo)
    lbl_menu_tab_logo.image = img_vtb_logo
    lbl_menu_tab_logo.grid(row=0, column=1)

    ### Вкладка с настройками приложения.
    create_options_page(notebook, main_color)

    # Выбираем первую вкладку - "Лого".
    tab_names = [notebook.tab(i, option='text') for i in notebook.tabs()]
    notebook.select(notebook.index(len(tab_names)-2))

    root.mainloop()


def build_side_buttons(root, notebook, main_color):
    """Функция строит боковые кнопки интерфейса приложения."""
    user_login = os.getlogin()

    for i in range(11):
        # Кнопки для подключения к БД
        if i in (0, 1, 2, 3):
            if i == 0:
                value = ['Teradata', main_color, user_login]
            elif i == 1:
                value = ['Oracle', main_color, 'STAGE']
            elif i == 2:
                value = ['PostgreSQL', main_color, user_login.lower()]
            elif i == 3:
                value = ['Hadoop', main_color, user_login.lower()]
            Button(root, text=value[0], bg=main_color, anchor='w', command=lambda title=value[0], color=value[1], login=value[2]:
                   create_check_page(notebook, window_title=title, bg_color=color, username=login)).grid(row=i, column=0, sticky='WE')
        # Пропуск
        elif i == 4:
            Label(root, text='', bg=main_color, anchor='w').grid(row=i, column=0, sticky='WE')
        # Результаты
        elif i == 5:
            Button(root, text='Результаты', bg=main_color, anchor='w', command=lambda title='Результаты': open_explorer(get_path() + '\\results\\')).grid(row=i, column=0, sticky='WE')
        # Наборы
        elif i == 6:
            Button(root, text='Наборы', bg=main_color, anchor='w', command=lambda title='Наборы': open_explorer(get_path() + '\\templates\\')).grid(row=i, column=0, sticky='WE')
        # Логи
        elif i == 7:
            Button(root, text='Логи', bg=main_color, anchor='w', command=lambda title='Логи': open_explorer(get_path() + '\\logs\\')).grid(row=i, column=0, sticky='WE')
        # Пропуск
        elif i == 8:
            Label(root, text='', bg=main_color, anchor='w').grid(row=i, column=0, sticky='WE')
        # Конфигурация
        elif i == 9:
            Button(root, text='Конфигурация', bg=main_color, anchor='w',
                   command=lambda: create_config_page(notebook, main_color)).grid(row=i, column=0, sticky='WE')
        # Справка
        elif i == 10:
            Button(root, text='Справка', bg=main_color, anchor='w', command=show_readme_file).grid(row=i, column=0, sticky='WE')