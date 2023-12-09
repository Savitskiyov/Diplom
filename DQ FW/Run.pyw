import os, sys
from datetime import datetime
from tkinter import messagebox


try: # Для перехвата мгновенно отображающихся в окне и пропадающих ошибок.
    modules_path = r'./resources/site-packages/'
    sys.path.append(os.path.dirname(modules_path)) # Чтобы не устанавливать библиотеки.
    from resources.scripts.app.app_main_gui import main

    if __name__ == '__main__':
        main()
except Exception as ex:
    # Пишем ошибку запуска в файл.
    ex_type, dttm, ex_msg = type(ex).__name__, datetime.now().strftime("%d.%m.%Y %H:%M:%S"), str(ex)
    ex_file, ex_file_line, ex_user = __file__, 'line:' + str(ex.__traceback__.tb_lineno), os.getlogin()
    err_string = ex_user + '\t' + dttm + '\t' + ex_type + '\t' + ex_msg + '\t' + ex_file + '\t' + ex_file_line + '\n'

    with open('error_description.txt', 'w') as f:
        f.write(err_string)

    # Показываем уведомление пользователю.
    messagebox.showerror('Ошибка при запуске приложения', 'Возникла ошибка:\n' + str(ex))