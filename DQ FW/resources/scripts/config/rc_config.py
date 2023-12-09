import json
import os

class Configuration:
    """Класс для работы с файлом конфигурации. Позволяет хранить все настройки в файле
    config.json, получать конфигурацию по запросу."""

    def __init__(self):
        """Читаем конфигурационный файл при инициализации."""
        cwd = os.getcwd()

        if cwd[-10:] == 'RunChecker':
            self.jsonfile = r'./resources/scripts/config/config.json'
        else:
            self.jsonfile = r'//region.vtb.ru/dfs/OKKKD/Команды/Кластер КД Платформы данных/RunChecker/resources/scripts/config/rc_config.json'

        with open(self.jsonfile, "r") as read_config:
            self.config_settings = json.load(read_config)


    def get(self, db):
        """Метод для возврата значения настройки по ее названию."""
        return self.config_settings.get(db)


c = Configuration()