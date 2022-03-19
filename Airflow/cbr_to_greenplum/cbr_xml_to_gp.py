"""
Складываем курсы валют (from: Центральный банк России) в GreenPlum по будням
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import csv
import xml.etree.ElementTree as ET


from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(200),
    'owner': 'loverberg', # отображает владельца DAG'а в интерфейсе
    'poke_interval': 600, #  время в секундах, которое задание должно ждать между каждыми попытками
    'retries': 3, # количество перезапусков таска в случае падения (1 запуск всегда основной + переданное количество в аргументе)
    'retry_delay': 10, # время между попытками перезапуска
    'priority_weight': 2 # вес приоритета этого таска перед другими
}

url = 'https://www.cbr.ru/scripts/XML_daily.asp?date_req={ macros.ds_format(ds, "%Y-%m-%d", "%Y/%m/%d") }}'
    # создаю динамическую переменную с адресом
xml_file = '/tmp/loverberg_cbr.xml'
csv_file = '/tmp/loverberg_cbr.csv'

with DAG("loverberg_load_cbr",
          schedule_interval='0 0 * * 1-6', # запуск экземпляров DAG только в будние дни (т.к. в выходные курсы не обновляются)
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['loverberg']
          ) as dag:

    delete_xml_file_script = f'rm {xml_file}'

    delete_xml_file = BashOperator(
        task_id='delete_xml_file',
        bash_command=delete_xml_file_script,
        trigger_rule='dummy' # состояние таска не влияет на состояне экземпляра DAG
    ) # удаляем предыдущие файлы при наличии

    delete_csv_file_script = f'rm {csv_file}'

    delete_csv_file = BashOperator(
        task_id='delete_csv_file',
        bash_command=delete_csv_file_script,
        trigger_rule='dummy'
    )

    load_cbr_xml_script = f'curl {url} | iconv -f Windows-1251 -t UTF-8 > {xml_file}'
        # используем утилиту iconv (bash) для изменения кодировки

    load_cbr_xml = BashOperator(
        task_id='load_cbr_xml',
        bash_command=load_cbr_xml_script
    )

    # пишу парсер и конвертер xml -> csv
    def export_xml_to_csv_func():
        parser = ET.XMLParser(encoding="UTF-8")
        tree = ET.parse(xml_file, parser=parser)
        root = tree.getroot()

        with open(csv_file, 'w') as file:
            writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for Valute in root.findall('Valute'):
                NumCode = Valute.find('NumCode').text
                CharCode = Valute.find('CharCode').text
                Nominal = Valute.find('Nominal').text
                Name = Valute.find('Name').text
                Value = Valute.find('Value').text
                writer.writerow([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                                [Name] + [Value.replace(',', '.')])
                logging.info([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                             [Name] + [Value.replace(',', '.')])
        return root.attrib['Date'] # использую XCom для передачи значения в следующий таск

    export_xml_to_csv = PythonOperator(
        task_id='export_xml_to_csv',
        python_callable=export_xml_to_csv_func,
    )

    # подключение к экземпляру GreenPlum и загрузка данных (предусмотренно пакетное удаление данных в случае частичной отработки предыдущего DAG
    def load_csv_to_gp_func(**kwargs):
        pg_hook = PostgresHook('conn_greenplum_write')
        conn = pg_hook.get_conn()
        conn.autocommit = True
        cursor = conn.cursor()  # ("named_cursor_name")
        logging.info("DELETE FROM public.loverberg_cbr WHERE dt = '{}'".format(kwargs['templates_dict']['implicit']))
        cursor.execute("DELETE FROM public.loverberg_cbr WHERE dt = '{}'".format(kwargs['templates_dict']['implicit']))
        conn.close()
        pg_hook.copy_expert("COPY public.loverberg_cbr FROM STDIN DELIMITER ','", csv_file)


    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func,
        templates_dict={'implicit': '{{ ti.xcom_pull(task_ids="export_xml_to_csv") }}'}, # pull Xcom
        provide_context=True
    )

load_cbr_xml >> export_xml_to_csv >> load_csv_to_gp