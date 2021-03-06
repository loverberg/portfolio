# Hive.Create DataMart

#### Задача: Обеспечить постоянный доступ к холодным данным, создать схему `Star`, создать витрину вида:

| Payment type | Date |	Tips average amount | Passengers total |
| :------------| :--- | :------------------ | :--------------- |
|Cash|	2020-01-31|	999.99|	112|

#### Input-data: file.csv:
---
1,2020-04-01 00:41:22,2020-04-01 01:01:53,1,1.20,1,N,41,24,2,5.5,0.5,0.5,0,0,0.3,6.8,0

*семпл данных Нью-Йоркского такси за 2020. Это открытый датасет, который лежит на amazon `s3`. Подробнее здесь [Trip Record Data][1]

---




### Ход работы:
1. *Разворачивание кластера Hadoop* с использованием облачного решения Yandex.Cloud:
  - Имплементация сервера на базе операционной системы Ubuntu (инсталировано: `HDFS` v. 3.2.2, `YARN` v. 3.2.2, `MapReduce` v.3.2.2, `Hive` v. 3.1.2, `TEZ` v. 0.10.0);
    NameNode: 1x4Cores&16RAM, DataNode: 3x4Cores&16RAM; Subcluster.Compute: 2x4Cores&16RAM.
  - Создание бакета на `s3`.
2. *Копирование данных* на созданный бакет `s3` с помощью утилиты distcp, которая позволяет выполнить распределенное копирование данных между распределенными файловыми системами, использовал протокол - `s3a`. Это публичный бакет, возможен доступ без авторизации.
3. *Определение иерархии Hive:*
---
          - Database (yellow_taxi)
                - Table (payment - dimension table, input_data - fact table(input format), trip_part - fact table) 
                       - Partition (date) *опционально
                       - Bucket (не использую, т.к. не планирую использовать SMB (Sort Merge Bucket) Join) *опционально         
                - View (не планирую)
                - Materialized view (в качестве Data Mart)
                
       *утилита доступа - Hive CLI
       **таблицы будут созданны как внешние (`external`) для предотвращения потери данных
---
4. [*Cоздании Database*][2] (базы данных). 
5. [*Определение движка исполнения*][2] Hive - TEZ.
6. [*Создание таблицы — справочника][2] "payment" согласно описанию формата данных. Формат хранения — parquet. Имена полей id и name.
7. [*Наполнение dimension table.*][2]
8. [*Создание таблицы*][3] поездок надстроенной над имеющимеся данными в формате `csv`.
9. [*Создание таблицы фактов*][3] поездок партиционированные по дню начала поездки, формат хранения — `parquet`. Таким образом поиск нужных данных в таблице будет занимать нименьшее возможное время.
10. [*Определение параметров для автоматического партицирования*][4] таблицы фактов (связано с дополнительными рисками).
11. [*Трансформация и загрузка данных*][4] в таблицу фактов.
12. [*Создание витрины данных*][5] с помощью материализованного представления, при джойне таблиц использован хинт `MAPJOIN`.
---
Классический Join очень дорогая операция в Hive – требует сортировки обоих таблиц в MapReduce. При джойне больших таблиц, Hive может падать с ошибкой памяти. Hive позволяет MapReduce писать более лаконично. 

использован `Map side Join`

Для обеих таблиц запускаются mappers. Mappers, работающие с таблицей payment будет на выходе предоставлять пару ключ-значение. В качестве ключа будет использоваться id, в качестве значения - name. Mappers, работающие с таблицей поездок будут использовать фильтр и на выходе из них мы получаем ключ payment_type и значение - amount. Эти записи будут приходить на reducer, где мы уже получаем id, который для обеих таблиц по значению является одним и тем же, name из mappers, которые работали с таблицей payment и amount от mappers, работающих с таблицей поездок. Так как все ключи гарантированно на одном reducer, мы можем провести агрегацию по этим ключам и получить результат. 

---
13. [*Создание сценария кома́ндной стрoки*][6] для автоматизации построения витрины.
14. [*Rebuild*][7] витрины данных.



[1]:https://registry.opendata.aws/nyc-tlc-trip-records-pds/
[2]:https://github.com/loverberg/portfolio/blob/main/HiveStarSchemaDataMart/dicts.hql
[3]:https://github.com/loverberg/portfolio/blob/main/HiveStarSchemaDataMart/fact_table.hql
[4]:https://github.com/loverberg/portfolio/blob/main/HiveStarSchemaDataMart/put_in_fact_table.hql
[5]:https://github.com/loverberg/portfolio/blob/main/HiveStarSchemaDataMart/view.hql
[6]:https://github.com/loverberg/portfolio/blob/main/HiveStarSchemaDataMart/run.sh
[7]:https://github.com/loverberg/portfolio/blob/main/HiveStarSchemaDataMart/rebuld.sh
