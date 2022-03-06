# MapReduce.Aggregation function

#### Задача: Написать map-reduce приложение, использующее данные Нью-Йоркского такси и вычисляющее среднее значение "чаевых" за каждый месяц 2020 года:

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
2. [*Копирование данных*][1] на `HDFS` с помощью утилиты distcp, которая позволяет выполнить распределенное копирование данных между распределенными файловыми системами. Это публичный бакет, возможен доступ без авторизации.
3. [*Разработка mapper*][2].
4. [*Разработка reducer*][3]. 
5. [*Разработка shell-скрипта*][4] для запуска MapReduce задания. Дополнительно настроен размер контейнера для операций `map` (т.к. стандартный размер составляет 3072Mb, а обрабатываемые файлы меньше 1024Mb, значение изменено на 1024Mb), что значительно ускоряет стадию `map`.
6. [*Доставка кода*][5] на удаленный сервер `make copy`.
7. *Запуск приложения*.


[1]:https://github.com/loverberg/portfolio/blob/main/MapReduceAgg/download.sh
[2]:https://github.com/loverberg/portfolio/blob/main/MapReduceAgg/mapper.py
[3]:https://github.com/loverberg/portfolio/blob/main/MapReduceAgg/reducer.py
[4]:https://github.com/loverberg/portfolio/blob/main/MapReduceAgg/run.sh
[5]:https://github.com/loverberg/portfolio/blob/main/MapReduceAgg/Makefile
