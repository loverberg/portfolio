## Deequ для Data Quality

#### Проблематика: 
От внешней системы-источника поставляются данные о доступных товарах (обуви).


#### Задача: 
Используя возможности `PyDeequ` необходимо разработать приложение для анализа
данных, предусмотрев накопление отчетов о качестве.
___
__Предложены проверки:__
1) Размер датасета
2) Полнота всех атрибутов (x9)
3) Уникальность id
4) Присутствуют ли записи со скидкой меньше 0
5) Присутствуют ли записи со скидкой больше 100
6) Присутствуют ли записи с доступным количеством на складе меньше 0
7) Присутствуют ли записи с количеством предзаказов меньше 0
___
#### Структура исходных данных

|       Поле      | 	Описание                                                                     |
|:---------------:|:------------------------------------------------------------------------------|
|        id       | 	Уникальный идентификатор объекта                                       |
|   vendor_code	  | 	Уникальный идентификатор позиции (товара)                                    |
|       name      | Название модели                                                          |
|       type      | Тип модели обуви                                                     |
|      label      | Бренд                                                        |
|      price      | Цена ($)                 |
|    discount	    | 	Скидка в процентах [0-100]                |
|        available_count         | Количество доступное на складе                                       |
| preorder_count| Количество предзаказов                                    |


#### Реализация: [*PySparkAnalyzer.py*][1] 


*запуск приложения*

---
    python PySparkAnalyzer.py --data_path=data/data.parquet --report_path=report
#### OR
    spark-submit PySparkAnalyzer.py --data_path=data/data.parquet --report_path=report
---

*сохраненный результат выполнения:*
 

| entity|            instance|        name|   value|
|:------|:-------------------|-----------:|-------:|
| Column|discount great th...|  Compliance|     0.0|
| Column|available count l...|  Compliance|   0.001|
| Column|         vendor_code|Completeness|     0.9|
| Column|               label|Completeness|     1.0|
| Column|discount less than 0|  Compliance|  3.5E-4|
| Column|     available_count|Completeness|     1.0|
| Column|            discount|Completeness|     1.0|
| Column|                type|Completeness|    0.95|
| Column|      preorder_count|Completeness|     1.0|
| Column|               price|Completeness|    0.97|
|Dataset|                   *|        Size|100000.0|
| Column|                  id|  Uniqueness|     1.0|
| Column|                  id|Completeness|     1.0|
| Column|preorder count le...|  Compliance|     0.0|
| Column|                name|Completeness|     1.0|

[1]:https://github.com/loverberg/portfolio/blob/main/%20DeequForDataQuality/PySparkAnalyzer.py
