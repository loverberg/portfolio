## Идентификация ботов среди пользовательских сессий

#### Проблематика: 
Занимаемая позиция - Data Endineer в продуктовой компании в сфере розничной торговли. 

По данным исследований аналитиков объемы продаж снизились из-за конкурентов, которые сбивают цены предлагая более низкие, чем на нашей платформе. У аналитиков данных есть подтвержденная гипотеза о странном росте количества выбросов в поведении пользователей, которые скорей всего являются краулерами или ботами и возможно как раз именно они и собирают информацию о ценах на товары размещенные у вас конкурентам.

#### Задача: 
Реализовать классификатор пользовательских сессии для того, чтобы отсеивать ботов показывая им капчу, для этого необходимо понимать идентификатор подозрительной сессии (session_id). (Реализовать две задачи для обучения лучшей модели и для ее применения.)

#### Структура исходных данных
(собраны за узкий промежуток времени, описывают поведение пользователей сайта)

|   Поле   | 	Описание                                                     |
|:--------:|:--------------------------------------------------------------|
|session_id| 	уникальный идентификатор сессии пользователя                 |
|user_type| 	тип пользователя [authorized/guest]                          |
|duration	| время длительности сессии (в секундах)                        |
|platform	| платформа пользователя [web/ios/android]                      |
|item_info_events	| число событий просмотра информации о товаре за сессию         |
|select_item_events	| число событий выбора товара за сессию                         |
|make_order_events	| число событий оформления заказа на товар за сессию            |
|events_per_min	| число событий в минуту в среднем за сессию                    |
|is_bot	| признак бота [0 - пользователь, 1 - бот]                      |


### Ход работы:
1. Создание Pipeline моделей, задача должна создавать план обучения моделей с учетом оптимизации гиперпараметров, определять лучшую модель и сохранять ее для дальнейшего использования. 

Реализация: [*PySparkMLFit.py*][1] 

2. Запуск задачи обучения моделей

Параметры запуска задачи:

data_path - путь к файлу с данными |
model_path - путь куда будет сохранена модель

---

    - python PySparkMLFit.py --data_path=data/session-stat.parquet --model_path=spark_ml_model
#### OR

    - spark-submit PySparkMLFit.py --data_path=data/session-stat.parquet --model_path=spark_ml_model
---
3. Применение модели, задача должна загружать модель, применять ее к указанному датасету и сохранять результат предсказаний в parquet формат содержащий два атрибута - [session_id, prediction].

Реализация: [*PySparkMLPredict.py*][2]

4. Запуск задачи применения модели

Параметры запуска задачи:

data_path - путь к файлу с данными |
model_path - путь к модели | 
result_path - output

---
    python PySparkMLPredict.py --data_path=data/test.parquet --model_path=spark_ml_model --result_path=result
#### OR
    spark-submit PySparkMLPredict.py --data_path=data/test.parquet --model_path=spark_ml_model --result_path=result
---


[1]:https://github.com/loverberg/portfolio/blob/main/BigML/SparkPipelineModel/PySparkMLFit.py
[2]:https://github.com/loverberg/portfolio/blob/main/BigML/SparkPipelineModel/PySparkMLPredict.py
