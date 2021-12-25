В данном репозитории представлены результаты лабораторных работ, выполненных в ходе изучения курса “Taming Big Data with Apache Spark and Python - Hands On!”. Кроме того были рассмотрены:
* использование DataFrames и Structured Streaming в Spark 3;
* формулирование проблем анализа больших данных как проблемы Spark;
* использование сервиса Amazon Elastic MapReduce для выполнения заданий на кластере с Hadoop YARN;
* проблематика инициализации и запуска Apache Spark на настольном компьютере и на кластере;
* использование Spark's Resilient Distributed Datasets для обработки и анализа больших наборов данных с помощью набора процессоров;
* реализация итеративных алгоритмов, таких как breadth-first-search , используя Spark;
* использование библиотек машинного обучения MLLib для решения распространенных вопросов, связанных с анализом данных;
* рассмотрение вопросов в рамках работы со  структурированными данными с помощью Spark SQL;
* работа над обработкой непрерывных потоков данных в реальном времени Spark Streaming;
* работа с ошибками при выполнении больших заданий на кластере;
* проблематика обмена информацией между узлами кластера Spark с помощью broadcast variables и accumulators;
* библиотека GraphX и вопросы связанные с сетевым анализом.

### Навигатор 

| **Technology**               | Проблематика                                                                                                                                                                                                                                                                               |
| :-- | :-- |
| `SparkSQL`                   | [Коллаборативная фильтрация, может быть использована после первого отзыва пользователя стриминговой платформы, использует косинусное расстояние] (https://github.com/loverberg/portfolio/blob/main/BigDataWithApacheSpark/CollaborativeFiltering/collaborativeFilteringCosineSimilarity.py)|
| `RDD for AWS S3`             | [Коллаборативная фильтрация, может быть использована после первого отзыва пользователя стриминговой платформы, использует косинусное расстояние] (https://github.com/loverberg/portfolio/blob/main/BigDataWithApacheSpark/CollaborativeFiltering/MovieSimilarities1MviaAWSS3.py)           |
| `MLlib&SparkSQL`             | [Рещающие деревья для предсказания стоимости квадратного метра недвижимости](https://github.com/loverberg/portfolio/blob/main/BigDataWithApacheSpark/SparkML/DecisionTreesInSparkML.py)                                                                                                    |
| `Windows&StructuredStreaming`| [Использование оконной функции со структурированной потоковой передачей для отслеживания наиболее просматриваемых URL-адресов] (https://github.com/loverberg/portfolio/blob/main/BigDataWithApacheSpark/UseWindowswithStructuredStreaming/top-urls.py)                                     |

[Taming Big Data with Apache Spark and Python - Hands On!](https://www.udemy.com/course/taming-big-data-with-apache-spark-hands-on/)
