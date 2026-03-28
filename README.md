# Лабораторные работы по Apache Spark (Scala)

Учебный репозиторий с практическими заданиями по распределённой обработке данных на **Apache Spark**. Материалы выполняются в рамках обучения в **[New Professions Lab](https://newprolab.com/)** (трек развития дата-инженеров, в том числе работа со Spark на Scala в программе **Spark Scala DE** и связанных модулях DE Path).

## Стек

- **Scala** 2.12.18  
- **Apache Spark** 3.4.3 (SQL, Structured Streaming, MLlib)  
- **Сборка:** sbt  
- **Интеграции (по лабораторным):** Kafka, Cassandra, Elasticsearch, PostgreSQL; в отдельных работах — Python-скрипты

## Структура репозитория

| Каталог | Содержание |
|--------|------------|
| `lab03/data_mart` | Сборка витрины данных: чтение из Cassandra и Elasticsearch, запись в PostgreSQL; параметры подключения из файла `.env` (см. ниже) |
| `lab04a/filter` | Structured Streaming: чтение из Kafka, фильтрация событий, запись результата (путь и топик задаются через `spark.*`) |
| `lab04b/agg` | Стриминг из Kafka, агрегации по микробатчам, чекпоинты, вывод в HDFS |
| `lab05/users_items` | Объединение событий по пользователям и товарам, pivot-матрица «user × item», работа с датами и путями в HDFS |
| `lab06/features` | Подготовка признаков из веб-логов: домены, оконные функции, агрегаты по пользователям |
| `lab07/mlproject` | Обучение и оценка модели на Spark (train / test) |
| `lab07s/mlproject` | Расширенный ML-проект на Scala и вспомогательные скрипты в `python/` |
| `lab08/dashboard` | Стриминговая обработка, применение сохранённой ML-модели (`PipelineModel`), индексация в Elasticsearch |

## Требования

- **JDK** 8 или 11 (совместимость со Spark 3.4)  
- **[sbt](https://www.scala-sbt.org/)**  
- Для части лаб — доступ к кластеру и сервисам (Kafka, HDFS, ES и т.д.), как на учебной инфраструктуре New Pro Lab

## Запуск

В корне каждого подпроекта со своим `build.sbt`:

```bash
cd lab03/data_mart   # или другая лабораторная
sbt run
```

Либо сборка JAR и запуск через `spark-submit` — по инструкциям курса.

**Важно:** в коде и конфигурациях могут быть захардкожены хосты, топики Kafka, пути HDFS (`/user/...`, `/labs/...`) и другие параметры **учебного кластера**. Для локального или своего окружения их нужно заменить на актуальные значения.

### Лабораторная `lab03/data_mart`

В каталоге `lab03/data_mart` должен лежать файл **`.env`** (он в `.gitignore` и в репозиторий не коммитится). Пример ключей, которые читает `Env.scala`:

- `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`  
- `CASSANDRA_HOST`, `CASSANDRA_PORT`  
- `ES_HOST`, `ES_PORT`

Формат: строки `KEY=value`, без кавычек вокруг значения или с кавычками — как поддерживает разбор в [Env.scala](lab03/data_mart/Env.scala).

---

*English:* Course lab exercises for Apache Spark with Scala, completed as part of [New Professions Lab](https://newprolab.com/) data engineering programs.
