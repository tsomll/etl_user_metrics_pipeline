# ETL User Metrics Pipeline

## 📌 Описание проекта

Этот проект реализует **ежедневный ETL-процесс с помощью Apache Airflow** для агрегации пользовательской активности из двух источников: `feed_actions` и `message_actions`. Полученные метрики ежедневно записываются в финальную таблицу ClickHouse.

---

## 🎯 Цель

Построить автоматизированный пайплайн, который:
- Считает ключевые метрики пользовательской активности за каждый предыдущий день
- Делает агрегации по срезам: **пол (gender)**, **возраст (age)**, **операционная система (OS)**
- Загружает результат в ClickHouse для последующего анализа

---

## 🧩 Источники данных

- **`feed_actions`** — данные о взаимодействии с лентой:
  - Количество просмотров (`views`)
  - Количество лайков (`likes`)

- **`message_actions`** — данные по сообщениям:
  - Отправленные и полученные сообщения
  - Уникальные пользователи, от которых пришли/которым отправлены сообщения

---

## 🛠️ Этапы пайплайна (DAG)

1. **Извлечение данных**
   - `feed_actions`: агрегация просмотров и лайков
   - `message_actions`: агрегация сообщений и уникальных пользователей

2. **Объединение**
   - Мерж таблиц по `user_id` на уровне дня

3. **Агрегации по срезам**
   - Отдельные таски для:
     - `gender`
     - `age`
     - `os`

4. **Загрузка в ClickHouse**
   - Итоговая таблица обновляется ежедневно
   - Структура финальной таблицы:
     ```
     event_date | dimension | dimension_value | views | likes | messages_received | messages_sent | users_received | users_sent
     ```

---

## 💻 Используемые технологии

- `Python`
- `Apache Airflow`
- `ClickHouse`
- `SQL`

---

## 🚀 Результат

Автоматизация ежедневного сбора и агрегации пользовательской активности с удобным разделением по ключевым демографическим срезам. Готовое решение для аналитики и построения продуктовых дашбордов.




Используемые технологии
Python
Apache Airflow
ClickHouse
SQL
