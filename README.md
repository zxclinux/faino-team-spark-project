# Big Data Mining Project  
**Course:** Big Data Mining  
**Team:** Faino Team  

## Overview  

This project is developed as part of the *Big Data Mining* course.  
Its main goal is to gain practical experience working with large-scale datasets using **Apache Spark** and distributed SQL queries.

The project focuses on:

- Processing and analyzing large datasets from open data sources  
- Writing SQL queries over distributed data  
- Performing data exploration and extracting meaningful insights  
- Practicing collaborative development using GitHub / GitLab  
- Working effectively as a team in a Big Data environment  

## Objectives  

- Develop hands-on skills with Apache Spark  
- Apply distributed data processing techniques  
- Analyze real-world large datasets  
- Present analytical findings clearly and logically  
- Strengthen teamwork and version control practices  

## Team  

Faino Team 

## Відповідальності учасників

Етап налаштування:
Кожен учасник встановив Docker та перевірив його роботу.

Ярослав та Дмитро: Конфігурація Dockerfile для створення образу my-spark-img.

Юрій: Налаштування main.py як точки входу в застосунок.

Роман: Налаштування команд запуску контейнера.

Етап видобування:
Кожен учасник: Робота над спільним модулем зчитування даних.

Дмитро: Створення відповідних схем для набору даних.

Роман: Реалізація логіки створення DataFrame шляхом зчитування розбитих на партиції файлів.

Ярослав: Створення окремого модуля та функцій для операції зчитування.

Юрій: Перевірка коректності зчитування (валідація схеми та кількості рядків).

Етап попередньої обробки даних:
Роман: Отримання загальної та статистичної інформації про числові ознаки.

Дмитро: Приведення ознак до потрібного типу та видалення неінформативних полів.

Ярослав: Обробка пропущених значень та парсинг дати.

Юрій: Проведення аналізу на наявність дублікатів та їх видалення.

Етап трансформації:
Кожен учасник: У своєму файлі реалізує 6 власних бізнес-питань та аналізує план виконання.

Етап запису результатів:
Кожен учасник: Реалізує функцію запису своїх відповідей у .csv, налаштовує параметри запису та ігнорування результатів у .gitignore, координує структури вихідних папок output/, перевіряє коректність формату записаних файлів та виконує збір усіх планів у документацію проекту.

Етап аналізу даних:
Дмитро: Регресійна модель для business.stars — побудова таблиці ознак (ml_features.py), навчання та порівняння LinearRegression, DecisionTreeRegressor, RandomForestRegressor, GBTRegressor з крос-валідацією (regression.py), оцінка RMSE / R² / MAE, аналіз процесу навчання (objective history, крива навчання GBT, важливість ознак).

Ярослав: Класифікаційна модель для business.is_open (відкритий / закритий бізнес) — підготовка цілі без витоку ознаки is_open, балансування класів вагами (weightCol), навчання та порівняння LogisticRegression, DecisionTreeClassifier, RandomForestClassifier, GBTClassifier з крос-валідацією (classification.py), оцінка Accuracy / Precision / Recall / F1 (загальних і для класу «закритий») та ROC AUC проти бейзлайну «завжди відкритий», аналіз процесу навчання (objective history, крива навчання GBT, важливість ознак, матриці помилок, порівняння train/test).

### Висновки: класифікація (is_open)

Дані: 150 243 бізнеси (train 120 284 / test 29 959), класи нерівні — 79.7% відкритих і 20.3% закритих. Бейзлайн «завжди відкритий» має Accuracy 0.793, але не знаходить жодного закритого бізнесу (Recall класу 0 = 0), тому основні метрики порівняння — F1 і Recall для класу «закритий» та ROC AUC.

| Модель (test) | Accuracy | F1 (weighted) | ROC AUC | Precision (закр.) | Recall (закр.) | F1 (закр.) | F1 train |
|---|---|---|---|---|---|---|---|
| GBTClassifier | 0.759 | 0.777 | **0.832** | 0.450 | **0.736** | **0.558** | 0.783 |
| RandomForestClassifier | **0.764** | **0.780** | 0.816 | **0.454** | 0.694 | 0.549 | 0.799 |
| LogisticRegression | 0.738 | 0.759 | 0.811 | 0.423 | 0.728 | 0.535 | 0.757 |
| DecisionTreeClassifier | 0.725 | 0.748 | 0.642 | 0.408 | 0.721 | 0.521 | 0.765 |
| Baseline (always open) | 0.793 | 0.701 | 0.500 | 0.000 | 0.000 | 0.000 | 0.707 |

- **Найкраща модель — GBTClassifier** (maxDepth=5, maxIter=60): найвищі ROC AUC і F1 для закритих бізнесів, знаходить 74% закритих. Бустинг послідовно виправляє помилки попередніх дерев і краще ловить нелінійні залежності, ніж логістична регресія. RandomForest майже не поступається і має найвищі Accuracy/weighted F1, але пропускає більше закритих.
- **Ваги класів (weightCol) критичні**: без них моделі прагнули б до бейзлайну. Нижча Accuracy моделей порівняно з бейзлайном — свідомий компроміс: модель частіше помилково вважає відкриті бізнеси закритими, зате знаходить ~70% справді закритих.
- **Перенавчання**: різниця train/test невелика (F1 train−test ≤ 0.02, у RandomForest найбільша — 0.019), у LogisticRegression test навіть трохи кращий. CV на всіх деревах обирає найглибші конфігурації, отже моделі ще не перенавчені.
- **Процес навчання**: LogisticRegression збіглася за 35 ітерацій L-BFGS; регуляризація майже не впливає (regParam 0–0.01 дають однаковий CV F1), сильна (0.1 + L1) погіршує. У GBT log-loss на train і test спадає разом на всіх 60 ітераціях (classification_learning_curves).
- **Найважливіші ознаки** в усіх моделях: review_years_span (бізнеси з довшою історією відгуків частіше ще працюють), належність до категорії Restaurants (ресторани закриваються частіше), кількість check-in/tip/відгуків. Одне дерево рішень має найнижчий AUC (0.64) — його ймовірності грубі (листки), тому ансамблі помітно кращі.
- **Обмеження**: ознака review_years_span частково є наслідком закриття, а не причиною (закритий бізнес перестає отримувати відгуки, тому його історія коротша). Через це метрики дещо оптимістичніші, ніж були б для прогнозу закриття наперед.

Запуск лише ML-етапу: docker compose run --rm spark python main.py --stage ml
