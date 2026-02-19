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