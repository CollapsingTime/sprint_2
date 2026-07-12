## Изучите [README.md](.\README.md) файл и структуру проекта.

# Задание 1

[Контейнерная диаграмма C4](diagrams/c4-container-diagram.puml)


# Задание 2

### 1. Proxy
- прокси реализован на Go
- тесты успешно проходят (без учета events сервиса)
- при смене значения в переменной MOVIES_MIGRATION_PERCENT в логах контейнера видна правильная маршрутизация

### 2. Kafka
- сервис реализован на Go
[Результат тестов](screenshots/tests_result.png)
[Состояние топиков Kafka](screenshots/kafka_topics_status.png)


# Задание 3
- Обе части задания реализованы

### CI/CD
- после проверки workflows откатит main ветку в первоначальное состоение
[Результаты workflows](screenshots/task3_part1.png)

### Proxy в Kubernetes
[Вызов curl http://cinemaabyss.example.com/api/movies](screenshots/task3_part2_1.png)
[Логи event-service после запуска тестов](screenshots/task3_part2_2.png)


# Задание 4
[run helm](screenshots/task_4_run_helm.png)
[статус подов](screenshots/task_4_kube_get_pods.png)

[вывод curl http://cinemaabyss.example.com/api/movies](screenshots/task_4_get_api_movies.png)
