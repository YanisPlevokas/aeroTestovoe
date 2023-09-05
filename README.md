# Тестовое задание для компании AERO
## Первоначальная настройка
- Скачать/установить docker, docker-compose
- Настроить .env файл - прокинуть кастомные POSTGRES_PASSWORD, POSTGRES_USER, POSTGRES_DB
- Продублировать данную инфу в connections_dict в блоке с pg_cannabis
- Мб если запускаете на Linux (данный код тестился на ***винде 10***), то надо раскомментить 166-167 строки в docker-compose.yaml - это нужно для того, чтобы можно было ходить в постгрю по дефолтному хосту *host.docker.internal*

## Запуск

- Собрать docker-compose.yaml
- Запустить даг aero_test 
