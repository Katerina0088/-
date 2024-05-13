# Используем официальный образ Python как базовый
FROM python:3.12

# Устанавливаем рабочую директорию в контейнере
WORKDIR /app

# Копируем файлы проекта в контейнер
COPY . /app

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Команда для запуска приложения
CMD ["python", "u.py"]


#команды для запуска докера:
#docker build -t название образа
#docker run -p 4000:80 название образа
#docker start цифровой id контейнера