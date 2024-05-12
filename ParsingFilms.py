import logging
import yaml

# Configure logging
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Table, Float, UniqueConstraint
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
import requests
from bs4 import BeautifulSoup
import os
from sqlalchemy import create_engine


from appsettings import SettingsParser

# Открытие файла с настройками
with open('appsettings.yaml') as f:
    settings = yaml.safe_load(f)

# Извлечение значений переменных
url_pars_site = settings.get('URL_PARS_SITE', '')
adress_database = settings.get('ADRESS_DATA_BASE', '')
path_log = settings.get('PATH_LOGS', '')
page_number = settings.get('PAGE_NUMBER', '')

# Создание экземпляра SettingsParser
parser = SettingsParser()

# Добавление аргументов с использованием значений из YAML
parser.add_argument('--urlParsSite', default=url_pars_site, env_var='URL_PARS_SITE')
parser.add_argument('--adressDataBase', default=adress_database, env_var='ADRESS_DATA_BASE')
parser.add_argument('--pathLog', default=path_log, env_var='PATH_LOGS')
parser.add_argument('--pageNumber', default=page_number, env_var='PAGE_NUMBER')

# Парсинг аргументов
args = parser.parse_args()


# Создание базового класса для моделей SQLAlchemy
Base = declarative_base()

# Настройка логгера

log_filename = args.pathLog
if not os.path.exists(log_filename):
    with open(log_filename, 'w') as log_file:
        pass  # Создание пустого файла, если он не существует
logging.basicConfig(filename=args.pathLog, filemode='w', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO, encoding='utf-8')

# Определение модели Film для хранения информации о фильмах
class Film(Base):
    __tablename__ = 'films'
    id = Column(Integer, primary_key=True)  # ID фильма, первичный ключ
    name = Column(String)  # Название фильма
    year = Column(Integer)  # Год выпуска фильма
    rating = Column(Float)  # Оценка фильма

# Определение модели Tag для хранения тегов, связанных с фильмами
class Tag(Base):
    __tablename__ = 'tags'
    id = Column(Integer, primary_key=True)  # ID тега, первичный ключ
    name = Column(String, unique=True)  # Название тега, уникальное

# Определение модели Connection для хранения связи между фильмами и тегами
class Connection(Base):
    __tablename__ = 'connection'
    film_id = Column(Integer, ForeignKey('films.id'), primary_key=True)  # ID фильма, внешний ключ, первичный ключ связи
    tag_id = Column(Integer, ForeignKey('tags.id'), primary_key=True)  # ID тега, внешний ключ, первичный ключ связи
    __table_args__ = (UniqueConstraint('film_id', 'tag_id', name='uix_connection'),)  # Уникальное ограничение для связи

# Создание таблиц в базе данных
Base.metadata.create_all(bind=create_engine(f'sqlite:///{args.adressDataBase}'))


st_accept = "text/html" 
st_useragent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 YaBrowser/24.1.0.0 Safari/537.36"
headers = {
   "Accept": st_accept, # Что мы хотим от сервера/сайта
   "User-Agent": st_useragent
}
moviesList = []

class Movie:
    def __init__(self, name, year, rating, id=None,  tags=None ):  # Добавляем параметр tags с значением по умолчанию None
        self.id = id if id is not None else 0   # По умолчанию id равен 0
        self.name = str(name)
        self.year = int(year)
        self.rating = float(rating)
        if tags is None:  # Если tags не были предоставлены при создании объекта, инициализируем его пустым списком
            self.tags = []
        else:
            self.tags = list(tags)  # Преобразуем входные данные в список, если они уже представлены в другом формате

    def __str__(self):
        tag_str = ', '.join(self.tags) if self.tags else 'No tags'  # Если у фильма есть теги, объединяем их в строку, иначе указываем, что тегов нет
        return f"ID: {self.id}, Name: {self.name}, Year: {self.year}, Rating: {self.rating}, Tags: {tag_str}"


def GetHTML(link):
    req = requests.get(link, headers)
    # считываем текст HTML-документа
    src = req.text
    return src

def GetPage(codeHTML):
    logging.info("Обработка страницы")
    soup = BeautifulSoup(codeHTML, 'html.parser')
    navElement = soup.find('nav', class_='ratings_pagination bricks bricks-unite swipe outer-mobile inner-mobile')
    linkPageSoup = navElement.find_all('a', class_='bricks_item', string=lambda t: t and t.strip()!= "Вперед")
    linkPageList = [[lps.text, lps['href']] for lps in linkPageSoup]
    logging.info("Завершена обработка страницы")
    
    return linkPageList

def GetFilms(pageFilm):
    logging.info(f"Получение фильмов с {pageFilm[1]}")
    htmlLinkPageFilm = GetHTML(pageFilm[1])
    soup = BeautifulSoup(htmlLinkPageFilm, 'html.parser')

    movieItems = soup.find_all('a', class_='movieItem_title')


    # Проходим по всем найденным элементам и извлекаем нужные данные
    for item in movieItems:
        nameFilm = item.text  # Текст внутри тега <a>
        hrefFilm = item['href']  # Атрибут href
        moviesList.append(GetFilm(nameFilm , hrefFilm))
    logging.info(f"Завершено получение фильмов с {pageFilm[1]}")
        

def GetFilm(nameFilm, hrefFilm):
    srcFilm = GetHTML(hrefFilm)
    logging.info(f"Finished network request to {hrefFilm}")  
    soupFilm = BeautifulSoup(srcFilm, 'html.parser')
    
    yearFilm = soupFilm.find_all('span', class_='filmInfo_infoData')
    if(len(yearFilm) > 0):
        yearFilm = yearFilm[1].text
    else:
        yearFilm = 0
    yearFilm = CheckYear(yearFilm)

    ratingFilm = soupFilm.find('div', class_= 'ratingBlockCard_local').text
    
    tags = GetTags(soupFilm)
    
    result = Movie(name = nameFilm, year = yearFilm, rating = ratingFilm, tags = tags)

    return result


def GetTags(soupFilm):
    tags = []

    tagsSoup = soupFilm.find_all('span', class_='filmInfo_genreItem button-main')
    for tagSoup in tagsSoup:
        tags.append(tagSoup.text)
    
    return tags

def CheckYear(yearFilm):
    try:
        # Попытка преобразовать yearFilm в целое число
        int(yearFilm)
        return int(yearFilm)
    except ValueError:
        # Если произошло исключение ValueError, yearFilm не является целым числом
        print(f"{yearFilm} не является целым числом. Год 0.")
        return 0

# Функция для вставки фильма в базу данных
def InsertMovieToDb(movie):
    # Создание сессии для работы с базой данных
    engine = create_engine('sqlite:///films.db')
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
         # Checking if a movie with the same name already exists
        existingMovie = session.query(Film).filter_by(name=movie.name).first()
        if existingMovie:
            logging.info(f"Movie with name {movie.name} already exists. Skipping insertion.")
            return  # Optionally, you can update the existing movie here

        # Вставка нового фильма в базу данных
        logging.info(f"Starting to insert movie: {movie.name}")
        new_film = Film(name=movie.name, year=movie.year, rating=movie.rating)
        session.add(new_film)
        session.commit()

        # Вставка тегов и связывание их с фильмом
        for tag in movie.tags:
            # Проверка на существование тега в базе данных
            existing_tag = session.query(Tag).filter_by(name=tag).first()
            if existing_tag:
                # Если тег существует, создаем связь с фильмом
                new_connection = Connection(film_id=new_film.id, tag_id=existing_tag.id)
                session.add(new_connection)
                session.commit()
                logging.info(f"Successfully inserted movie: {movie.name}")
            else:
                # Если тег не существует, вставляем его и создаем связь с фильмом
                new_tag = Tag(name=tag)
                session.add(new_tag)
                session.commit()

                new_connection = Connection(film_id=new_film.id, tag_id=new_tag.id)
                session.add(new_connection)
                session.commit()
    except Exception as e:
        # В случае ошибки откатываем изменения и логируем ошибку
        session.rollback()
        logging.error(f"Error inserting movie: {movie.name}. Error: {e}")
    finally:
        # Закрытие сессии
        session.close()



if __name__ == "__main__":
    try:
        mainHtml = GetHTML("https://www.kinoafisha.info/rating/movies/")
        pagesFilms = GetPage(mainHtml)
        print(pagesFilms)
        for pageFilm in pagesFilms:
            if(int(pageFilm[0]) > int(args.pageNumber)):
                break
            GetFilms(pageFilm)
        print(moviesList)
        for movie in moviesList:
            print(movie)
            InsertMovieToDb(movie)
    except Exception as e:
        logging.error(f"Произошла ошибка: {e}")