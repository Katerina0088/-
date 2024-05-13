from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Table, Float, UniqueConstraint
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from appsettings import SettingsParser
import requests
from bs4 import BeautifulSoup
import os
from sqlalchemy import create_engine
import re
import logging
import yaml



# Открытие файла с настройками
with open('appsettings.yaml') as f:
    settings = yaml.safe_load(f)

# Извлечение значений переменных
url_pars_site = settings.get('URL_PARS_SITE', '')
adress_database = settings.get('ADRESS_DATA_BASE', '')
path_log = settings.get('PATH_LOGS', '')


# Создание экземпляра SettingsParser
parser = SettingsParser()

# Добавление аргументов с использованием значений из YAML
parser.add_argument('--urlParsSite', default=url_pars_site, env_var='URL_PARS_SITE')
parser.add_argument('--adressDataBase', default=adress_database, env_var='ADRESS_DATA_BASE')
parser.add_argument('--pathLog', default=path_log, env_var='PATH_LOGS')

# Парсинг аргументов
args = parser.parse_args()

logging.basicConfig(filename=args.pathLog, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', encoding='utf-8')
# Определение базового класса для моделей SQLAlchemy
Base = declarative_base()

# Определение модели Vacancy для хранения информации о вакансиях
class Vacancy(Base):
    __tablename__ = 'vacancy'
    id = Column(Integer, primary_key=True)  # ID вакансии, первичный ключ
    name = Column(String)  # Название вакансии
    salary = Column(String)  # Зарплата
    description = Column(String)  # Текст вакансии
    work_time = Column(String)
    experience = Column(String)
    address = Column(String)
    education = Column(String)

# Определение модели Skill для хранения навыков
class Skill(Base):
    __tablename__ = 'skill'
    id = Column(Integer, primary_key=True)  # ID навыка, первичный ключ
    name = Column(String, unique=True)  # Название навыка

# Определение модели Connection для хранения связи между вакансиями и навыками
class Connection(Base):
    __tablename__ = 'connection'
    vacancy_id = Column(Integer, ForeignKey('vacancy.id'), primary_key=True)  # ID вакансии, внешний ключ, первичный ключ связи
    skill_id = Column(Integer, ForeignKey('skill.id'), primary_key=True)  # ID навыка, внешний ключ, первичный ключ связи
    __table_args__ = (UniqueConstraint('vacancy_id', 'skill_id', name='uix_connection'),)  # Уникальное ограничение для связи


engine = create_engine(f'sqlite:///{args.adressDataBase}')  # Убедитесь, что путь к файлу базы данных указан правильно
Base.metadata.create_all(bind=create_engine(f'sqlite:///{args.adressDataBase}'))

# Создаем сессию для работы с базой данных
Session = sessionmaker(bind=engine)
session = Session()

def insert_in_db(name, salary, work_time, experience, skills, address, education, description):
    try:
        # Создаем новый объект Vacancy
        new_vacancy = Vacancy(
            name=name,
            salary=salary,
            work_time=work_time,
            experience=experience,
            address=address,
            education=education,
            description=description
        )
        
        # Добавляем новый объект Vacancy в сессию
        session.add(new_vacancy)
        
        # Сохраняем изменения в базе данных
        session.commit()

        # Получаем ID последней добавленной вакансии
        vacancy_id = new_vacancy.id

        # Обрабатываем навыки
        for skill in skills:
            # Приводим навык к нижнему регистру перед проверкой на уникальность
            skill_lower = skill.lower()
            
            # Проверяем, существует ли навык в таблице Skill
            skill_exists = session.query(Skill).filter_by(name=skill_lower).first()
            if skill_exists:
                # Если навык существует, используем его ID
                skill_id = skill_exists.id
            else:
                # Если навык не существует, создаем новый объект Skill и сохраняем его
                new_skill = Skill(name=skill_lower)
                session.add(new_skill)
                session.commit()
                skill_id = new_skill.id

            # Проверяем, существует ли уже связь между вакансией и навыком
            connection_exists = session.query(Connection).filter_by(vacancy_id=vacancy_id, skill_id=skill_id).first()
            if not connection_exists:
                # Создаем новый объект Connection для связи между вакансией и навыком, если связи еще нет
                new_connection = Connection(vacancy_id=vacancy_id, skill_id=skill_id)
                session.add(new_connection)
        
        # Сохраняем изменения в базе данных
        session.commit()

        logging.info(f"Vacancy '{name}' has been added to the database with {len(skills)} skills.")
    except Exception as e:
        logging.error(f"Error inserting vacancy into database: {e}")


st_accept = "text/html" 
st_useragent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 YaBrowser/24.1.0.0 Safari/537.36"
headers = {
   "Accept": st_accept, # Что мы хотим от сервера/сайта
   "User-Agent": st_useragent
}
moviesList = []



def GetHTML(url):
    try:
        req = requests.get(url, headers)
        # считываем текст HTML-документа
        src = req.text
        logging.info(f"Successfully fetched HTML for URL: {url}")
        return src
    except Exception as e:
        logging.error(f"Failed to fetch HTML for URL: {url}. Error: {e}")

def GetPage(codeHTML, url):
    try:
        soup = BeautifulSoup(codeHTML, 'html.parser')
        navElement = soup.find('div', class_='pagination')
        links = navElement.find_all('a', class_='pagination-list__item')
        page_links = [(link.text.strip(), url + link['href']) for link in links]
        logging.info(f"Successfully parsed page links from HTML.")
        return page_links
    except Exception as e:
        logging.error(f"Failed to parse page links from HTML. Error: {e}")


def GetInfoFromPage(page):
    try:
        html = GetHTML(page[1])
        soup = BeautifulSoup(html, 'html.parser')
        # Находим элемент с названием вакансии
        title_elements = soup.find_all('a', class_='vacancy-preview-card__title_border') #</a>, <a class="vacancy-preview-card__title_border" href="/vacancy/49904730/" target="_blank">
        vacancies_list = get_vacancies_and_href(title_elements)
        return vacancies_list
    except Exception as e:
        logging.error(f"Failed to get info from page. Error: {e}")

def get_vacancies_and_href(vacancy_elements):
    try:
        vacancies_list = []
        for element in vacancy_elements:
            title = element.text.strip()  # Получаем текст элемента (название вакансии)
            link = element['href']  # Получаем атрибут href (ссылка на вакансию)
            vacancies_list.append({'name': title, 'link': f"{args.urlParsSite}{link}"})  # Добавляем словарь с информацией о вакансии в список
        return vacancies_list
    except Exception as e:
        logging.error(f"Failed to get vacancies and href from elements. Error: {e}")


def get_info_about_vacancy(name, link):
    try:
        html = GetHTML(link)
        soup = BeautifulSoup(html, 'html.parser')
        salary_info_element = soup.find('h3', itemprop='baseSalary')
        if salary_info_element:
            salary_info = salary_info_element.text
            salary_range = re.sub(r'&nbsp;', ' ', salary_info)
            formatted_salary_range = re.sub(r'(\d{3})\s', r'\1 ', salary_range).replace(' ', '—').strip()
            salary = formatted_salary_range
        else:
            salary = 'not found'
        
        work_time_element = soup.find('div', itemprop='workHours')
        if work_time_element:
            work_time = work_time_element.text.strip()
            work_time = work_time_element.text.strip()
        else:
            work_time = 'not found'
        
        experience_element = soup.find('div', itemprop='experienceRequirements')
        if experience_element:
            experience = experience_element.text.strip()
            experience = 'not found'
        
        skills_element = soup.find('div', class_='vacancy-card__skills-list')
        if skills_element:
            skills = [skill.text.strip() for skill in skills_element.find_all('div', class_='vacancy-card__skills-item')]
        else:
            skills = ['not found']
        
        address_element = soup.find('div', class_='vacancy-locations__address')
        if address_element:
            address = address_element.text.strip()
        else:
            address = 'not found'
        
        description = soup.find('div', itemprop='description')
        if description:
            description = description.text.strip()
        else:
            description = 'not found'
        
        education_element = soup.find('div', itemprop='educationRequirements')
        if education_element:
            education = education_element.text.strip()
        else:
            education = 'not found'
        
        # запись в базу данных
        insert_in_db(name, salary, work_time, experience, skills, address, education, description)
    except Exception as e:
        pass



if __name__ == "__main__":
    try:
        url = args.urlParsSite
        mainHtml = GetHTML(url)
        pages = GetPage(mainHtml, url)
        for page in pages:
            try:
                vacancies_list = GetInfoFromPage(page)
                for vacancy in vacancies_list:
                    try:
                        name = vacancy['name']
                        link = vacancy['link']
                        get_info_about_vacancy(name, link)
                    except Exception as e:
                        logging.error(f"Error occurred while getting info about vacancy '{name}': {e}")
            except Exception as e:
                logging.error(f"Error occurred while processing page: {e}")
    except Exception as e:
        logging.error(f"Error occurred while fetching main HTML or processing pages: {e}")

 