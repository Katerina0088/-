from ParsingFilms import Tag, Film, Connection
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
import yaml
from appsettings import SettingsParser

def get_average_rating_by_year_and_tag():
    
    engine = create_engine('sqlite:///films.db')
    Session = sessionmaker(bind=engine)
    session = Session()

    query = session.query(Tag.name, Film.year, func.avg(Film.rating).label('average_rating'))\
  .join(Connection, Film.id == Connection.film_id)\
  .join(Tag, Connection.tag_id == Tag.id)\
  .group_by(Tag.name, Film.year)\
  .order_by(Tag.name, Film.year)\
  .all()

    df = pd.DataFrame(query, columns=['Tag', 'Year', 'Average Rating'])

    session.close()

    return df

def plot_average_rating_over_years():
    df = get_average_rating_by_year_and_tag()

    # Plotting
    for tag in df['Tag'].unique():
        plt.figure(figsize=(10, 6))
        tag_data = df[df['Tag'] == tag]
        plt.plot(tag_data['Year'], tag_data['Average Rating'], label=tag)

        plt.title(f'Средний рейтинг по годам: {tag}')
        plt.xlabel('Год')
        plt.ylabel('Средний рейтинг')
        plt.legend()
        plt.show()

# Example usage
if __name__ == "__main__":
    plot_average_rating_over_years()