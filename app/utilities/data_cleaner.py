import pandas as pd
from mongoengine import connect, ValidationError
from typing import List

from app.schemas.director import Director
from app.schemas.movie import Movie


class MovieDataCleaner:
    def __init__(self, filepath, mongo_uri, db_name):
        self.filepath = filepath
        connect(db_name, host=mongo_uri)  # Connect to MongoDB
        self.df = pd.read_csv(self.filepath)

    def clean_data(self):
        # Clean data as before...
        self.df.columns = self.df.columns.str.strip()
        critical_columns = ['Title', 'Year', 'IMDB ID']
        self.df = self.df.dropna(subset=critical_columns)
        self.df['Year'] = self.df['Year'].astype(int, errors='raise')
        self.df = self.df.drop_duplicates(subset=['IMDB ID'])
        # Clean directors, writers, and cast
        self.df['Directors'] = self.df['Director'].str.split(',').apply(lambda x: [d.strip() for d in x])
        self.df['Writers'] = self.df['Writers'].str.split(',').apply(lambda x: [w.strip() for w in x])
        self.df['Cast'] = self.df['Cast'].str.split(',').apply(lambda x: [c.strip() for c in x])
        self.df['Directors'] = self.df['Directors'].apply(lambda x: list(set(x)))
        return self.df

    def save_cleaned_data(self, output_filepath):
        cleaned_df = self.clean_data()
        cleaned_df.to_csv(output_filepath, index=False)
        print(f"Cleaned data saved to {output_filepath}")

        # Insert movies and directors into MongoDB
        for index, row in cleaned_df.iterrows():
            # Prepare movie data
            movie = Movie(
                title=row['Title'],
                year=row['Year'],
                summary=row['Summary'],
                short_summary=row['Short Summary'],
                imdb_id=row['IMDB ID'],
                runtime=row['Runtime'],
                youtube_trailer=row['YouTube Trailer'],
                rating=row['Rating'],
                movie_poster=row['Movie Poster'],
                directors=row['Directors'],
                writers=row['Writers'],
                cast=row['Cast']
            )

            # Insert the movie, catching validation errors
            try:
                movie.save()
                print(f"Inserted movie: {movie.title}")
            except ValidationError as e:
                print(f"Error inserting movie {movie.title}: {e}")

            # Insert directors if they do not exist
            for director_name in row['Directors']:
                try:
                    director = Director(name=director_name)
                    director.save()  # Will only save if unique
                    print(f"Inserted new director: {director.name}")
                except ValidationError:
                    print(f"Director {director_name} already exists or is invalid.")
