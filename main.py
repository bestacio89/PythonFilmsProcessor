from app.persistence.data_processor import DataProcessor
from app.persistence.database import Database

def main():
    # Path to your CSV file
    csv_file = 'data/movies.csv'

    # Create an instance of Database with the desired database name
    db_name = 'movies'  # You can change this as needed
    db = Database(db_name=db_name)

    # Load data from the CSV file
    movie_data = db.load_data_from_csv(csv_file)

    # Setup the database and insert movie data into the 'movies' collection
    # The unique key here could be 'movie_id' or any unique identifier for movies
    db.setup_database(movie_data, collection_name='movies', unique_key='movie_id')

    # Optionally, you can verify by loading the data back into a pandas DataFrame
    data_df = db.load_data('movies')
    print("Loaded data from MongoDB:")
    print(data_df.head())  # Display the first few rows

    # Create an instance of DataProcessor for movie dataset
    data_processor = DataProcessor(db_name=db_name)

    # Example: Display data from the movies collection
    data_processor.display_data('movies')

    # Example: Show descriptive statistics
    data_processor.describe_data('movies')

    # Aggregation Queries

    # 1. Top 5 rated directors
    print("\nTop 5 Rated Directors:")
    top_rated_directors = data_processor.get_top_rated_directors('movies')
    for director in top_rated_directors:
        print(director)

    # 2. Directors with the longest average movie length
    print("\nDirectors with the Longest Average Movie Length:")
    longest_average_length = data_processor.get_directors_with_longest_average_length('movies')
    for director in longest_average_length:
        print(director)

    # 3. Top 5 Directors with the most films made
    print("\nTop 5 Directors with the Most Films Made:")
    most_films_directors = data_processor.get_top_directors_by_film_count('movies')
    for director in most_films_directors:
        print(director)

    # 4. Top 15 Most Present Actors with Their Movies
    print("\nTop 15 Most Present Actors with Their Movies:")
    most_present_actors = data_processor.get_top_present_actors('movies')
    for actor in most_present_actors:
        print(actor)

if __name__ == "__main__":
    main()
