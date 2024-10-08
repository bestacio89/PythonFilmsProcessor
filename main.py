from app.persistence.data_processor import DataProcessor
from app.persistence.database import Database
from app.utilities.data_visualizer import DataVisualizer


def main():
 
    # Path to your CSV file
    csv_file = 'data/movies.csv'

    # Create an instance of Database with the desired database name
    db_name = 'movies'  # You can change this as needed
    db = Database(db_name=db_name)

    # Load data from the CSV file
    movie_data = db.load_data_from_csv(csv_file)

    # Setup the database and insert movie data into the 'movies' collection
    # The unique key here is 'IMDB ID'
    db.setup_database(movie_data, collection_name='movies', unique_key='IMDB ID')

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

   #Plots for the Aggregations
    visualizer = DataVisualizer('movies')  # Replace with your database name
    visualizer.plot_top_5_directors_most_films()
    visualizer.plot_top_5_directors_rated()
    visualizer.plot_director_average_runtime()
    visualizer.plot_top_15_actors_with_movies()




if __name__ == "__main__":
    main()
