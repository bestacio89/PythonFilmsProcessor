import pandas as pd
from pymongo import MongoClient

class DataProcessor:
    def __init__(self, db_name='cancer_db'):
        # Connect to MongoDB on localhost
        self.client = MongoClient('mongodb://localhost:27017/')
        # Access the specified database
        self.db = self.client[db_name]

    def load_data(self, collection_name):
        """Load data from MongoDB into pandas DataFrame."""
        collection = self.db[collection_name]
        return pd.DataFrame(list(collection.find()))

    def display_data(self, collection_name):
        """Display data from the specified collection."""
        df = self.load_data(collection_name)
        print(f"\n{collection_name.capitalize()} DataFrame:")
        print(df.head())  # Display the first few rows

    def describe_data(self, collection_name):
        """Display descriptive statistics of the collection."""
        df = self.load_data(collection_name)
        print(f"\nDescriptive Statistics of {collection_name.capitalize()}:")
        print(df.describe())

    def create_views(self):
        # Drop existing views if they exist
        views_to_drop = [
            'top_5_directors_most_films',
            'top_5_directors_rated',
            'top_5_directors_longest_avg_runtime',
            'top_15_actors_with_movies'
        ]

        for view in views_to_drop:
            try:
                self.db.command('drop', view)
                print(f"Dropped view: {view}")
            except Exception as e:
                print(f"Error dropping view {view}: {e}")

        # Create new views with updated pipelines
        self.db.command('create', 'top_5_directors_most_films',
                        viewOn='movies',
                        pipeline=[
                            {
                                "$group": {
                                    "_id": "$Director",  # Group by director's name
                                    "film_count": {"$sum": 1}  # Count the number of films
                                }
                            },
                            {
                                "$sort": {"film_count": -1}  # Sort by film count in descending order
                            },
                            {
                                "$limit": 5  # Limit to top 5 directors
                            }
                        ]
                        )

        self.db.command('create', 'top_5_directors_rated',
                        viewOn='movies',
                        pipeline=[
                            {
                                "$group": {
                                    "_id": "$Director",  # Group by director's name
                                    "average_rating": {"$avg": "$Rating"}  # Calculate average rating
                                }
                            },
                            {
                                "$sort": {"average_rating": -1}  # Sort by average rating in descending order
                            },
                            {
                                "$limit": 5  # Limit to top 5 rated directors
                            }
                        ]
                        )

        self.db.command('create', 'top_5_directors_longest_avg_runtime',
                        viewOn='movies',
                        pipeline=[
                            {
                                "$group": {
                                    "_id": "$Director",  # Group by director's name
                                    "average_runtime": {"$avg": "$Runtime"}  # Calculate average runtime
                                }
                            },
                            {
                                "$sort": {"average_runtime": -1}  # Sort by average runtime in descending order
                            },
                            {
                                "$limit": 5  # Limit to top 5 directors with longest average runtime
                            }
                        ]
                        )

        self.db.command('create', 'top_15_actors_with_movies',
                        viewOn='movies',
                        pipeline=[
                            {
                                "$project": {
                                    "Title": 1,  # Keep the movie title
                                    "Cast": {
                                        "$cond": {
                                            "if": {"$eq": [{"$type": "$Cast"}, "string"]},  # Check if Cast is a string
                                            "then": {"$split": ["$Cast", "|"]},  # Split if it's a string
                                            "else": []  # Return an empty array if it's not a string
                                        }
                                    }
                                }
                            },
                            {
                                "$unwind": "$Cast"  # Flatten the Cast array
                            },
                            {
                                "$group": {
                                    "_id": "$Cast",  # Group by actor's name
                                    "movies": {"$push": "$Title"},  # Collect movies for each actor
                                    "film_count": {"$sum": 1}  # Count the number of films
                                }
                            },
                            {
                                "$sort": {"film_count": -1}  # Sort by film count in descending order
                            },
                            {
                                "$limit": 15  # Limit to top 15 actors
                            }
                        ]
                        )

        print("Views created successfully.")

    def get_top_rated_directors(self):
        return pd.DataFrame(list(self.db.top_rated_directors.find()))

    def get_longest_average_runtime_directors(self):
        return pd.DataFrame(list(self.db.longest_average_runtime_directors.find()))

    def get_top_directors_by_film_count(self):
        return pd.DataFrame(list(self.db.top_directors_by_film_count.find()))

    def get_directors_with_most_movies(self):
        return pd.DataFrame(list(self.db.directors_with_most_movies.find()))





