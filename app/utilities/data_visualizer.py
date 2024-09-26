import matplotlib.pyplot as plt
import pandas as pd
from pymongo import MongoClient

class DataVisualizer:
    def __init__(self, db_name):
        self.client = MongoClient()  # Adjust connection parameters as needed
        self.db = self.client[db_name]

    def fetch_data(self, view_name):
        """Fetch data from the specified view."""
        return pd.DataFrame(list(self.db[view_name].find()))

    def plot_top_5_directors_most_films(self):
        data = self.fetch_data('top_5_directors_most_films')
        plt.figure(figsize=(10, 8))
        plt.pie(data['film_count'], labels=data['_id'], autopct='%1.1f%%', startangle=140)
        plt.title('Top 5 Directors by Number of Films')
        plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
        plt.show()

    def plot_top_5_directors_rated(self):
        data = self.fetch_data('top_5_directors_rated')
        plt.figure(figsize=(10, 6))
        plt.barh(data['_id'], data['average_rating'], color='salmon')
        plt.title('Top 5 Directors by Average Rating')
        plt.xlabel('Average Rating')
        plt.ylabel('Directors')
        plt.grid(axis='x', linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.show()

    def plot_director_average_runtime(self):
        data = self.fetch_data('top_5_directors_longest_avg_runtime')  # Adjust as needed
        plt.figure(figsize=(10, 6))
        plt.barh(data['_id'], data['average_runtime'], color='skyblue')
        plt.title('Average Runtime by Director')
        plt.xlabel('Average Runtime (minutes)')
        plt.ylabel('Directors')
        plt.grid(axis='x', linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.show()
    def plot_top_15_actors_with_movies(self):
        data = self.fetch_data('top_15_actors_with_movies')
        plt.figure(figsize=(10, 6))
        plt.scatter(data['_id'], data['film_count'], color='lightcoral', s=100)
        plt.title('Top 15 Actors by Number of Movies')
        plt.xlabel('Actors')
        plt.ylabel('Number of Movies')
        plt.xticks(rotation=45)
        plt.grid(True)
        plt.tight_layout()
        plt.show()

# Example usage
if __name__ == "__main__":
    visualizer = DataVisualizer('movies')  # Replace with your database name
    visualizer.plot_top_5_directors_most_films()
    visualizer.plot_top_5_directors_rated()
    visualizer.plot_director_average_runtime()
    visualizer.plot_top_15_actors_with_movies()
