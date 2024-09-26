from pymongo import MongoClient
import pandas as pd
import json

from app.persistence.data_quality import DataQualityChecker


class Database:
    def __init__(self, db_name, uri='mongodb://localhost:27017/'):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]

    def create_collection(self, collection_name):
        """Create collection dynamically."""
        if collection_name not in self.db.list_collection_names():
            self.db.create_collection(collection_name)
            print(f"{collection_name.capitalize()} collection created.")

    def clean_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Clean the DataFrame based on quality checks."""
        checker = DataQualityChecker(data)
        cleaned_data = checker.clean_data()
        return cleaned_data

    def insert_data(self, data, collection_name, unique_key):
        """Insert data into the specified collection while avoiding duplicates."""
        collection = self.db[collection_name]

        for item in data:
            # Use the correct unique key based on the collection
            if collection_name == 'movies':
                unique_value = item.get('imdb')
            elif collection_name == 'directors':
                unique_value = item.get('name')
            else:
                print(f"Unknown collection: {collection_name}")
                continue

            if not self.item_exists(collection, item, unique_key):
                collection.insert_one(item)
                print(f"Inserted into {collection_name}: {unique_value}")
            else:
                print(f"Item already exists in {collection_name}: {unique_value}")

    def item_exists(self, collection, item, unique_key):
        """Check if an item already exists in the collection."""
        return collection.find_one({unique_key: item[unique_key]}) is not None

    def load_data_from_json(self, json_file):
        """Load data from a JSON file."""
        with open(json_file, 'r', encoding='utf-8') as file:
            data = json.load(file)
        return data

    def load_data_from_csv(self, csv_file):
        """Load data from a CSV file using pandas."""
        return pd.read_csv(csv_file).to_dict(orient='records')

    def setup_database(self, data, collection_name, unique_key):
        """Setup the database and insert data into the specified collection while avoiding duplicates."""
        # Ensure data is a DataFrame
        if isinstance(data, list):
            data = pd.DataFrame(data)

        # Clean the data
        cleaned_data = self.clean_data(data)

        # Insert movie data
        self.insert_data(cleaned_data.to_dict(orient='records'), collection_name, unique_key)

        # Insert directors into the 'directors' collection if applicable
        if collection_name == 'movies':
            directors = set()
            for item in cleaned_data.to_dict(orient='records'):
                directors.add(item['Director'])  # Collect unique director names

            # Prepare data for directors collection
            director_data = [{'name': director} for director in directors if director is not None]
            self.insert_data(director_data, collection_name='directors', unique_key='name')

    def load_data(self, collection_name):
        """Load data from a collection into a pandas DataFrame."""
        collection = self.db[collection_name]
        return pd.DataFrame(list(collection.find()))
