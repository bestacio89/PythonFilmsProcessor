import pandas as pd
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DataQualityChecker:
    def __init__(self, data: pd.DataFrame):
        """Initialize with the DataFrame that will be checked for quality."""
        self.data = data
        logging.info("DataQualityChecker initialized with data.")

    def check_missing_values(self) -> pd.DataFrame:
        """Return DataFrame with missing values removed."""
        missing_count = self.data.isnull().sum().sum()
        if missing_count > 0:
            logging.warning(f"Missing values found: {missing_count}. Removing rows with missing values.")
        return self.data.dropna()

    def check_valid_year(self) -> pd.DataFrame:
        """Check if 'Year' is a valid integer within a reasonable range."""
        if 'Year' in self.data.columns:
            valid_year_range = (1888, datetime.now().year)  # Movies start from 1888
            invalid_count = self.data[~self.data['Year'].between(valid_year_range[0], valid_year_range[1])].shape[0]
            if invalid_count > 0:
                logging.warning(f"Invalid years found: {invalid_count}. Removing these rows.")
                return self.data[self.data['Year'].between(valid_year_range[0], valid_year_range[1])]
        return self.data  # If 'Year' doesn't exist, return as is

    def check_imdb_id_format(self) -> pd.DataFrame:
        """Check if 'IMDB ID' follows the typical format (e.g., tt1234567)."""
        if 'IMDB ID' in self.data.columns:
            valid_imdb_format = self.data['IMDB ID'].str.match(r'^tt\d{7}$', na=False)
            invalid_count = self.data[~valid_imdb_format].shape[0]
            if invalid_count > 0:
                logging.warning(f"Invalid IMDB IDs found: {invalid_count}. Removing these rows.")
                return self.data[valid_imdb_format]
        return self.data  # If 'IMDB ID' doesn't exist, return as is

    def check_runtime_format(self) -> pd.DataFrame:
        """Check if 'Runtime' is a valid positive integer."""
        if 'Runtime' in self.data.columns:
            invalid_count = self.data[~self.data['Runtime'].apply(lambda x: isinstance(x, (int, float)) and x > 0)].shape[0]
            if invalid_count > 0:
                logging.warning(f"Invalid Runtime values found: {invalid_count}. Removing these rows.")
                return self.data[self.data['Runtime'].apply(lambda x: isinstance(x, (int, float)) and x > 0)]
        return self.data  # If 'Runtime' doesn't exist, return as is

    def check_rating_format(self) -> pd.DataFrame:
        """Check if 'Rating' is a valid float between 0 and 10."""
        if 'Rating' in self.data.columns:
            invalid_count = self.data[~self.data['Rating'].between(0, 10)].shape[0]
            if invalid_count > 0:
                logging.warning(f"Invalid Ratings found: {invalid_count}. Removing these rows.")
                return self.data[self.data['Rating'].between(0, 10)]
        return self.data  # If 'Rating' doesn't exist, return as is

    def clean_data(self) -> pd.DataFrame:
        """Clean the DataFrame based on defined quality checks and return only valid data."""
        # Start with the original data
        cleaned_data = self.data

        # Remove missing values
        cleaned_data = self.check_missing_values()

        # Check valid year
        cleaned_data = self.check_valid_year()

        # Check IMDB ID format
        cleaned_data = self.check_imdb_id_format()

        # Check runtime format
        cleaned_data = self.check_runtime_format()

        # Check rating format
        cleaned_data = self.check_rating_format()

        logging.info("Data cleaning completed.")
        return cleaned_data


# Example usage
if __name__ == "__main__":
    # Example DataFrame creation
    data = pd.DataFrame({
        'Title': ['Movie A', 'Movie B', 'Movie C', None],
        'Year': [2020, 1990, 1880, 2023],
        'Summary': ['Summary A', 'Summary B', 'Summary C', 'Summary D'],
        'Short Summary': ['Short A', 'Short B', 'Short C', 'Short D'],
        'IMDB ID': ['tt1234567', 'tt7654321', 'invalid-id', 'tt1111111'],
        'Runtime': [120, 90, -100, 150],
        'YouTube Trailer': ['link1', 'link2', 'link3', 'link4'],
        'Rating': [8.5, 9.0, 12.0, 7.0],
        'Movie Poster': ['poster1', 'poster2', 'poster3', 'poster4'],
        'Director': ['Director A', 'Director B', None, 'Director D'],
        'Writers': ['Writer A', 'Writer B', 'Writer C', 'Writer D'],
        'Cast': ['Cast A', 'Cast B', 'Cast C', 'Cast D']
    })

    # Initialize and clean data
    checker = DataQualityChecker(data)
    cleaned_data = checker.clean_data()

    # Display cleaned data
    print(cleaned_data)
