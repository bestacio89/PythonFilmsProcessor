from mongoengine import Document, StringField, ListField, IntField, FloatField, URLField, connect, ValidationError

class Director(Document):
    name = StringField(required=True, unique=True, max_length=100)

class Movie(Document):
    title = StringField(required=True)
    year = IntField(required=True)
    summary = StringField(required=True)
    short_summary = StringField()
    imdb_id = StringField(required=True, unique=True)
    runtime = IntField(required=True)
    youtube_trailer = URLField()
    rating = FloatField(required=True, min_value=0, max_value=10)  # Ensures rating is between 0 and 10
    movie_poster = URLField()
    directors = ListField(StringField(), required=True)  # List of director names
    writers = ListField(StringField())  # List of writer names
    cast = ListField(StringField())  # List of cast names
