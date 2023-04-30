import pandas as pd
import json
from kafka import KafkaProducer
from jsonschema import validate

# Define the schema for each row in the CSV dataset.
schema = {
    "type": "object",
    "properties": {
        "user_name": {"type": "string"},
        "user_location": {"type": "string"},
        "user_description": {"type": "string"},
                "user_created": {"type": "string"},
        "user_followers": {"type": "string"},
        "user_friends": {"type": "string"},
                "user_favourites": {"type": "string"},
        "user_verified": {"type": "string"},
        "date": {"type": "string"},
                "text": {"type": "string"},
        "hashtags": {"type": "array"},
        "source": {"type": "string"},
                "is_retweet": {"type": "string"}

    }
}

# Load the CSV file into a Pandas dataframe.
df = pd.read_csv('bitcoin_tweets_dataset.csv',error_bad_lines=False)

# Convert each row in the dataframe to a JSON string and validate it against the schema.
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
for index, row in df.iterrows():
    json_string = json.dumps(row.to_dict())
    #validate(instance=json.loads(json_string), schema=schema)
    producer.send('mytopic', json_string.encode('utf-8'))
