import json
from json.decoder import JSONDecodeError


class Database:
    """
    Creates database files for given Twitter Data.
    """

    def __init__(self, database_file: str):
        """
        Constructor.

        :param database_file: Path to existing database file
        """

        self.database_file = database_file
        self.database = self.get_database()
        self.new_data = None

    def get_database(self):
        """
        Opens the database file and extracts JSON information if included.
        """

        with open(self.database_file, 'r') as in_file:
            try:
                database = json.load(in_file)
            except JSONDecodeError:
                database = {}

        return database

    def get_new_data(self, new_data_file: str):
        """
        Extracts the new data for updating from JSON file.

        :param new_data_file: Path to data file
        """

        with open(new_data_file, 'r') as in_file:
            self.new_data = json.load(in_file)

    def add_entry(self, user: dict, tweet_id: int, tweet_text: str):
        """
        Adds a new entry to the database.

        :param user: Contains the user dict
        :param tweet_id: Tweet id
        :param tweet_text: Tweet text
        """

        self.database[user['Id']] = {"User_Data": {}}
        self.database[user['Id']]['User_Data']['Id'] = user['Id']
        self.database[user['Id']]['User_Data']['Name'] = user['Name']
        self.database[user['Id']]['User_Data']['Location'] = user['Location']
        self.database[user['Id']]['User_Data']['Created_At'] = user['Created_At']

        self.database[user['Id']]['Tweets'] = [{"Tweet_Id": tweet_id, "Text": tweet_text}]

        self.database[user['Id']]['Count'] = len(self.database[user['Id']]['Tweets'])

    def update_entry(self, user_id, tweet_id, tweet_text):
        """
        Updates the entry of an existing user.

        :param user_id: User id
        :param tweet_id: Tweet id
        :param tweet_text: Tweet text
        """

        self.database[user_id]['Tweets'].append({"Tweet_Id": tweet_id, "Text": tweet_text})
        self.database[user_id]['Count'] = len(self.database[user_id]['Tweets'])

    def update_database(self):
        """
        Method for updating the whole database file.
        """

        # Iterate through new data
        for tweet in self.new_data:
            entry = self.new_data[tweet]
            # Check if user entry exists
            if entry['User']['Id'] in self.database:
                self.update_entry(entry['User']['Id'], entry['Data']['Id'], entry['Data']['Text'])
            else:
                self.add_entry(entry['User'], entry['Data']['Id'], entry['Data']['Text'])

    def save_database(self):
        """
        Saves the database file.
        """

        with open(self.database_file, 'w', encoding='utf-8') as out_file:
            json.dump(self.database, out_file, ensure_ascii=False, indent=4, default=str)
