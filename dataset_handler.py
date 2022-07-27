import json
import pandas as pd


class DatasetHandler:
    """
    Transforms Tweet csv file to JSON file.
    """

    def __init__(self):
        """
        Constructor.
        """

        self.csv_data = None
        self.json_data = {}

    def get_csv(self, csv_file: str, separator: str):
        """
        Reads csv file.

        :param csv_file: Path to csv file
        :param separator: Seperator for csv file
        """

        with open(csv_file, 'r', encoding='utf-8'):
            self.csv_data = pd.read_csv(csv_file, sep=separator)

    def create_json(self):
        """
        Creates JSON dict from dataframe.
        """

        for index, row in self.csv_data.iterrows():
            data = {'Id': row['tweet.id'], 'Created_At': row['tweet.created_at'], 'Text': row['tweet.text'],
                    'Tweet_Source': row['tweet.source'], 'Retweet_Count': row['tweet.retweet_count'],
                    'Reply_Count': row['tweet.reply_count'], 'Like_Count': row['tweet.like_count'],
                    'Quote_Count': row['tweet.quote_count'], 'Language': row['tweet.lang']}

            user = {'Id': row['user.id'], 'Name': row['user.name'], 'Location': row['user.location'],
                    'Created_At': row['user.created_at']}

            geo = {'Id': row['place.id'], 'Name': row['place.name'], 'Country_Code': row['place.country_code'],
                   'Geo': row['place.geo'], 'Type': row['place.place_type']}

            hashtags = row['tweet.hashtags']

            self.json_data[row['tweet.id']] = {'Data': data, 'User': user, 'Geo': geo, 'Hashtags': hashtags}

    def save_json(self, name: str):
        """
        Saves dict as JSON object.

        :param name: File name
        """

        with open(name + '.json', 'w', encoding='utf-8') as out_file:
            json.dump(self.json_data, out_file, indent=4)

    def append_json(self, file1: str, file2: str, file_name: str):
        """
        Merges tow JSON files.

        :param file1: JSON file 1
        :param file2: JSON file 2
        :param file_name: File name for new file
        """

        with open(file1, 'r', encoding='utf-8') as in_file1:
            json1 = json.load(in_file1)

        with open(file2, 'r', encoding='utf-8') as in_file2:
            json2 = json.load(in_file2)

        self.json_data = {**json1, **json2}

        self.save_json("Combined")

    @staticmethod
    def remove_none(json_file):
        """
        Replaces the "None" with None in JSON dict.

        :param json_file: Path to Geo Tweet data
        """

        with open(json_file, 'r', encoding='utf-8') as in_file:
            json_data = json.load(in_file)

            for entry in json_data.keys():
                for value in json_data[entry]['Geo'].keys():
                    if json_data[entry]['Geo'][value] == 'None':
                        json_data[entry]['Geo'][value] = None

        with open(json_file, 'w', encoding='utf-8') as out_file:
            json.dump(json_data, out_file, indent=4, ensure_ascii=False)
