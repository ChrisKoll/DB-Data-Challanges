import pandas as pd
import json


class DatasetHandler:

    def __init__(self):
        self.csv_data = None
        self.json_data = {}

    def get_csv(self, csv_file, separator):
        with open(csv_file, 'r', encoding='utf-8'):
            self.csv_data = pd.read_csv(csv_file, sep=separator)

    def create_json(self):
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

    def save_json(self, name):
        with open(name + '.json', 'w', encoding='utf-8') as out_file:
            json.dump(self.json_data, out_file, indent=4)

    def append_json(self, file1, file2):
        with open(file1, 'r', encoding='utf-8') as in_file1:
            json1 = json.load(in_file1)

        with open(file2, 'r', encoding='utf-8') as in_file2:
            json2 = json.load(in_file2)

        self.json_data = {**json1, **json2}

        self.save_json("Combined")

    @staticmethod
    def remove_none(json_file):
        with open(json_file, 'r', encoding='utf-8') as in_file:
            json_data = json.load(in_file)

            for entry in json_data.keys():
                for value in json_data[entry]['Geo'].keys():
                    if json_data[entry]['Geo'][value] == 'None':
                        json_data[entry]['Geo'][value] = None

        with open(json_file, 'w', encoding='utf-8') as out_file:
            json.dump(json_data, out_file, indent=4, ensure_ascii=False)


def main():
    csv_1 = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/15.06.2022/tweets_15-06-2022_21-48-general.csv"
    csv_2 = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/15.06.2022/tweets_15-06-2022_22-03-nine2.csv"
    csv_3 = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/22.06.2022/tweets_22-06-2022_17-16_general.csv"
    csv_4 = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/22.06.2022/tweets_22-06-2022_17-08_nine.csv"
    csv_5 = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/29.06.2022/tweets_29-06-2022_16-28_general.csv"
    csv_6 = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/29.06.2022/tweets_29-06-2022_16-22_nine.csv"
    csv_7 = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/06.07.2022/tweets_06-07-2022_15-16_general.csv"
    csv_8 = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/06.07.2022/tweets_06-07-2022_15-07_nine.csv"

    json_1 = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/tweets_15-06-2022_general.json"
    json_2 = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/tweets_22-06-2022_general.json"
    json_3 = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/tweets_29-06-2022_general.json"
    json_4 = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/tweets_06-07-2022_general.json"
    json_5 = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/tweets_21-07-2022_16-05_general.json"
    json_6 = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/tweets_15-06-2022_nine.json"
    json_7 = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/tweets_22-06-2022_nine.json"
    json_8 = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/tweets_29-06-2022_nine.json"
    json_9 = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/tweets_06-07-2022_nine.json"
    json_10 = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/tweets_21-07-2022_16-08_nine.json"

    new_handler = DatasetHandler()
    # new_handler.get_csv(csv_8, '$')
    # new_handler.create_json()
    # new_handler.save_json("tweets_06-07-2022_nine")
    # new_handler.append_json(json_1, json_2)
    new_handler.remove_none("/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/Nine/tweets_15-06_21-07_nine.json")


if __name__ == '__main__':
    main()
