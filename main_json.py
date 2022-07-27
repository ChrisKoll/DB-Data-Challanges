from download_handler import DownloadHandler
from sentiment_analysis import SentimentAnalyser
from user_analysis import Database
from tweet_mapper import TweetMapper
from dataset_handler import DatasetHandler


def download_tweets_json(config_file: str, query: str):
    download_handler = DownloadHandler()
    download_handler.read_config_file(config_file)
    download_handler.create_api_interface()
    download_handler.get_tweets_json(query, 12000)
    download_handler.save_tweets_json()


def sentiment_analysis(tweet_data: str):
    analyser = SentimentAnalyser(tweet_data)
    analyser.sentiment_analysis()
    analyser.save_json()


def create_user_database(database_file: str, new_data: str):
    database = Database(database_file)
    database.get_new_data(new_data)
    database.update_database()
    database.save_database()


def transform_csv(csv_file: str, separator: str, file_name: str):
    handler = DatasetHandler()
    handler.get_csv(csv_file, separator)
    handler.create_json()
    handler.save_json(file_name)


def merge_json(json1: str, json2: str, file_name: str):
    handler = DatasetHandler()
    handler.append_json(json1, json2, file_name)

    handler.remove_none(file_name + ".json")


def extract_geo_tweets(mapper: TweetMapper, locations: str):
    mapper.extract_geo_tweets()
    mapper.extract_locations(locations)


def add_locations(mapper: TweetMapper, locations: str):
    mapper.get_locations(locations)
    mapper.add_locations()


def plot_distribution(mapper: TweetMapper, geo_tweets: str, file_name: str):
    mapper.plot_geo_data(geo_tweets, file_name)


def create_relationship_graph(mapper: TweetMapper, csv_file: str, separator: str):
    mapper.plot_new_data(csv_file, separator)


def main():
    """
    config_file = "Data/config.ini"

    query_nine_euro = "(#9EuroTicket OR #9EuroTickets OR #NeunEuroTicket OR #NeunEuroTickets OR neun-euro-ticket OR" \
                      " neun-euro-tickets OR (9 euro ticket) OR (9 euro tickets)) (lang:en OR lang:de) -RT"
    # query_db_general = "((@DB_Bahn OR @DB_Info OR @DB_Presse OR (deutsche bahn) OR #DeutscheBahn OR #DBNavigator) " \
    #                    "-(RT OR #9EuroTicket OR #9EuroTickets OR #NeunEuroTicket OR #NeunEuroTickets OR #9euro OR " \
    #                    "neun-euro OR (9 euro) OR 9€)) (lang:de OR lang:en)"
    download_tweets_json(config_file, query_nine_euro)

    tweet_data = "/Data/Nine/tweets_15-06_21-07_nine.json"
    sentiment_analysis(tweet_data)

    database_file = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/Nine/geo_database_nine.json"
    new_data = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/Nine/geo_tweets_15-06_21-07_nine.json"
    create_user_database(database_file, new_data)

    csv = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/15.06.2022/tweets_15-06-2022_21-48-general.csv"
    transform_csv(csv, '$', "tweets_06-07-2022_nine")

    json1 = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/tweets_15-06-2022_general.json"
    json2 = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/tweets_22-06-2022_general.json"
    merge_json(json1, json2, "json1_json2_combined")

    json_file = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/tweets_15-06-2022_general.json"
    geo_file = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/Nine/geo_tweets_15-06_21-07_nine.json"

    new_mapper = TweetMapper(json_file, geo_file)

    locations = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/location_database.json"
    extract_geo_tweets(new_mapper, locations)

    add_locations(new_mapper, locations)

    plot_distribution(new_mapper, geo_file, "germany_distribution")

    csv_file = "/home/ubuntu/Projects/DeutscheBahnDataChallanges/Data/9euro-annotation.csv"
    create_relationship_graph(new_mapper, csv_file, '$')
    """


if __name__ == '__main__':
    main()
