import tweepy
from tweepy.parsers import JSONParser
import configparser
import json
from datetime import datetime


class DownloadHandler:
    """
    Creates API access and downloads a given number of Tweets if available.
    """

    def __init__(self):
        """
        Constructor.
        """
        # Authentication attributes
        self.api_key = None
        self.api_key_secret = None
        self.access_token = None
        self.access_token_secret = None
        self.bearer_token = None
        self.api = None
        # Attributes
        self.available = False
        self.tweet_ids = []
        self.tweet_batches = []
        self.tweet_data = {}

    def read_config_file(self, path_to_file: str):
        """
        Reads the config file and saves the information in the attributes.

        :param path_to_file: Path to config file --> contains authentication keys .
        """
        config = configparser.RawConfigParser()
        config.read(path_to_file)

        # Assign values
        self.api_key = config["twitter"]["api_key"]
        self.api_key_secret = config["twitter"]["api_key_secret"]
        self.access_token = config["twitter"]["access_token"]
        self.access_token_secret = config["twitter"]["access_token_secret"]
        self.bearer_token = config["twitter"]["bearer_token"]

    def create_api_interface(self):
        """
        Uses authentication details to create an API interface.
        """
        # Authentication
        authentication = tweepy.OAuthHandler(self.api_key, self.api_key_secret)
        authentication.set_access_token(self.access_token, self.access_token_secret)

        # Create API interface
        self.api = tweepy.API(authentication, parser=tweepy.parsers.JSONParser())

    def check_available(self, client, query):
        """
        Checks if new tweets are available.

        :param client: Contains the client used for Twitter access
        :param query: Search query
        """

        # Finds all available tweets for the last 7 days
        available_tweets = client.get_recent_tweets_count(query=query, granularity="day")

        # Adds all day count values up to one week count value
        week_count = 0
        for daily_count in available_tweets.data:
            week_count += daily_count["tweet_count"]

        # Checks for tweet availability
        if week_count != 0:
            print("Tweets uploaded in last 7 days:", week_count)
            self.available = True
        else:
            print("There are no Tweets available...")
            self.available = False

    def remove_duplicates(self, response):
        """
        Removes the duplicate Tweets that were pulled from Twitter.

        :param response: Contains all Tweets pulled from Twitter.
        """

        # Extract Tweet ids
        self.tweet_ids = []
        for tweet in response:
            self.tweet_ids.append(tweet.id)

        # Removes duplicates by putting the Tweets into a set.
        # --> Only unique values
        print("Number of Tweets before deduplication:", len(self.tweet_ids))
        self.tweet_ids = [*{*self.tweet_ids}]
        print("Number of Tweets after deduplication:", len(self.tweet_ids))

    def create_batches(self):
        """
        Creates batches of size 100 as input for the tweepy.get_tweets() function.
        """

        # Iterating through all Tweets.
        # --> Constructing batches of size 100
        batch = []
        for i in range(1, len(self.tweet_ids) + 1):
            batch.append(self.tweet_ids[i - 1])
            if i % 100 == 0:
                self.tweet_batches.append(batch)
                batch = []

        # Append last batch < 100
        self.tweet_batches.append(batch)

    def get_tweets(self, query, verbose, batch_size):
        """
        Method to download the recent tweets.
        """

        # Create client with bearer token as authentication
        client = tweepy.Client(bearer_token=self.bearer_token)

        # Check for available Tweets
        self.check_available(client, query)

        # If Tweets are available
        if self.available:
            # Retrieve Tweets for given query
            response = tweepy.Paginator(client.search_recent_tweets, query=query, max_results=100). \
                flatten(limit=batch_size)

            # Remove duplicate Tweets
            self.remove_duplicates(response)
            # Create batches for further processing
            self.create_batches()

            # Information that needs to be extracted from the Tweets
            tweet_fields = ["id", "created_at", "text", "source", "public_metrics", "entities", "lang", "geo"]
            user_fields = ["id", "name", "location", "created_at"]
            place_fields = ["place_type", "geo", "id", "name", "country_code"]
            expansions = ["author_id", "geo.place_id"]
            # Get Tweets from Twitter by searching for their ID
            for batch in self.tweet_batches:
                response = client.get_tweets(ids=batch, tweet_fields=tweet_fields, user_fields=user_fields,
                                             place_fields=place_fields, expansions=expansions)

                # Create JSON object
                for tweet in response.data:
                    self.tweet_data[tweet.id] = {}

                    tweet_data = {"Id": tweet.id, "Created_At": tweet.created_at,
                                  "Text": tweet.text.strip().replace("\n", " "), "Tweet_Source": tweet.source,
                                  "Retweet_Count": tweet.public_metrics["retweet_count"],
                                  "Reply_Count": tweet.public_metrics["reply_count"],
                                  "Like_Count": tweet.public_metrics["like_count"],
                                  "Quote_Count": tweet.public_metrics["quote_count"], "Language": tweet.lang}
                    self.tweet_data[tweet.id]["Data"] = tweet_data

                    tweet_user = {}
                    for user in response.includes["users"]:
                        if tweet.author_id == user.id:
                            tweet_user["Id"] = user.id
                            tweet_user["Name"] = user.name
                            tweet_user["Location"] = user.location
                            tweet_user["Created_At"] = user.created_at

                    self.tweet_data[tweet.id]["User"] = tweet_user

                    tweet_place = {}
                    if tweet.geo:
                        for place in response.includes["places"]:
                            if tweet.geo["place_id"] == place.id:
                                tweet_place["Id"] = place.id
                                tweet_place["Name"] = place.name
                                tweet_place["Country_Code"] = place.country_code
                                tweet_place["Geo"] = place.geo
                                tweet_place["Type"] = place.place_type
                    else:
                        tweet_place["Id"] = None
                        tweet_place["Name"] = None
                        tweet_place["Country_Code"] = None
                        tweet_place["Geo"] = None
                        tweet_place["Type"] = None

                    self.tweet_data[tweet.id]["Geo"] = tweet_place

                    if tweet.entities and "hashtags" in tweet.entities:
                        tweet_hashtags = tweet.entities["hashtags"]
                        self.tweet_data[tweet.id]["Hashtags"] = tweet_hashtags
                    else:
                        self.tweet_data[tweet.id]["Hashtags"] = None
        else:
            print("No data can be extracted from Twitter - Try it again later...")

    def save_tweets(self):
        """
        Method to store input tweets in a csv file.
        """
        time = datetime.now().strftime("%d-%m-%Y_%H-%M")
        with open('Data/tweets_' + time + '.json', 'w', encoding='utf-8') as out_file:
            json.dump(self.tweet_data, out_file, ensure_ascii=False, indent=4, default=str)


def main():
    query_nine_euro = "(#9EuroTicket OR #9EuroTickets OR #NeunEuroTicket OR #NeunEuroTickets OR neun-euro-ticket OR" \
                      " neun-euro-tickets OR (9 euro ticket) OR (9 euro tickets)) (lang:en OR lang:de) -RT"
    query_db_general = "((@DB_Bahn OR @DB_Info OR @DB_Presse OR (deutsche bahn) OR #DeutscheBahn OR #DBNavigator) " \
                       "-(RT OR #9EuroTicket OR #9EuroTickets OR #NeunEuroTicket OR #NeunEuroTickets OR #9euro OR " \
                       "neun-euro OR (9 euro) OR 9€)) (lang:de OR lang:en)"

    download_handler = DownloadHandler()
    download_handler.read_config_file("Data/config.ini")
    download_handler.create_api_interface()
    download_handler.get_tweets(query_nine_euro, True, 12000)
    download_handler.save_tweets()


if __name__ == '__main__':
    main()
