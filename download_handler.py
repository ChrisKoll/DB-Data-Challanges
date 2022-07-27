import tweepy
import json
import configparser
import pandas as pd
from tweepy.parsers import JSONParser
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

        :param path_to_file: Path to config file --> contains authentication keys
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

    def check_available(self, client: tweepy.client, query: str):
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

        :param response: Contains all Tweets pulled from Twitter
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

    def get_tweets_json(self, query: str, batch_size: int):
        """
        Method to download the recent tweets in a json format.

        :param query: Query with keywords that are searched for
        :param batch_size: Number of Tweets that are pulled from Twitter
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

                    # Tweet data object: Contains useful information about the Tweet itself
                    tweet_data = {"Id": tweet.id, "Created_At": tweet.created_at,
                                  "Text": tweet.text.strip().replace("\n", " "), "Tweet_Source": tweet.source,
                                  "Retweet_Count": tweet.public_metrics["retweet_count"],
                                  "Reply_Count": tweet.public_metrics["reply_count"],
                                  "Like_Count": tweet.public_metrics["like_count"],
                                  "Quote_Count": tweet.public_metrics["quote_count"], "Language": tweet.lang}
                    self.tweet_data[tweet.id]["Data"] = tweet_data

                    # Tweet user object: Contains useful information about the user that posted the Tweet
                    tweet_user = {}
                    for user in response.includes["users"]:
                        if tweet.author_id == user.id:
                            tweet_user["Id"] = user.id
                            tweet_user["Name"] = user.name
                            tweet_user["Location"] = user.location
                            tweet_user["Created_At"] = user.created_at
                    self.tweet_data[tweet.id]["User"] = tweet_user

                    # Tweet place object: Contains useful information about the location the Tweet was posted on
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

                    # Collects the Hashtags from each Tweet
                    if tweet.entities and "hashtags" in tweet.entities:
                        tweet_hashtags = tweet.entities["hashtags"]
                        self.tweet_data[tweet.id]["Hashtags"] = tweet_hashtags
                    else:
                        self.tweet_data[tweet.id]["Hashtags"] = None
        else:
            print("No data can be extracted from Twitter - Try it again later...")

    def save_tweets_json(self):
        """
        Method to store Tweets in a JSON file.
        """

        time = datetime.now().strftime("%d-%m-%Y_%H-%M")
        with open('Data/tweets_' + time + '.json', 'w', encoding='utf-8') as out_file:
            json.dump(self.tweet_data, out_file, ensure_ascii=False, indent=4, default=str)

    @staticmethod
    def verbose_function(data_object, print_type: str):
        """
        Prints the requested Tweet data for debugging.

        :param data_object: Tweet, user or place object of a pulled tweet
        :param print_type: Set print type fitting to handed over data_object
        """

        if print_type == "general":
            print("###############################################\n###############################################")
            print("##### Tweet object data #####")
            print("tweet.id: ", data_object.id)
            print("tweet.created_at", data_object.created_at)
            print("tweet.text.strip()", data_object.text.strip())
            print("tweet.source", data_object.source)
            print("tweet.public_metrics", data_object.public_metrics)
            print("tweet.entities", data_object.entities)
            print("tweet.lang", data_object.lang, "\n")

        if print_type == "user":
            print("##### User object data #####")
            print("user.id", data_object.id)
            print("user.name", data_object.name)
            print("user.location", data_object.location)
            print("user.created_at", data_object.created_at, "\n")

        if print_type == "place":
            print("##### Geo object data #####")
            print("place.id", data_object.id)
            print("place.name", data_object.name)
            print("place.country_code", data_object.country_code)
            print("place.geo", data_object.geo)
            print("place.place_type", data_object.place_type, "\n")

    def get_tweets_csv(self, query, verbose, check_available_data, tweet_batch_size):
        """
        Method to download the recent tweets in a csv format.

        :param query: Query with keywords that are searched for
        :param verbose: Contains information if process is printed
        :param check_available_data: Contains information if it needs to be checked for new data
        :param tweet_batch_size: Number of Tweets that are pulled from Twitter
        :return: All pulled Tweets in csv format
        """

        # Start client
        client = tweepy.Client(bearer_token=self.bearer_token)

        if check_available_data:
            # Check how many tweets are available
            counts = client.get_recent_tweets_count(query=query, granularity="day")
            week_count = 0
            for daily_count in counts.data:
                print(daily_count)
                week_count += daily_count["tweet_count"]

            print("Tweets in the last 7 days:", week_count)
            exit()

        # Retrieve tweets for given query
        response_basic = tweepy.Paginator(client.search_recent_tweets, query=query, max_results=100).\
            flatten(limit=tweet_batch_size)

        # Extract tweet.ids
        tweet_ids = []
        for tweet in response_basic:
            tweet_ids.append(tweet.id)

        print("len before duplicate drop:", len(tweet_ids))
        # Filter out duplicates
        tweet_ids = list(set(tweet_ids))
        print("len after duplicate drop:", len(tweet_ids))

        # Create batches:
        batch_list = []
        for step in range(0, len(tweet_ids), 100):
            batch_list.append([step, step + 100])

        # replace last element, to adapt batch size (if there is at least one element)
        if batch_list:

            # Chase that one batch is left
            if (len(tweet_ids)) % 100 != 0:
                # Replace last element
                batch_list.pop(-1)
                batch_list.append([batch_list[-1][1], batch_list[-1][1] + (len(tweet_ids) % 100)])

        # Only one query, which is smaller than 100 Tweets
        else:
            batch_list.append([0, len(tweet_ids)])

        # Iterate through the tweet ids
        tweet_data = []  # Stores data of all tweets
        for current_batch in batch_list:
            # Hydrating the tweet.id with additional information
            response = client.get_tweets(ids=tweet_ids[current_batch[0]:current_batch[1]],
                                         tweet_fields=["id", "created_at", "text", "source", "public_metrics",
                                                       "entities", "lang", "geo"],
                                         user_fields=["id", "name", "location", "created_at"],
                                         place_fields=["place_type", "geo", "id", "name", "country_code"],
                                         expansions=["author_id", "geo.place_id"])

            # Define dictionary with  users in list from the includes object
            users = {u["id"]: u for u in response.includes["users"]}

            # There has to be at least one tweet with geo info
            # Dict out of list of places from includes object
            places = None
            if "places" in response.includes:
                places = {p["id"]: p for p in response.includes["places"]}

            # Extract data
            for tweet in response.data:

                # Checks if hashtags data is available:
                if tweet.entities and "hashtags" in tweet.entities:
                    hashtag_data = tweet.entities["hashtags"]
                else:
                    hashtag_data = None

                # Create list with data of current tweet
                current_tweet_data = [tweet.id, tweet.created_at, tweet.text.strip().replace("\n", " "), tweet.source,
                                      tweet.public_metrics["retweet_count"], tweet.public_metrics["reply_count"],
                                      tweet.public_metrics["like_count"], tweet.public_metrics["quote_count"],
                                      hashtag_data, tweet.lang]
                # Extract tweet data
                if verbose:
                    self.verbose_function(data_object=tweet, print_type="general")

                if users[tweet.author_id]:  # Extract user data
                    user = users[tweet.author_id]

                    # Append user data to current tweet data
                    current_tweet_data += [user.id, user.name, user.location, user.created_at]

                    if verbose:
                        self.verbose_function(data_object=user, print_type="user")

                if tweet.geo:  # Not all tweets have geo data
                    if places[tweet.geo["place_id"]]:
                        place = places[tweet.geo["place_id"]]

                        current_tweet_data += [place.id, place.name, place.country_code, place.geo, place.place_type]

                        if verbose:
                            self.verbose_function(data_object=place, print_type="place")

                # Append empty element when there is no geo data
                else:
                    current_tweet_data += [None, None, None, None, None]

                # Format elements to list and replace potential false separators
                current_tweet_data = list(map(lambda x: x.replace("$", "€"), list(map(str, current_tweet_data))))

                if verbose:
                    print(current_tweet_data)
                # Append formatted tweet data to final list
                tweet_data.append(current_tweet_data)

        return tweet_data

    @staticmethod
    def save_tweets_csv(tweets, columns: list[str]):
        """
        Method to store input tweets in a csv file.

        :param tweets: All pulled Tweets
        :param columns: All column names for the data frame
        """

        data_frame = pd.DataFrame(tweets, columns=columns)

        time = datetime.now().strftime("%d-%m-%Y_%H-%M")
        data_frame.to_csv("Data/tweets_" + time + ".csv", sep="$")
