import tweepy
import configparser


class HistorySearcher:
    """
    Class to search for user histories on Twitter.
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

    def pull_user_histories(self, user_id_list: list, verbose: bool, max_results: int):
        """
        Pulls the last up to 100 tweets of every user that is handed over in the user_id_list parameter.
        Retweets are excluded from the analysis. The tweets are returned as python list.

        :param user_id_list: List with the user_id's
        :param verbose: Detailed info in terminal when true
        :param max_results: Maximum tweets puller per user, current cap 100
        :return: Tweet_data
        """

        # Start client
        client = tweepy.Client(bearer_token=self.bearer_token)

        # Iterate through the tweet ids
        tweet_data = []  # Stores data of all tweets

        for user_id in user_id_list:
            response = client.get_users_tweets(id=user_id, exclude="retweets", max_results=max_results,
                                               end_time="2022-05-31T00:00:01Z",
                                               tweet_fields=["id", "created_at", "text", "source", "public_metrics",
                                                             "entities", "lang", "geo"],
                                               user_fields=["id", "name", "location", "created_at"],
                                               place_fields=["place_type", "geo", "id", "name", "country_code"],
                                               expansions=["author_id", "geo.place_id"])

            # Skip user that have not tweeted before the 9 euro ticket - skip to next loop run / user
            if not response.data:
                continue

            # In the user history case only one user
            # Define dictionary with  users in list from the includes object
            users = None
            if "users" in response.includes:
                users = {u["id"]: u for u in response.includes["users"]}

            places = None
            # There has to be at least one tweet with geo info
            # Dict out of list of places from includes object
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

                else:
                    current_tweet_data += ["None", "None", "None", "None"]

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

        print("User history tweets pulled:", len(tweet_data))

        return tweet_data

    def pull_user_histories_deep(self, user_id_list, max_results):
        """
        Pulls the last x tweets of every user that is handed over in the user_id_list parameter.
        Retweets are excluded from the analysis. The tweets are returned as python list.

        :param user_id_list: List with the user_id's
        :param max_results: maximum tweets puller per user, current cap 3200
        :return: tweet_data
        """

        # Start client
        client = tweepy.Client(bearer_token=self.bearer_token)

        # Iterate through the tweet ids
        tweet_data = []  # Stores data of all tweets
        count = 0
        for user_id in user_id_list:

            print("current user:", count)
            count += 1

            paginator_response = tweepy.Paginator(client.get_users_tweets, id=user_id, max_results=100,
                                                  exclude="retweets", end_time="2022-05-31T00:00:01Z",
                                                  tweet_fields=["id", "created_at", "text"]).flatten(limit=max_results)

            # Skip user that have not tweeted before the 9 euro ticket - skip to next loop run / user
            if not paginator_response:
                continue

            # Extract data
            for tweet in paginator_response:

                # Create list with data of current tweet
                current_tweet_data = [tweet.id, tweet.created_at, tweet.text.strip().replace("\n", " "), user_id]

                # Format elements to list and replace potential false separators
                current_tweet_data = list(map(lambda x: x.replace("$", "€"), list(map(str, current_tweet_data))))

                # Append formatted tweet data to final list
                # Tweet_id, tweet_Created_at, tweet_text, user_id
                tweet_data.append(current_tweet_data)

        print("User history tweets pulled:", len(tweet_data))

        return tweet_data
