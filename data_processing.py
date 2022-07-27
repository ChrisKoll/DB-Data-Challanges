import pandas as pd
import glob
from datetime import datetime


class DataProcessing:
    """
    Imports, filter and save data in geopandas dataframes.
    """

    def __init__(self):
        """
        Constructor.
        """
        self.dataframe = None
        self.input_directory = None
        self.city_location_count = None
        self.tweet_df = pd.DataFrame([])
        self.history_tweet_df = pd.DataFrame([])
        self.city_key_dict = {}
        self.short_tweet_df = pd.DataFrame
        self.history_short_tweet_df = pd.DataFrame
        self.user_id_dict = {}
        self.relevant_city_combination_dict = {}
        self.relevant_user_dict = {}
        self.intersting_routes_dict = {"Dresden$Leipzig": 6, "Berlin$München": 15, "Frankfurt$Kassel": 5,
                                       "Berlin$Hamburg": 18, "Hamburg$Rostock": 10, "Hamburg$Kiel": 5,
                                       "Berlin$Frankfurt": 9, "Berlin$Köln": 11, "Berlin$Leipzig": 5,
                                       "Berlin$Magdeburg": 5, "Berlin$Rostock": 6, "Düsseldorf$Köln": 9}

    def create_df_with_storage_data(self, input_dir_path):
        """
        Reads in the tweet storage csv files and combines them to a single pandas dataframe. All files in the handed
        over directory, which are formatted like a storage file are considered.
        :param input_dir_path: path to input directory
        """

        # Get all relevant files that are stored in the input dir
        # Returns every file in the directory with .out at the end
        input_file_list = glob.glob(input_dir_path + "/tweets_*.csv")
        pd.set_option('display.max_columns', None)  # prints all columns

        self.city_location_count = 0     # Saves the number of tweets with city location data

        entry_count = 0

        # Read in tweet data from files
        for current_file_path in input_file_list:
            # print("Current file path: ", current_file_path)

            current_tweet_df = pd.read_csv(current_file_path, sep="$", index_col=0)
            # print(len(current_tweet_df))
            entry_count += len(current_tweet_df)
            # print(sum(current_tweet_df["place.name"] != "None"))
            # print(sum(current_tweet_df["place.place_type"] == "city"))
            self.tweet_df = pd.concat([self.tweet_df, current_tweet_df])

        # Drop duplicates
        # print(self.tweet_df.head())
        # self.tweet_df = self.tweet_df.set_index("tweet.id", inplace=False) # Sets index column to tweeti
        self.tweet_df = self.tweet_df.drop_duplicates(subset="tweet.id")
        # print("???", len(self.tweet_df[self.tweet_df.index.duplicated()]))
        # gives back indexes that are present multiple times
        # print("???", self.tweet_df.index)  # prints all present indexes

        # print("Tweets with geo object: ", sum(self.tweet_df["place.name"] != "None"))
        # print("Tweets with city geo object: ", sum(self.tweet_df["place.place_type"] == "city"))
        # self.city_location_count += sum(self.tweet_df["place.place_type"] == "city")

        print("length tweet.df (without duplicates)", len(self.tweet_df))

        print("Counted csv entries (with duplications):", entry_count)

    def load_city_key_data(self, city_key_file_path):
        """
        Load the file with all key parts of all cities and smaller tows in germany. And creates a dictionary for a fast
        key search.

        :param city_key_file_path: path to city key file
        """

        city_key_file = open(city_key_file_path, "r", encoding="utf-8")

        # Append key parts of the city names in the key list
        for line in city_key_file:
            converted = line.strip().split()[0]
            self.city_key_dict[converted] = converted

    def text_city_key_extraction(self, tweet_text):
        """
        Filters for a given tweet text the keywords out. And checks if the city keys are used in combination with
        other keywords, which might indicate an assignment as start or end position of the traveling.

        :param tweet_text: stripped text of a tweet
        :return: isolated_keys_list, start_keys_list, end_keys_list
        """

        # Split tweet text, to search for words not substrings
        tweet_text_split = tweet_text.split()

        # Remove all  hashtags, dots, commas in front of the words to enable key search
        tweet_text_split = list(map(lambda x: x.replace("#", "").replace(".", "").replace(",", ""), tweet_text_split))

        # Stores isolated city key words found in the tweet text
        isolated_keys_list = []

        # Stores city key words that are found in combination with start key words in the tweet text
        start_keys_list = []
        # Stores city key words that are found in combination with end key words in the tweet text
        end_keys_list = []
        # saves the position of the current word in the text to check previous words as keys
        current_list_position = 0

        # Check for each word, if it is a key in the city key name dict
        for current_word in tweet_text_split:

            # Checks if word is a key
            if current_word in self.city_key_dict:

                previous_pos = tweet_text_split[current_list_position - 1]

                # Check if the key is a start position key
                if ((previous_pos == "von") or (previous_pos == "Von")
                    or (previous_pos == "aus") or (previous_pos == "Aus") or (previous_pos == "from") or
                                                  (previous_pos == "From")) and current_list_position != 0:
                    start_keys_list.append(current_word)

                # Check if the key is an end position key
                elif ((previous_pos == "nach") or (previous_pos == "Nach") or (previous_pos == "to") or
                                                (previous_pos == "To")) and current_list_position != 0:
                    end_keys_list.append(current_word)

                # Isolated key found
                else:
                    isolated_keys_list.append(current_word)

            current_list_position += 1

        return start_keys_list, end_keys_list, isolated_keys_list

    def create_short_tweet_df(self):
        """
        Write function which kicks out unessesary columns and add columns for city key storage
        The information of the geo location name should also be considered, when type is city
        The information of the enteties can be included, but i think they are based on the tweet text.
        English tweets should be excluded from the analysis.
        Account location as start.
        """

        # Remove dots form column names, they are obstructive
        self.tweet_df.columns = self.tweet_df.columns.str.replace('.', '_')

        # Drop unnecessary columns
        self.short_tweet_df = self.tweet_df.drop(["tweet_source", "tweet_retweet_count", "tweet_reply_count",
                                                  "tweet_like_count", "tweet_quote_count", "tweet_hashtags",
                                                  "user_name", "tweet_lang"], axis=1)

        # New columns
        self.short_tweet_df["hometowns"] = self.short_tweet_df.apply(lambda x: [], axis=1)
        self.short_tweet_df["destinations"] = self.short_tweet_df.apply(lambda x: [], axis=1)
        self.short_tweet_df["unassigned_locations"] = self.short_tweet_df.apply(lambda x: [], axis=1)

        # transfer user.location in hometowns with city key detection
        self.short_tweet_df["hometowns"] = self.short_tweet_df.apply(
            lambda name_list: name_list.hometowns + self.text_city_key_extraction(name_list.user_location)[2], axis=1)

        # Transfer tagged geo data in unassigned column with city key detection
        self.short_tweet_df["unassigned_locations"] = self.short_tweet_df.apply(
            lambda name_list: name_list.unassigned_locations + self.text_city_key_extraction(name_list.place_name)[2],
            axis=1)

        # Drop unnecessary geo columns
        self.short_tweet_df = self.short_tweet_df.drop(["place_name", "place_country_code", "place_id",
                                                        "place_geo", "place_place_type", "user_location"], axis=1)

        # City name extraction from tweet texts
        # For hometowns
        self.short_tweet_df["hometowns"] = self.short_tweet_df.apply(
            lambda name_list: name_list.hometowns + self.text_city_key_extraction(name_list.tweet_text)[0],
            axis=1)
        # For travel destinations
        self.short_tweet_df["destinations"] = self.short_tweet_df.apply(
            lambda name_list: name_list.destinations + self.text_city_key_extraction(name_list.tweet_text)[1],
            axis=1)
        # For city names without route assignment
        self.short_tweet_df["unassigned_locations"] = self.short_tweet_df.apply(
            lambda name_list: name_list.unassigned_locations + self.text_city_key_extraction(name_list.tweet_text)[2],
            axis=1)

        """#print(self.short_tweet_df.head())
        print("Hometown count distribution: \n", self.short_tweet_df["hometowns"].value_counts())
        print("Destination count distribution: \n", self.short_tweet_df["destinations"].value_counts())
        print("Unassigned_locations count distribution: \n", self.short_tweet_df["unassigned_locations"].value_counts())
        """

        # Drop all rows, where no geolocation was assigned
        self.short_tweet_df = self.short_tweet_df[(self.short_tweet_df["hometowns"].map(lambda x: len(x)) > 0) |
                                                  (self.short_tweet_df["destinations"].map(lambda x: len(x)) > 0) |
                                                  (self.short_tweet_df["unassigned_locations"].map(lambda x: len(x))
                                                   > 0)]

        """print("###########################################")
        print(len(self.short_tweet_df))
        print(self.short_tweet_df.head())


        print("Hometown count distribution: \n", self.short_tweet_df["hometowns"].value_counts())
        print("Destination count distribution: \n", self.short_tweet_df["destinations"].value_counts())
        print("Unassigned_locations count distribution: \n", self.short_tweet_df["unassigned_locations"].value_counts())

        # Print specific index
        #print(self.short_tweet_df.loc[[3]])"""

    def extract_individual_user_ids(self):
        """
        Extracts individual user id of the short_tweet_df for the user history pulls.
        :return: user_id_df
        """

        print("\n")
        print("(Short_df) Number of geo tweets in df without duplicates", len(self.short_tweet_df))

        # Extract tweets with assigned hometown and travel destination
        self.short_tweet_df = self.short_tweet_df[(self.short_tweet_df["hometowns"].map(lambda x: len(x)) > 0) &
                                                  (self.short_tweet_df["destinations"].map(lambda x: len(x)) > 0)]

        print("Number of top 657 User tweets", len(self.short_tweet_df))
        # Drop user id duplicates
        user_id_df = self.short_tweet_df.drop_duplicates(subset="user_id")
        user_id_list = user_id_df['user_id'].tolist()
        print("Indidual users with assigned geo data", len(user_id_list))

        # print(user_id_list[:10])

        return user_id_list

    @staticmethod
    def db_key_extraction(tweet_text):
        """
        Function determines if a tweet text is related to the deutsch bahn
        :param tweet_text: trivial
        :return: db_related
        """

        # True if tweet text is db related
        db_related = False

        # Split tweet text, to search for words not substrings
        tweet_text_split = tweet_text.split()

        # Remove all  hashtags, dots, commas in front of the words to enable key search
        tweet_text_split = list(map(lambda x: x.replace("#", "").replace(".", "").replace(",", ""), tweet_text_split))

        key_dict = {"@DB_Bahn": "@DB_Bahn", "@DB_Info": "@DB_Info", "@DB_Presse": "@DB_Presse", "bahn": "bahn",
                    "Bahn": "Bahn", "DeutscheBahn": "DeutscheBahn", "#DBNavigator": "#DBNavigator",
                    "#9EuroTicket": "#9EuroTicket", "#9EuroTickets": "#9EuroTickets",
                    "#NeunEuroTicket": "#NeunEuroTicket", "#NeunEuroTickets": "#NeunEuroTickets",
                    "neun-euro-ticket": "neun-euro-ticket", "neun-euro-tickets": "neun-euro-tickets",
                    }

        # Check for each word, if it is a key in the db key name dict
        for current_word in tweet_text_split:

            # Checks if word is a key
            if current_word in key_dict:
                db_related = True
                break

        return db_related

    def check_user_abundance_in_df(self, user_id):
        """
        Checks how often a user is present in a dataframe a returns False if the user is already overrepresented.
        :param user_id: trivial
        :return:
        """

        user_overrepresented = False

        if self.user_id_dict[user_id] >= 3:
            user_overrepresented = True

        else:
            self.user_id_dict[user_id] = self.user_id_dict[user_id] + 1

        return user_overrepresented

    def save_db_related_tweets_for_annotation(self):
        """
        Extracts the tweets out of the dataframe that are related to the the Deutsche Bahn and saves them as csv file.
        :return:
        """

        # Remove dots form column names, they are obstructive
        self.tweet_df.columns = self.tweet_df.columns.str.replace('.', '_')

        # Add column for DB relation assessment
        self.tweet_df["db_related"] = False

        # Determine if the tweet texts are DB related
        self.tweet_df["db_related"] = self.tweet_df.apply(lambda db_related:
                                                          self.db_key_extraction(tweet_text=db_related.tweet_text,
                                                                                 self=self), axis=1)

        # Drop tweets that are not db related
        self.tweet_df = self.tweet_df[(self.tweet_df["db_related"] == True)]

        # Drop unnecessary column
        self.tweet_df = self.tweet_df.drop(["db_related"], axis=1)

        print("DB related tweets in df:", len(self.tweet_df))

        # Individual users ids in the dataset
        user_id_df = self.tweet_df.drop_duplicates(subset="user_id")
        print("Individual users with DB related Tweets in history", len(user_id_df))

        # Create key dict for all the users with DB tweets in history
        user_id_list_db = user_id_df["user_id"].tolist()
        # Initialise dictionary
        for userd_id in user_id_list_db:
            self.user_id_dict[userd_id] = 0  # Initialise count with zero

        # Add additional row to df for assignment if a tweet should be dropped because there are already enough tweets
        # of this user in the dataframe
        self.tweet_df["overrepresented"] = False

        # Assign if tweets should be kicked out because of overrepresentation
        self.tweet_df["overrepresented"] = self.tweet_df.apply(lambda x: self.check_user_abundance_in_df(x.user_id),
                                                               axis=1)

        # Drop overrepresented tweets from dataframe
        self.tweet_df = self.tweet_df[(self.tweet_df["overrepresented"] == False)]

        # Drop column for overrepresented
        self.tweet_df = self.tweet_df.drop(["overrepresented"], axis=1)

        # Add column for annotation
        self.tweet_df["sentiment"] = None

        print("Number of tweets for annotation:", len(self.tweet_df))

        # Save tweets as csv file
        time = datetime.now().strftime("%d-%m-%Y_%H-%M")
        self.tweet_df.to_csv("Data/tweets_" + time + ".csv", sep="$")

    def assign_tweet_text_to_city_combination(self, hometowns, destinations, user_id):
        """
        Checks what city combination is part of the tweet text and increments the count if a tweet text that is related
        to a top combination in the dictionary
        :param hometowns: trivial
        :param destinations: trivial
        :return: None
        """

        # Stores all possible city combinations of the current tweet
        temp_dict = {}

        for city in hometowns:
            for city_2 in destinations:
                # Irrelevant case
                if city == city_2:
                    continue
                else:
                    name_constallation = [city, city_2]
                    name_constallation.sort()
                    temp_dict[name_constallation[0] + "$" + name_constallation[1]] = 0

        # Check if there are relevant city combination in the current tweet data
        for key, value in temp_dict.items():
            if key in self.relevant_city_combination_dict:
                # Increment the number of occurrence of the specific city combination
                self.relevant_city_combination_dict[key] = self.relevant_city_combination_dict[key] + 1

                if key in self.intersting_routes_dict:
                    self.relevant_user_dict[user_id] = user_id

        # Return with no effect
        return hometowns

    def check_overrepresented_city_combination(self):
        """
        Determines which cities have a high abundance, and thus checks which city combinations alias pseudo train lines
        are highly present in the dataset.
        :return:
        """

        # Save dataframe for manual annotation as csv
        time = datetime.now().strftime("%d-%m-%Y_%H-%M")
        self.short_tweet_df.to_csv("Data/9euro-annotation" + time + ".csv", sep="$")

        # Check the abundance of all cities in the tweets of the top 657 User tweets
        # Extract the city names as list of lists with city names
        hometown_city_list = self.short_tweet_df["hometowns"].tolist()
        destination_city_list = self.short_tweet_df["destinations"].tolist()

        # Stores all cities that are present in the data
        city_dict = {}

        # Count the city abundance and save this information in the dict
        for city_list in hometown_city_list:
            for city_name in city_list:

                # Case that city already occurs
                if city_name in city_dict:
                    city_dict[city_name] = city_dict[city_name] + 1

                # Case for first appearance of a city
                else:
                    city_dict[city_name] = 1

        for city_list in destination_city_list:
            for city_name in city_list:

                # Case that city already occurs
                if city_name in city_dict:
                    city_dict[city_name] = city_dict[city_name] + 1

                # Case for first appearance of a city
                else:
                    city_dict[city_name] = 1

        relevant_city_list = []
        # Print the abundance of all cities
        for key, value in city_dict.items():
            if value >= 20 and (key != "Sylt"):
                print(key, value)
                relevant_city_list.append(key)

        # Determine all possible city combinations
        for city in relevant_city_list:
            for city_2 in relevant_city_list:

                # Irrelevant case
                if city == city_2:
                    continue
                else:
                    name_constallation = [city, city_2]
                    name_constallation.sort()
                    self.relevant_city_combination_dict[name_constallation[0] + "$" + name_constallation[1]] = 0

        # Check in the tweets how often the constellation occur and increment the values in the dict
        self.short_tweet_df["hometowns"] = self.short_tweet_df.apply(
            lambda x: self.assign_tweet_text_to_city_combination(x.hometowns, x.destinations, x.user_id), axis=1)

        for key, value in self.relevant_city_combination_dict.items():
            if value > 4:
                print(key, value)

        # Print the user that are interesting for the manual annotation
        relevant_user_list = []
        for key, value in self.relevant_user_dict.items():
            relevant_user_list.append(key)

        print("Number of relevant users:", len(relevant_user_list))
        print(relevant_user_list)


def main():
    # Read in storage files
    """storage_dir_path = "C://Users//19joh//Desktop//testdir//"

    city_key_file_path = "C://Users//19joh//Desktop//deutschland_gemeinden_short.txt"

    tweet_processing = DataProcessing()
    tweet_processing.load_city_key_data(city_key_file_path)

    tweet_processing.create_df_with_storage_data(input_dir_path=storage_dir_path)

    tweet_processing.create_short_tweet_df()

    tweet_processing.extract_individual_user_ids()"""


if __name__ == '__main__':
    main()
