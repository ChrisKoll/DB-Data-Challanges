import json
import configparser
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from datetime import datetime
from pyvis.network import Network
from geopy.geocoders import Nominatim


class TweetMapper:
    """
    Toolkit for working with Geo Tweets.
    """

    def __init__(self, config_file: str, twitter_json_file=None, geo_tweets_json_file=None):
        """
        Constructor.

        :param twitter_json_file: Path to Twitter data
        :param geo_tweets_json_file:  Path to Geo Tweets
        """

        self.twitter_json_file = twitter_json_file
        self.geo_tweets_json_file = geo_tweets_json_file
        self.tweet_data = {}
        self.geo_tweets = []
        self.locations = {}
        self.config = configparser.RawConfigParser()
        self.config.read(config_file)

    @staticmethod
    def get_tweets(json_file: str):
        """
        Gets a dataset filled with Tweets and saves them in a dictionary.

        :param json_file: JSON file containing Twitter data
        """

        with open(json_file, 'r', encoding='utf-8') as in_file:
            data = json.load(in_file)

        return data

    def extract_geo(self):
        """
        Extracts all Tweets from the dataset that have a Geo object attached.
        """

        self.geo_tweets = {}
        for tweet_id in self.tweet_data:
            tweet = self.tweet_data[tweet_id]

            if all(tweet['Geo'].values()):
                self.geo_tweets[tweet_id] = tweet

    def save_geo_tweets(self, data_file=None):
        """
        Saves the extracted Tweets into a JSON file that can be expanded.

        :param data_file: If a data file is given, the values are attached to the file
        """

        if data_file is not None:
            with open(data_file, 'a', encoding='utf-8') as out_file:
                json.dump(self.geo_tweets, out_file, ensure_ascii=False, indent=4)
        else:
            time = datetime.now().strftime("%d-%m-%Y_%H-%M")
            with open('Geo_Tweets_' + time + '.json', 'w', encoding='utf-8') as out_file:
                json.dump(self.geo_tweets, out_file, ensure_ascii=False, indent=4)

    def extract_geo_tweets(self):
        """
        Handles the extraction process of geo Tweets.
        """

        self.tweet_data = self.get_tweets(self.twitter_json_file)
        self.extract_geo()
        self.save_geo_tweets()

    def get_locations(self, json_file: str):
        """
        Gets the location data.

        :param json_file: JSON file that contains the information
        """

        with open(json_file, "r", encoding="utf-8") as in_file:
            self.locations = json.load(in_file)

    def update_locations(self):
        """
        Creates a location file with latitude and longitude for all locations found in the geo Tweets.
        """

        # Create dataframe with Geo Tweets
        self.geo_tweets = pd.read_json(self.geo_tweets_json_file, orient='index', convert_axes=False)

        # Add locations to all Tweets with country code = DE
        for i in range(len(self.geo_tweets['Geo'])):
            if self.geo_tweets['Geo'][i]['Country_Code'] == 'DE':
                if self.geo_tweets['Geo'][i]['Name'] not in self.locations:
                    self.locations[self.geo_tweets['Geo'][i]['Name']] = None

        # Get longitude / latitude from city name with Nominatim API
        for place in self.locations.keys():
            if self.locations[place] is None:
                geolocator = Nominatim(user_agent=self.config["nominatim"]["user_agent"])
                location = geolocator.geocode(place, country_codes='de')
                if location is None:
                    self.locations[place] = {"Latitude": "n/a", "Longitude": "n/a"}
                else:
                    self.locations[place] = {"Latitude": location.latitude, "Longitude": location.longitude}

    def save_locations(self, locations_database=None):
        """
        Saves the locations in a JSON file.
        """

        if locations_database is not None:
            with open(locations_database, "w", encoding='utf-8') as out_file:
                json.dump(self.locations, out_file, indent=4)
        else:
            time = datetime.now().strftime("%d-%m-%Y_%H-%M")
            with open('locations_database_' + time + '.json', 'w', encoding='utf-8') as out_file:
                json.dump(self.locations, out_file, ensure_ascii=False, indent=4)

    def add_locations(self):
        """
        Adds longitude and latitude to the Geo Tweets.
        """

        with open(self.geo_tweets_json_file, "r", encoding="utf-8") as in_file:
            self.geo_tweets = json.load(in_file)

            for entry in self.geo_tweets.keys():
                location = self.geo_tweets[entry]['Geo']['Name']
                if location in self.locations.keys():
                    self.geo_tweets[entry]['Geo']['Place'] = self.locations[location]

        with open(self.geo_tweets_json_file, 'w', encoding="utf-8") as out_file:
            json.dump(self.geo_tweets, out_file, indent=4)

    def extract_locations(self, locations_database):
        """
        Handles the location extraction process.

        :param locations_database: Path to locations database file
        """

        if locations_database:
            self.get_locations(locations_database)
            self.update_locations()
            self.save_locations(locations_database)
        else:
            self.update_locations()
            self.save_locations()

    @staticmethod
    def plot_geo_data(json_file: str, file_name: str):
        """
        Plots the Geo Tweet data on a map of Germany.

        :param json_file: Path to Geo Tweet file
        :param file_name: File name for output
        """

        with open(json_file, 'r', encoding='utf-8') as in_file:
            data = json.load(in_file)

            # Creates dataframe with essential information
            df = {}
            for entry in data.keys():
                values = data[entry]
                if values['Geo']['Country_Code'] == 'DE':
                    df[values['Data']['Id']] = {'User': values['User']['Id'],
                                                'Latitude': values['Geo']['Place']['Latitude'],
                                                'Longitude': values['Geo']['Place']['Longitude'],
                                                'Sentiment': values['Data']['Sentiment'],
                                                'Created_At': values['Data']['Created_At']}

            df = pd.DataFrame.from_dict(df, orient='Index')

            # Drops rows with no location data
            to_drop = []
            for index, row in df.iterrows():
                if row['Latitude'] == "n/a" or row['Longitude'] == "n/a":
                    to_drop.append(index)
            df = df.drop(to_drop)

            # Centimeters in inches
            cm = 1 / 2.54
            # Plots data
            fig, ax = plt.subplots(figsize=(21*cm, 29*cm))

            countries = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
            countries[countries["name"] == "Germany"].plot(color="lightgrey", ax=ax)

            df.plot(x="Longitude", y="Latitude", kind="scatter", c="Sentiment", colormap="Blues",
                    title=f"Distribution Germany", ax=ax)

            plt.savefig(file_name + ".pdf")

    @staticmethod
    def create_nodes_and_edges(df):
        """
        Creates nodes and edges dataframe.

        :param df: Dataframe containing track info
        :return: Dict for all nodes and dict for all edges
        """

        place_nodes, track_nodes, user_nodes = {}, {}, {}
        edges = {}
        for index, row in df.iterrows():
            # Save user_id
            user = row['user_id']

            # Get starting points and destinations
            starting, ending, unassigned = [], [], []
            if row['hometowns'] != "NaN":
                starting = row['hometowns'].split()
            if row['destinations'] != "NaN":
                ending = row['destinations'].split()

            # Create all tracks
            if starting and not ending:
                for entry in starting:
                    if entry in place_nodes.keys():
                        place_nodes[entry] += 1
                    else:
                        place_nodes[entry] = 1
                    # Save information as edge
                    if entry in edges.keys():
                        edges[(user, entry)] += 1
                    else:
                        edges[(user, entry)] = 1
            elif ending and not starting:
                for entry in ending:
                    if entry in place_nodes.keys():
                        place_nodes[entry] += 1
                    else:
                        place_nodes[entry] = 1
                    # Save information as edge
                    if entry in edges.keys():
                        edges[(user, entry)] += 1
                    else:
                        edges[(user, entry)] = 1
            else:
                for start in starting:
                    for end in ending:
                        track = start + " --> " + end
                        if track in track_nodes.keys():
                            track_nodes[track] += 1
                        else:
                            track_nodes[track] = 1
                        # Save information as edge
                        if track in edges.keys():
                            edges[(user, track)] += 1
                        else:
                            edges[(user, track)] = 1

            if row['unassigned_locations'] != "NaN":
                unassigned = row['unassigned_locations'].split()

            if unassigned:
                for entry in unassigned:
                    if entry in place_nodes.keys():
                        place_nodes[entry] += 1
                    else:
                        place_nodes[entry] = 1
                    # Save information as edge
                    if entry in edges.keys():
                        edges[(user, entry)] += 1
                    else:
                        edges[(user, entry)] = 1

            if user in user_nodes.keys():
                user_nodes[user] += 1
            else:
                user_nodes[user] = 1

        return {**track_nodes, **place_nodes, **user_nodes}, edges

    def plot_new_data(self, csv_file: str, separator: str):
        """
        Creates the relationship graph for users and tracks.

        :param csv_file: Path to csv file
        :param separator: Seperator for csv file
        """

        # Create dataframe from csv
        # Example row: 79$1543858536228290560$2022-07-04 07:25:33+00:00$Okay, ich pendle ...$159233006$
        # 2010-06-24 20:50:10+00:00$Augsburg München$München$Kissing
        df = pd.read_csv(csv_file, sep=separator, na_values=" NaN")
        df = df.fillna("NaN")

        net = Network(height='100%', width='100%')

        nodes, edges = self.create_nodes_and_edges(df)

        for node in nodes.keys():
            net.add_node(node, value=nodes[node], label=str(node))

        for edge in edges.keys():
            net.add_edge(edge[0], edge[1], weight=edges[edge])

        net.show("track_impact.html")
