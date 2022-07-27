import json
from textblob_de import TextBlobDE as Blob


class SentimentAnalyser:
    """
    Does a sentiment analysis for a given set of Tweets.
    """

    def __init__(self, json_file: str):
        """
        Constructor.

        :param json_file: Path to JSON file with Twitter data
        """

        self.json_file = json_file
        self.data = self.read_json()

    def read_json(self):
        """
        Reads the JSON file.

        :return: Dict object
        """

        # Opens JSON file
        with open(self.json_file, 'r') as in_file:
            data = json.load(in_file)

        return data

    def sentiment_analysis(self):
        """
        Does sentiment analysis with Textblob and saves results in dict.
        """

        for tweet in self.data:
            # Textblob analysis
            analysis = Blob(self.data[tweet]["Data"]["Text"])
            # Save sentiment in Tweet object
            self.data[tweet]["Data"]["Sentiment"] = analysis.sentiment.polarity

    def save_json(self):
        """
        Saves the dict in a JSON file.
        """

        # Opens outgoing file
        with open(self.json_file, 'w', encoding='utf-8') as out_file:
            json.dump(self.data, out_file, ensure_ascii=False, indent=4, default=str)
