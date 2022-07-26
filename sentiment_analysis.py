import json
from textblob_de import TextBlobDE as Blob


class SentimentAnalyser:

    def __init__(self, json_file):
        self.json_file = json_file
        self.data = self.read_json()

    def read_json(self):
        with open(self.json_file, 'r') as in_file:
            data = json.load(in_file)

        return data

    def sentiment_analysis(self):
        for tweet in self.data:
            analysis = Blob(self.data[tweet]["Data"]["Text"])
            self.data[tweet]["Data"]["Sentiment"] = analysis.sentiment.polarity

    def save_json(self):
        with open(self.json_file, 'w', encoding='utf-8') as out_file:
            json.dump(self.data, out_file, ensure_ascii=False, indent=4, default=str)


def main():
    sa = SentimentAnalyser("/Data/Nine/tweets_15-06_21-07_nine.json")
    sa.sentiment_analysis()
    sa.save_json()


if __name__ == '__main__':
    main()
