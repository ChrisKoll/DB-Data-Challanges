
import download_handler
import data_processing


def main():

    """# Pull relevant user histories
    # Read in storage files
    storage_dir_path = "C://Users//19joh//Desktop//testdir//"
    # File with city key names
    city_key_file_path = "C://Users//19joh//Desktop//deutschland_gemeinden_short.txt"

    # Create tweet processing instance
    tweet_processing = data_processing.DataProcessing()
    # Load city key names
    tweet_processing.load_city_key_data(city_key_file_path)

    # Read in the tweets of the last 6 weeks
    tweet_processing.create_df_with_storage_data(input_dir_path=storage_dir_path)
    # Reduce the dataframe to drop unnecessary information and detect city names
    tweet_processing.create_short_tweet_df()

    # Extract relevant user ids for history search
    user_id_list = tweet_processing.extract_individual_user_ids()

    # Create download instance
    download_handler_1 = download_handler.DownloadHandler()
    key_location = "Data/config.ini"
    # Create API connection
    download_handler_1.read_config_file(key_location)
    download_handler_1.create_api_interface()

    # Pull the history tweets
    #tweets = download_handler_1.pull_user_histories_deep([1003746587842105346,
    #                                                      1003746587842105346], max_results=10)
    tweets = download_handler_1.pull_user_histories_deep(user_id_list=user_id_list[350:400], max_results=2000)

    # Save the tweets as file
    columns = ["tweet.id", "tweet.created_at", "tweet.text", "user.id"]
    download_handler_1.save_tweets(tweets, columns)
    #"""

    # Extract relevant history files and save them as csv
    """# Read in storage files
    storage_dir_path = "C://Users//19joh//Desktop//history_tweets//"
    # Create tweet processing instance
    tweet_processing = data_processing.DataProcessing()
    # Read in the tweets of user histories
    tweet_processing.create_df_with_storage_data(input_dir_path=storage_dir_path)
    # Extract DB related tweets and save them as csv
    tweet_processing.save_db_related_tweets_for_annotation()
    #"""

    # Extract relevant users
    """# Overrepresented railway line extraction out of short 650er df
    # Pull relevant user histories
    # Read in storage files
    storage_dir_path = "C://Users//19joh//Desktop//testdir//"
    # File with city key names
    city_key_file_path = "C://Users//19joh//Desktop//deutschland_gemeinden_short.txt"

    # Create tweet processing instance
    tweet_processing = data_processing.DataProcessing()
    # Load city key names
    tweet_processing.load_city_key_data(city_key_file_path)

    # Read in the tweets of the last 6 weeks
    tweet_processing.create_df_with_storage_data(input_dir_path=storage_dir_path)
    # Reduce the dataframe to drop unnecessary information and detect city names
    tweet_processing.create_short_tweet_df()

    # Extract relevant user ids for history search
    tweet_processing.extract_individual_user_ids()

    # Determine overrepresented city combinations
    tweet_processing.check_overrepresented_city_combination()
    #"""


if __name__ == '__main__':
    main()
