from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import ConfigParser


config = ConfigParser.ConfigParser()
config.read('config.ini')


ConsumerKey = config.get('Twitter', 'ConsumerKey', 0)
ConsumerSecret = config.get('Twitter', 'ConsumerSecret', 0)
AccessToken = config.get('Twitter', 'AccessToken', 0)
AccessSecret = config.get('Twitter', 'AccessSecret', 0)

class listener(StreamListener):

    def on_data(self, data):
        print(data)
        return(True)

    def on_error(self, status):
        print status

auth = OAuthHandler(ConsumerKey, ConsumerSecret)
auth.set_access_token(AccessToken, AccessSecret)

twitterStream = Stream(auth, listener())
twitterStream.filter(track=["Everton"])

