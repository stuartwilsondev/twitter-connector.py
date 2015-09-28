from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import ConfigParser
from couchdbkit import Server
import json

CouchDbServer = Server()

config = ConfigParser.ConfigParser()
config.read('config.ini')

ConsumerKey = config.get('Twitter', 'ConsumerKey', 0)
ConsumerSecret = config.get('Twitter', 'ConsumerSecret', 0)
AccessToken = config.get('Twitter', 'AccessToken', 0)
AccessSecret = config.get('Twitter', 'AccessSecret', 0)
TweetDbName = config.get('Twitter', 'DbName', 0)
Filter = config.get('Twitter', 'Filter', 0)
UserIds = config.get('Twitter', 'UserIds', 0)

db = CouchDbServer.create_db(TweetDbName)

class listener(StreamListener):
    
    def on_data(self, data):
        db.save_doc(json.loads(data))
        return(True)

    def on_error(self, status):
        print status

auth = OAuthHandler(ConsumerKey, ConsumerSecret)
auth.set_access_token(AccessToken, AccessSecret)

try:
    twitterStream = Stream(auth, listener())

    twitterStream.filter(track=[Filter], follow=[UserIds])
except (KeyboardInterrupt, SystemExit):
    print "Exiting..."



