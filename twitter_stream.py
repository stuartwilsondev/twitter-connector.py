from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import ConfigParser
from couchdbkit import Server
import json
from optparse import OptionParser

parser = OptionParser()
parser.add_option("-s","--skip_retweets",default=False,
                  help="Ignore Retweets")

(options, args) = parser.parse_args()


CouchDbServer = Server()

config = ConfigParser.ConfigParser()
config.read('config.ini')

ConsumerKey = config.get('Twitter', 'ConsumerKey', 0)
ConsumerSecret = config.get('Twitter', 'ConsumerSecret', 0)
AccessToken = config.get('Twitter', 'AccessToken', 0)
AccessSecret = config.get('Twitter', 'AccessSecret', 0)
TweetDbName = config.get('Twitter', 'DbName', 0)
ReweetDbName = config.get('Twitter', 'ReweetDbName', 0)
Filter = config.get('Twitter', 'Filter', 0)
UserIds = config.get('Twitter', 'UserIds', 0)
languages = config.get('Twitter', 'Languages', 0)


Tweetdb = CouchDbServer.create_db(TweetDbName)
ReTweetdb = CouchDbServer.create_db(ReweetDbName)

class listener(StreamListener):
    
    def on_data(self, data):
        
        doc = json.loads(data)
        
        if('lang' in doc):
            if(doc['lang'] not in languages):
                print "Tweet not in specified languages. %s" % doc['text']
                return(True)
            
        if(options.skip_retweets):
            print "This is a retweet so skipping. %s" %  doc['text']
            return(True)
        else:
            if('retweeted_status' in doc):
                print "Saving ReTweet. %s" % doc['text']
                ReTweetdb.save_doc(json.loads(data))
                return(True)
            else:
                print "Saving Tweet. %s" % doc['text']
                Tweetdb.save_doc(json.loads(data))
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



