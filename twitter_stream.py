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

#Config
ConsumerKey = config.get('Twitter', 'ConsumerKey', 0)
ConsumerSecret = config.get('Twitter', 'ConsumerSecret', 0)
AccessToken = config.get('Twitter', 'AccessToken', 0)
AccessSecret = config.get('Twitter', 'AccessSecret', 0)
TweetDbName = config.get('Twitter', 'DbName', 0)
ReweetDbName = config.get('Twitter', 'ReweetDbName', 0)
Filter = config.get('Twitter', 'Filter', 0)
UserIds = config.get('Twitter', 'UserIds', 0)
Languages = config.get('Twitter', 'Languages', 0)


Tweetdb = CouchDbServer.create_db(TweetDbName)
ReTweetdb = CouchDbServer.create_db(ReweetDbName)

#Print Red
def red(name): print ("\033[91m {}\033[00m" .format(name))

#Print Green
def green(name): print ("\033[92m {}\033[00m" .format(name))

#PRint Yellow
def yellow(name): print ("\033[93m {}\033[00m" .format(name))

class listener(StreamListener):
    
    def on_data(self, data):
        #Load Json
        doc = json.loads(data)
        
        #Check the Tweet has content
        if('text' in doc):
            
            #Check the Tweet is in our specified Language(s)
            if('lang' in doc):
                
                if(doc['lang'] not in Languages):
                    #Skip
                    red("Tweet not in specified languages.")
                    print " %s" % doc['text']
                    return(True)
                
            #Check if we want ReTweets
            if(options.skip_retweets):
                yellow("This is a retweet so skipping.")
                print "%s" %  doc['text']
                return(True)
            else:
                if('retweeted_status' in doc):
                    green("Saving ReTweet. ")
                    print "%s" % doc['text']
                    ReTweetdb.save_doc(doc)
                    return(True)
                else:
                    green("Saving Tweet.")
                    print " %s" % doc['text']
                    Tweetdb.save_doc(doc)
                    return(True)

    def on_error(self, status):
        #Print http status code
        print status

auth = OAuthHandler(ConsumerKey, ConsumerSecret)
auth.set_access_token(AccessToken, AccessSecret)

#Handle exit
try:
    twitterStream = Stream(auth, listener())

    twitterStream.filter(track=[Filter], follow=[UserIds])
except (KeyboardInterrupt, SystemExit):
    print "Exiting..."



