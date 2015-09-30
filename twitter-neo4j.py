from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import ConfigParser
import json
from optparse import OptionParser
from py2neo import Graph, authenticate
from httplib import IncompleteRead
from requests.packages.urllib3.exceptions import ProtocolError
import time


authenticate("localhost:7474", "neo4j", "pa55word")
graph = Graph()

# clear database
#graph_db.clear()
   
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

#Print Red
def print_red(text): print ("\033[91m {}\033[00m" .format(text))

#Print Green
def print_green(text): print ("\033[92m {}\033[00m" .format(text))

#PRint Yellow
def print_yellow(text): print ("\033[93m {}\033[00m" .format(text))

class listener(StreamListener):
    
    def on_data(self, data):
        #Load Json
        tweet = json.loads(data)
       
        if all (key in tweet for key in ("id","text","created_at","user")):

            query = """
            UNWIND {tweet} AS t
            WITH t
            ORDER BY t.id
            WITH t,
                 t.entities AS e,
                 t.user AS u,
                 t.retweeted_status AS retweet
            MERGE (tweet:Tweet {id:t.id})
            SET tweet.text = t.text,
                tweet.created_at = t.created_at,
                tweet.favorites = t.favorite_count
            MERGE (user:User {screen_name:u.screen_name})
            SET user.name = u.name,
                user.location = u.location,
                user.followers = u.followers_count,
                user.following = u.friends_count,
                user.statuses = u.statusus_count,
                user.profile_image_url = u.profile_image_url
            MERGE (user)-[:POSTS]->(tweet)
            MERGE (source:Source {name:t.source})
            MERGE (tweet)-[:USING]->(source)
            FOREACH (h IN e.hashtags |
              MERGE (tag:Hashtag {name:LOWER(h.text)})
              MERGE (tag)-[:TAGS]->(tweet)
            )
            FOREACH (u IN e.urls |
              MERGE (url:Link {url:u.expanded_url})
              MERGE (tweet)-[:CONTAINS]->(url)
            )
            FOREACH (m IN e.user_mentions |
              MERGE (mentioned:User {screen_name:m.screen_name})
              ON CREATE SET mentioned.name = m.name
              MERGE (tweet)-[:MENTIONS]->(mentioned)
            )
            FOREACH (r IN [r IN [t.in_reply_to_status_id] WHERE r IS NOT NULL] |
              MERGE (reply_tweet:Tweet {id:r})
              MERGE (tweet)-[:REPLY_TO]->(reply_tweet)
            )
            FOREACH (retweet_id IN [x IN [retweet.id] WHERE x IS NOT NULL] |
                MERGE (retweet_tweet:Tweet {id:retweet_id})
                MERGE (tweet)-[:RETWEETS]->(retweet_tweet)
            )
            """
             # Send Cypher query.
            graph.cypher.execute(query, tweet=tweet)
            
            
            print_green("Saving Tweet.")
            print " %s" % tweet['text']
        
            return(True)
        else:
            print_red("Tweet doesn't contain data needed")
        
       
            
        
    def on_error(self, status):
        #Print http status code
        print status

auth = OAuthHandler(ConsumerKey, ConsumerSecret)
auth.set_access_token(AccessToken, AccessSecret)

while True:
    try:
        twitterStream = Stream(auth, listener())
        twitterStream.filter(track=[Filter], follow=[UserIds])
        
    except IncompleteRead:
        print_red("IncompleteRead caught. Waiting......")
        continue
    except ProtocolError:
        print_red("Protocol Error caught. Waiting......")
        time.sleep(2)
    except (KeyboardInterrupt,SystemExit):
        stream.disconnect()
        print "Exiting..."
        break





