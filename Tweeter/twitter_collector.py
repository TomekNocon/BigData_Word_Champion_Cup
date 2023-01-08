#!/usr/bin/python
import snscrape.modules.twitter as sntwitter
import pandas as pd
import json
def main():
	tweets_list1 = []

	for i, tweet in enumerate(sntwitter.TwitterSearchScraper("World cup").get_items()):
		if i > 1000: 
			break
		
		el =	{"date": tweet.date.strftime("%Y/%m/%d %H:%M:%S"),
                	"id": tweet.id,
                	"content": tweet.content.replace("'", ''),
                	"username": tweet.user.username,
                	"replyCount": tweet.replyCount,
                	"retweetCount": tweet.retweetCount,
                	"likeCount": tweet.likeCount,
                	"tweetCount": tweet.quoteCount,
                	"lang": tweet.lang,
	               	"retweetedTweet": str(tweet.retweetedTweet)
			}
		el = json.dumps(el)
		tweets_list1.append(el)
		print(tweets_list1[-1])
	return tweets_list1

if __name__ == "__main__":
	main()
