import json
import tweepy
from tweepy.streaming import StreamListener


class Captura(StreamListener):

    def setCollectionMongo(self, collection):
        self.colTwitter = collection

    def on_data(self, dados):
        tweet = json.loads(dados)
        created_at = tweet["created_at"]
        id_str = tweet["id_str"]
        text = tweet["text"]
        obj = {"created_at":created_at, "id_str":id_str,"text":text,}
        tweetind = self.colTwitter.insert_one(obj).inserted_id
        print(obj)
        return True