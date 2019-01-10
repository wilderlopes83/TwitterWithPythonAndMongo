import json
import tweepy
import datetime
import time
from tweepy import OAuthHandler
from tweepy import Stream
from datetime import datetime
from captura import Captura
from pymongo import MongoClient

def run():
    consumer_key = "14I4HIvVFHzKUdE31cLMCElRD"    
    consumer_secret = "U8bNwqkQV5MDV9KxXwCgkHn3bN7AeAmbb5QdU7RQuH15gdxaXI"
    access_token = "219641776-fkbcpgyCZEztKbtV3QTN31EBXNUP46UQ2GLYLI4Q"
    access_token_secret = "bFM35tBhDXFRS0X1xfriWL6GvYznkyyMptL0f6fpIZrbe"

    autenticacao = OAuthHandler(consumer_key, consumer_secret)
    autenticacao.set_access_token(access_token, access_token_secret)   

    client = MongoClient('localhost', 27017)
    db = client.bancoTwitter
    colTwitter = db.tweets

    myListener = Captura()
    myListener.setCollectionMongo(colTwitter)
    myStream = Stream(autenticacao, listener=myListener)

    keywords = ['Vasco', 'Palmeiras', 'Fluminense', 'Santos']

    print("iniciando leitura do twitter")    
    myStream.filter(track=keywords, is_async=True)
    #força 10 segundos rodando pesquisa no twitter
    time.sleep(10)
    myStream.disconnect()
    print("fim leitura do twitter")

    dataset = [{"created_at":item["created_at"], "text":item["text"],} for item in colTwitter.find()]

    print(dataset)

if __name__ == '__main__':
    run()