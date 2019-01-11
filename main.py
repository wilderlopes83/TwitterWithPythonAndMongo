import json
import tweepy
import datetime
import time
import pandas as pd
from tweepy import OAuthHandler
from tweepy import Stream
from datetime import datetime
from captura import Captura
from pymongo import MongoClient
from sklearn.feature_extraction.text import CountVectorizer

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

    keywords = ['Vasco', 'Palmeiras', 'Fluminense', 'Corinthians', 'Flamengo']

    print("iniciando leitura do twitter")    
    myStream.filter(track=keywords, is_async=True)
    #força 10 segundos rodando pesquisa no twitter
    time.sleep(10)
    myStream.disconnect()
    print("fim leitura do twitter")

    dataset = [{"created_at":item["created_at"], "text":item["text"],} for item in colTwitter.find()]

    #criando dataframe com os dados
    df = pd.DataFrame(dataset)

    #imprimindo o dataframe
    #print(df)

    #vetorizando os documentos
    cv = CountVectorizer()
    matriz = cv.fit_transform(df.text)

    #contando o número de ocorrências das principais palavras em nosso dataset
    word_count = pd.DataFrame(cv.get_feature_names(), columns=["word"])
    word_count["count"] = matriz.sum(axis=0).tolist()[0]
    word_count = word_count.sort_values("count", ascending=False).reset_index(drop=True)
    print(word_count[:50])
    

if __name__ == '__main__':
    run()