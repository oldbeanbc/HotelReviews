from flask import Flask
from flask import request
import pandas as pd
import numpy as np
from ibm_watson import ToneAnalyzerV3
from ibm_watson import ApiException
import json
from elasticsearch import Elasticsearch
import os

app = Flask(__name__)

@app.route('/get_tone_per_hotel', methods=['Get'])
def get_tone_per_hotel():
    input_hotel_name = request.args.get('name')
    data = load_data()
    reviews = data[data['name']==input_hotel_name]
    tone_summary_json = get_tone(reviews ,True)
    return tone_summary_json

###########################################################################
#take as input all the reviews of one hotel
# get document tone for each review
# accumulate score and count per tone for all reviews in a summary dataframe
# calculate the mean score per tone
###########################################################################
def get_tone(reviews,json_output):
    toness = []
    score = 0
    count = 0
    sum = []

    tone_analyzer = ToneAnalyzerV3(
        version='2017-09-21',
        iam_apikey= 'YWrrmTxtFzUDJAYUk2J93PEUiDC1O8368ec-OQEwuKDO',
        url='https://gateway-lon.watsonplatform.net/tone-analyzer/api'
    )

    #we can enable it, for the sake of speed disabled here
    #tone_analyzer.disable_SSL_verification()

    #***************************************************************************************************
    #all tones listed here : https://github.com/IBM/tone-analyzer-ios/blob/master/README.md
    #(anger, disgust, fear, joy and sadness),
    #social propensities (openness, conscientiousness, extroversion, agreeableness, and emotional range),
    #and language styles (analytical, confident and tentative)
    tone_id=['anger', 'disgust', 'fear', 'joy' ,'sadness' , 'openness', 'conscientiousness', 'extroversion',
           'agreeableness', 'emotional range','analytical', 'confident', 'tentative']


    #we initilaize a new summary object per hotel_reviews
    summary = pd.DataFrame(list(zip(tone_id,[0]*13,[0]*13)), columns= ['tone_id','score','count'])

    try:
        for r in reviews:
            json_output = tone_analyzer.tone({'text': r}, content_type='application/json').get_result()
            i = json_output['document_tone']['tones']
            #each review can have multiple tones
            for j in i:
                t = str(j['tone_id'])
                s = j['score']
                summary.loc[summary['tone_id']==t,'score'] += s
                summary.loc[summary['tone_id']==t,'count'] += 1
                toness.append(json.dumps(i))


        summary['mean']=summary['score']/summary['count']
        # not all tones are detected in the reviews of a hotel,
        # get rid of zero tones.
        summary = summary[summary['score']!=0]

        summary_json = summary.to_json(orient='records')

    except ApiException as ex:
        print "Method failed with status code " + str(ex.code) + ": " + ex.message
    if json_output:
        return summary_json
    else:
        return summary

def load_data():
    data = pd.read_csv('7282_1.csv')
    data= data[data['categories']=='Hotels']
    data=data[:1000][:]
    #reviews = hotel_data['reviews.text']
    #reviews.shape
    return data


@app.route('/get_index', methods=['Get'])
def get_index():
    create_docs()
    response = create_index()
    return response

############################################################################################
# In this method , we create a json document per hotel
# the document includes the hotel informaiton and a new object "reviews" is inserted, whose
# value is an array of all reviews of the hotel
# Added to the hotel information is a new attribute "tone", which includes the tone summary
# collected and calculated based on Watson API
############################################################################################
def create_docs():
    data = pd.read_csv('7282_1.csv')
    data= data[data['categories']=='Hotels']
    data = data[:1000][:]
    # dummy row entered at the end of the dataframe
    # in the loop below, we process the reviews of a hotel (A) only after we have seen all its reviews
    # and we are moving to a new hotel (B). For the last hotel in the dataframe, this is never the case
    # so we insert a dummy row at the end of the dataframe to trigger our processing of the last hotel
    data.append(data.iloc[0][:])

    #initialization used for the algorithm
    #start index for the reviews of the first hotel
    start = 0

    hname = data.at[0,'name']
    haddress= data.at[0,'address']

    #count of files created
    fcount = 0

    for index, row in data.iterrows():
        print (index)
        # we just moved to a new hotel, this means we have the full reviews of the previous hotel
        # they are stored in the rows between indices [start : end]-- end is excluded by Python conventions
        # WHY ( you may ask) I am conditioning on address as well, in the full dataset , hotel names are not unique per address
        if hname!= row['name'] or haddress !=row['address']:
            end= index #this is row index for reviews about a new hotel.
            review_list = pd.DataFrame(list(zip(data[start:end]['reviews.date'],
                                       data[start:end]['reviews.dateAdded'],
                                       data[start:end]['reviews.doRecommend'],
                                       data[start:end]['reviews.id'],
                                       data[start:end]['reviews.rating'],
                                       data[start:end]['reviews.text'],
                                       data[start:end]['reviews.title'],
                                       data[start:end]['reviews.userCity'],
                                       data[start:end]['reviews.username'],
                                       data[start:end]['reviews.userProvince'])),columns=['reviews.date','reviews.dateAdded',
                                                                                          'reviews.doRecommend','reviews.id',
                                                                                          'reviews.rating','reviews.text',
                                                                                          'reviews.title','reviews.userCity',
                                                                                   'reviews.username','reviews.userProvince',])


            obj1 = review_list.to_json(orient='records')
            hotel= {}
            hotel['name']= row['name']
            hotel['categories']=row['categories']
            hotel['address']=row['address']
            hotel['city']=row['city']
            hotel['province']=row['province']
            hotel['country']=row['country']
            hotel['postalCode']=row['postalCode']
            hotel['latitude']=row['latitude']
            hotel['longitude']=row['longitude']
            hotel['toneSummary']=get_tone(review_list,False)

            obj2 = pd.DataFrame.from_dict(hotel,orient='index').to_json()
            # This is dirty code, should be improved by merging the two jsons in memory. I am using python 2. so life is hard
            # So below I am removing the closing brace of the first json object and inserting the array of reviews as the
            # last field, then closing the brace for the initial json object. Not Cleanest code :'(
            obj3 = obj2[:-2]+ ','+ "\"reviews\" : " + obj1 + '}}'


            #yup, some hotel names have backslashes. Never trust the data. Good we dont have dead guys in there.
            modified_name = hotel['name']
            modified_name = modified_name.replace("/",".")

            with open("./HotelDocs/"+modified_name+str(index)+".json", "w") as text_file:
                #dumping json as json file insert unnecessary escape characters??
                print (modified_name)
                text_file.write(obj3)
                #json.dump(obj3,text_file)
            fcount +=1

            #updating my counters and vars for the next iteration.
            start = index
            hname = row['name']
            haddress= row['address']
            #json_data = json.dumps(data)
            #print (row['name'],row['address'])

            #this should return the count of files created
    return fcount

###########################################################################################
###  Create_Index
#accesses a local folder where we stored json documents created by create_docs()
#uses **local Elastic Search Server** to create indices
### assumptions:
#1- elastic search server is running at the default location localhost:9200
#2- json documents directoy is ./HotelDocs
### response:
#a confirmation on shard state if creation successful
#error message if index already exists
############################################################################################
def create_index():
    # by default we connect to localhost:9200
    es = Elasticsearch()

    # create an index in elasticsearch, ignore status code 400 (index already exists)
    #es.indices.create(index='Hotel-index', ignore=400)
    #{'acknowledged': True, 'shards_acknowledged': True, 'index': 'hotel-index'}

    INDEX_NAME = 'hotelsindex'
    response = es.indices.create(index=INDEX_NAME)
    print(response)


    directory = './HotelDocs/'
    i = 1
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            #filename.replace("/",".")
            f = open(directory+filename)
            docket_content = f.read()
            # Send the data into es
            es.index(index='hotelsindex', ignore=400, doc_type='docket', body=json.loads(docket_content))
            i = i + 1

    print('list of indices:')
    for index in es.indices.get('*'):
        print index

    return response
