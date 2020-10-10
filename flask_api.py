#!flask/bin/python
from flask import Flask, jsonify
from read_tweets import download
import subprocess
import sys
import time
import datetime
import os
import json
import glob
import flask

from celery import group
#from tasks import add

app = Flask(__name__)


@app.route('/analyse_tweets', methods=['GET'])
def analyse_tweets():
    #output_filename =  "{:%Y-%m-%d-%H-%M-%S}".format(datetime.datetime.now()) + ".txt" #"2020-10-06-21-11-24.txt" #
    BASEDIR ="/home/ubuntu/data/data"
    directory = glob.glob(BASEDIR + "/*")
    
    '''files = directory[0]
    
    #return (entry for entry in open(files,"r").readlines())
    tweets_inp = open(files,"r").readlines()
    tweets = []
    for entry in tweets_inp:
        if entry.strip() != "":
            json_line = json.loads(entry)
            tweets.append(json_line)
            json_inp = json.loads(entry)
            
    #return (str(len(json_line)))
    #return (download.delay(one_line).get(timeout=1))

    #return group( download.s(entry for entry in open(files,"r").readlines()))().get()
    #return group(download.s(files for files in directory)().get())
    
    #data = download.delay(directory)'''
    
    '''
    data = download.chunks(zip(directory), 1).group()
    
    output = data().get()
    
    output_dict = {}
    #return (str(output[0][0]))
    for entry in output:
        #return (str(entry))
        for key in entry[0]:
            if key not in output_dict:
                output_dict[key] = entry[0][key]
            else:
                output_dict[key] += entry[0][key]
                        
    return (str(output_dict))
    '''    
    
    data = download.chunks(zip(directory), 1).apply_async()
    
    #data.get()
    
    #return (str(data.ready()))
    while True:
        time.sleep(1)
        if data.ready():
            output = data.get()
    
            output_dict = {}
            #return (str(output[0][0]))
            for entry in output:
                #return (str(entry))
                for key in entry[0]:
                    if key not in output_dict:
                        output_dict[key] = entry[0][key]
                    else:
                        output_dict[key] += entry[0][key]
                        
            return (str(output_dict))
    
        
    '''
            file_open = open("/home/ubuntu/data/output_celery/" + output_filename,"r").readline()
                    
            tweet_count = json.loads(file_open)
                
            frequency_dict = {}
            for word in tweet_count:
                if word != "unique_tweets":
                    frequency_dict[word] = float(tweet_count[word]/float(tweet_count["unique_tweets"]))
                        
            return (str(file_open) + "\n" + str(frequency_dict) + "\n")'''
                #file_path = "/home/ubuntu/data/output_celery/" + output_filename
                
                #return (flask.send_from_directory("/home/ubuntu/data/output_celery/",output_filename, as_attachment=True))#, mimetype="image/png", as_attachment=True, attachment_filename=output_filename))
            
            #return (file_open)
            
@app.route('/get_output/<filename>', methods=['GET'])
def get_output(filename):
    open_file = open("/home/ubuntu/data/output_celery/" + filename,"r").read()
    return open_file


@app.route('/list_files', methods=['GET'])
def list_files():
    file_list = str(os.listdir("/home/ubuntu/data/output_celery/")).strip("[]")
    return file_list


if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)
