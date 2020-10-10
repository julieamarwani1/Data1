from celery import Celery
import os
import json
import glob


# Where the downloaded files will be stored
BASEDIR ="/home/ubuntu/data"
output_dir = "/home/ubuntu/data/output"

# Create the app and set the broker location (RabbitMQ)
app = Celery('downloaderApp',
             backend='amqp',
             broker='amqp://julie:12345678@192.168.2.57/juliehost')

app.conf.update(
    CELERY_RESULT_BACKEND='amqp'
)

@app.task(ignore_result=True)
def download(filename):
    """
    Download a page and save it to the BASEDIR directory
      url: the url to download
      filename: the filename used to save the url in BASEDIR
    """
    pronoun_list = ["han", "hon", "den", "det", "denna", "denne","hen"]
    
    unique_tweets = 0
    pronoun_dict = {}
    
    
    #tweet_files = glob.glob(BASEDIR+"/*")
    
    #for tweet_file in tweet_files:
    open_file = open(filename,'r').readlines()
    data = read_input(open_file)
    
    for count,line in enumerate(data):
        tweet = json.loads(line)
        try :
            if tweet["retweeted_status"]:
                None
        except:
            unique_tweets += 1 #print ("unique_tweets\t1")
            pure_words = pureword(tweet["text"]).split()
            for pronoun in pronoun_list:
                if pronoun in pure_words:
                    if pronoun.lower() not in pronoun_dict:
                        pronoun_dict[pronoun.lower()] = 1
                    else:
                        pronoun_dict[pronoun.lower()] += 1
        
            pronoun_dict["unique_tweets"] = unique_tweets
    
    return (pronoun_dict)
    #output_file = open(output_dir+"/"+filename,"w")
    #json.dump(pronoun_dict, output_file)
    #output_file.close()
    
    
@app.task
def list():
    """ Return an array of all downloaded files """
    return os.listdir(BASEDIR)

 
def read_input(file):
    for line in file:
        if line.strip() != "":
            yield line
            
def pureword(text):
    output_word = ""
    for letters in text:
        if letters.isalpha():
            output_word += letters
        else:
            output_word += " "
    return (output_word.lower())
