from celery import Celery
import json,glob


dir_path = '/home/ubuntu/data/*'

app = Celery('tasks', backend='amqp', broker='amqp://')

@app.task
def counts():
    pronounDict = {"den": 0, "denna": 0, "denne": 0, "det": 0, "han": 0, "hen": 0, "hon": 0}
    for files in glob.glob(dir_path):
        with open(files) as f:
            lines = f.readlines()
            lines = [x.strip() for x in lines]
            lines = [x for x in lines if len(x) != 0]

            tweetsDict = [json.loads(x) for x in lines]
            tweets = [x for x in tweetsDict if "retweeted_status" not in tweetsDict]
            tweets = [x["text"].strip() for x in tweets]
            tweets = [x.lower() for x in tweets]
            tweetToken = [x.split() for x in tweets]

            for lists in tweetToken:
                for words in lists:
                    if words in pronounDict:
                        pronounDict[words] += 1


    countsJson = json.dumps(pronounDict)
    return countsJson