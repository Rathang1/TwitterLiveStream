from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, explode,split, col
from pyspark.sql.types import StringType
from transformers import AutoModelForSequenceClassification
from transformers import AutoTokenizer
import numpy as np
from scipy.special import softmax
import csv
import urllib.request

# load and use pretraine model
def build_model():
    # this is a roBERTa-base model trained on ~58M tweet_df and finetuned for sentiment analysis with the TweetEval benchmar
    MODEL = "cardiffnlp/twitter-roberta-base-sentiment";

    # downloading tokenizer
    tokenizer = AutoTokenizer.from_pretrained(MODEL)

    # download label mapping
    labels=[]
    mapping = "https://raw.githubusercontent.com/cardiffnlp/tweeteval/main/datasets/sentiment/mapping.txt"
    with urllib.request.urlopen(mapping) as f:
        html = f.read().decode('utf-8').split("\n")
        csvreader = csv.reader(html, delimiter='\t')
    labels = [row[1] for row in csvreader if len(row) > 1]

    # downloading pre trained model
    trained_model = AutoModelForSequenceClassification.from_pretrained(MODEL)

    return tokenizer, trained_model, labels

# evaluating each tweet for getting sentiment
def evaluate_sentiment(text):
    # tokenize the text
    encoded_input = tokenizer(text, return_tensors='pt')
    
    # model predicts the sentiment
    output = model(**encoded_input)
    
    # get scores for each label
    scores = output[0][0].detach().numpy()
    scores = softmax(scores)

    # sort the scores
    ranking = np.argsort(scores)
    ranking = ranking[::-1]
    
    # selecting the label with highest score
    score = 0.0
    for i in range(scores.shape[0]):
        l = labels[ranking[i]]
        s = scores[ranking[i]]
        if s > score:
            sentiment = l

    return sentiment

# function for pre-processing data
def process_data(lines):
    # spitting receive socket into text and tag
    data = lines.select(explode(split(lines.value, "t_end")).alias("temp"))
    tweet_df = data.withColumn('text', split(data.temp, "t_tag").getItem(0))\
        .withColumn('tag', split(data.temp, "t_tag").getItem(1))
    
    # dropping rows having no data
    tweet_df = tweet_df.na.replace('', None)
    tweet_df = tweet_df.na.drop()
    
    # processing tweet_df
    tweet_df = tweet_df.withColumn('text', F.regexp_replace('text', r'http\S+', ''))
    tweet_df = tweet_df.withColumn('text', F.regexp_replace('text', '@\w+', ''))
    tweet_df = tweet_df.withColumn('text', F.regexp_replace('text', '#', ''))
    tweet_df = tweet_df.withColumn('text', F.regexp_replace('text', 'RT', ''))
    tweet_df = tweet_df.withColumn('text', F.regexp_replace('text', ':', ''))
    
    return tweet_df

if __name__ == "__main__":
    # create Spark session
    spark = SparkSession.builder.appName("TwitterSentimentAnalysis").getOrCreate()

    # get model, tokenizer and labels
    tokenizer, model, labels = build_model()
    
    # read the tweet data from socket
    lines = spark.readStream\
        .format("socket")\
        .option("host", "0.0.0.0")\
        .option("port", 5555)\
        .load()
    
    # evaluate sentiment and add to dataframe
    tweet_df = process_data(lines)
    evaluation_udf = udf(evaluate_sentiment, StringType())
    tweet_df = tweet_df.withColumn("value", evaluation_udf("text"))
    tweet_df = tweet_df.select(col["value"])

    # push sentiment from streaming dataframe to kafka 
    output = tweet_df.writeStream.outputName("all_tweet_df")\
        .outputMode('append')\
        .format('kafka')\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("topic", "twitter")\
        .option("checkpointLocation", "checkpoint")\
        .start()
    output.awaitTermination()