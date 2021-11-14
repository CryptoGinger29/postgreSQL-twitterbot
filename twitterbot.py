# -*- coding: utf-8 -*-
"""
Created on Mon Nov  8 13:53:21 2021

@author: KPRU
"""

import psycopg2
import pandas as pd
import tweepy
import requests
import configparser
import os
from datetime import timedelta
import argparse

#%%
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)
#%%

parser = argparse.ArgumentParser(description='A test program.')

parser.add_argument("--t", help="tweets a tweet from postgress", action="store_true")
parser.add_argument("--s", help="clean up tweet timeline", action="store_true")
parser.add_argument("--d", help="toggles dry run", action="store_true")

args = parser.parse_args()

#%%
class twitterbot:
    def __init__(self,args):
        #%% pull config
        #save arg
        self.args=args
        #
        self.config = configparser.ConfigParser()
        configpath=os.path.join("config","config.ini")
        self.config.read(configpath)
        # getting postgres config
        self.confpostgre=self.config["postgresql"]

        self.conn=psycopg2.connect(host=self.confpostgre["host"],
                              port=self.confpostgre["port"],
                              database=self.confpostgre["database"],
                              user=self.confpostgre["user"],
                              password=self.confpostgre["password"])

        #getting config for twitter API
        self.conftwitter=self.config["twitter"]

        # Authenticate to Twitter
        self.auth = tweepy.OAuthHandler(self.conftwitter["consumerkey"], 
                                   self.conftwitter["consumersecret"])
        self.auth.set_access_token(self.conftwitter["accesstoken"], 
                              self.conftwitter["accesstokensecret"])
        
        self.api = tweepy.API(self.auth)
    
    def authenticate(self):
        self.authenticated=False
        try:
            self.api.verify_credentials()
            self.authenticated=True
            print("Authentication OK")
        except:
            print("Error during authentication")
        return self.authenticated
        
    def posttweet(self):
        print("Starting tweet routine")
        # A sample query of all data from the "vendors" table in the "suppliers" database
        df=pd.read_sql_query("SELECT tt.id as id ,tt.body as body,tt.fileurl as fileurl,tt.lastsend as lastsend,tf.text as footer,tt.category as category FROM (tbl_tweets tt LEFT JOIN tbl_footer tf ON tf.id=tt.category) WHERE tt.active='1';",self.conn)
        # Close the cursor and connection to so the server can allocate
        #%% setting up the tweet
        
        df["tweet"]=df["body"] + "\r\n\r\n" +  df["footer"] + "\r\n#" + df["id"].astype(str)
        
        #%% setting up probability based on time since last send
        
        df["prob"]=(pd.to_datetime("now")-pd.to_datetime(df["lastsend"]).fillna(pd.to_datetime("2000-01-01 00:00:00.00000"))).dt.total_seconds()
        df["prob_norm"]=(df["prob"]-df["prob"].min())/(df["prob"].max()-df["prob"].min())
        #%%
        
        df["len"]=df["tweet"].str.len()
        mask=df["len"]>280
        
        if sum(mask)>0:
            print("DB contains tweets that exceed the 280 character limit")
        
        mask=df["len"]<=280
        
        df=df[mask].copy()
        #%% ensures to not spam twitter with a controlled max amount of tweets per category
        df["lastsend"]=pd.to_datetime(df["lastsend"])  
        
        df_limit=pd.read_sql_query("SELECT id, dailylimit FROM tbl_footer;",tb.conn)
        df_limit=df_limit.set_index("id")
        
        # controlling that the max number of tweet limit is respected
        tempmask=df["lastsend"]>=(pd.to_datetime("now")-timedelta(hours=24))
        temp_df=df[tempmask]
        df_count=temp_df['category'].value_counts()
        
        # find allowed
        allowed=[]
        for i in df_count.index:
            count=df_count.loc[i]
            limit=df_limit.loc[i]["dailylimit"]
            if count<limit:
                allowed.append(i)
        
        # filter df
        df=df[df["category"].isin(allowed)].copy()
        #%%
        # selects tweet to send
        if len(df.index)>0:
            if len(df.index)>1:
                self.selectedtweet=df.sample(1,weights="prob_norm")
            else:
                self.selectedtweet=df

        #%%
        #if is not a dryrun, try to tweet
        if not self.args.d and self.authenticate() and len(df.index)>0:
            filextension=self.selectedtweet.values[0].split(".")[-1]
            pathtogif=os.path.join('temp','tweet.' + filextension)
            mediadownloaded=False
            try:
                with open(pathtogif, 'wb') as f:
                    f.write(requests.get(self.selectedtweet["fileurl"].values[0]).content)
                
                mediadownloaded=True
            except Exception as e :
                print("Error in gif download")
                print(e)
            
            #if the media was succesfully downloaded
            tweetsuccess=False
            if mediadownloaded:
                try:
                    txt=self.selectedtweet["tweet"].values[0]
                    self.api.update_status_with_media(status=txt,filename=pathtogif)
                    tweetsuccess=True
                    print("Successfully tweeted!")
                except Exception as e:
                    print("Error in tweet send")
                    print(e)
            else:
                try:
                    txt=self.selectedtweet["tweet"].values[0]
                    self.api.update_status(status=txt)
                    tweetsuccess=True
                    print("Successfully tweeted!")
                except Exception as e:
                    print("Error in tweet send")
                    print(e)                    
            
            
            #update last send date with current time if tweet was succesfully submitted
            if tweetsuccess:
                try:
                    sql_template = "UPDATE tbl_tweets SET lastsend = '{}' WHERE id = {}".format(pd.to_datetime("now"),self.selectedtweet["id"].values[0])
                    cur = self.conn.cursor()
                    cur.execute(sql_template)
                    self.conn.commit()
                    cur.close()
                    print("Successfully updated postgres!")
                except Exception as e:
                    print("Error in postgreSQL write")
                    print(e)
        elif len(df.index)==0:
            print("Not allowed to sent any tweets as rolling daily has been met")
#%%
    def spamremover(self):
        print("Starting tweet clean up routine")
        if self.authenticate():
            ls_tweets=self.api.user_timeline()
            
        
        return ls_tweets
#%%
        
if __name__=="__main__":
    tb=twitterbot(args)
    
    if args.t:
        tb.posttweet()
    if args.s:
        tb.spamremover()