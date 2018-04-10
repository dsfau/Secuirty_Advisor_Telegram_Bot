#!/usr/bin/python
# -*- coding: utf-8 -*-
#CREATE TABLE IF NOT EXISTS advisors(
#name text NOT NULL,
#id integer NOT NULL,
#title text NOT NULL,
#link text NOT NULL);
import feedparser
import yaml
import sqlite3
import time
import sys
import os
import threading
import requests
import json
import re


class Output:
    def error(self, msg):
        sys.stderr.write("[X] {0}\n".format(msg))
        sys.exit(1)

    def warning(self, msg):
        print("[!] {0}".format(msg))

    def info(self, msg):
        print("[+] {0}".format(msg))

    def report(self, msg):
        print(msg)


class Feed:

    def __init__(self, feed_conf, db,verbose=False):
        self.conf=feed_conf
        try:
            self.frecuency = self.conf['check_updates']
        except:
            self.frecuency = 60
        try:
            self.regex_id = re.compile(self.conf['idregex'])
        except:
            self.regex_id = None
        self.name = self.conf['name']
        self.url = self.conf['url']
        self.verbose = verbose
        self.db = db
        self.reportedPost = []
        self.out = Output()
        self.reportedPostInDb()


    def dbexecute(self,query):
        con = sqlite3.connect(self.db)
        c = con.cursor()
        c.execute(query)
        if query[:2].upper() == "IN":
            con.commit()
            con.close()
            return 0
        elif query[:2].upper() == "SE":
            r = c.fetchall()
            con.close()
            return r
        else:
            if self.verbose:
                self.out.error("Unknow sql command")


    def __output_control__(self, msg, typem):
        type_msg = typem.upper()
        if type_msg == "W": #warning
            print("[!] {0}".format(msg))
        elif type_msg == "E": #Error
            sys.stderr.write("[X] {0}\n".format(msg))
            sys.exit(1)
        elif type_msg =="I": #Info
            print("[+] {0}".format(msg))
        elif type_msg == "R": #result
            print(msg)


    def getLastPost(self):
        try:
            feed = feedparser.parse(self.url)
            last_post = feed.entries[0]
            return last_post
        except:
            if self.verbose:
                self.out.error("Unable parser the feed {0}".format(self.name))


    def reportedPostInDb(self):
        query = "SELECT id FROM advisors WHERE name='{0}'".format(self.name)
        reported_in_db=self.dbexecute(query)
        for i in reported_in_db:
            self.reportedPost.append(i[0])


    def addReportedPost(self, id, title, link):
        query = "INSERT INTO advisors VALUES('{name}', '{id}', '{title}', '{link}')".format(name=self.name, id=id, title=title, link=link)
        self.dbexecute(query)
        self.reportedPost.append(id)


    def checkUpdatesReturnNews(self):
        post = self.getLastPost()
        while True:
            if self.regex_id is not None:
                post_id = self.regex_id.findall(post[self.conf['id']])[0]
            else:
                post_id = post[self.conf['id']]
            if post_id not in self.reportedPost:
                self.addReportedPost(post_id, post[self.conf['title']], post[self.conf['link']])
                yield post
            time.sleep(self.frecuency)


class TelBot():
    def __init__(self, api_key):
        self.api_key = api_key
        self.url = "https://api.telegram.org/bot" + self.api_key + "/"


    def callApi(self, function, data):
        r = requests.post(self.url+function, data=data)
        if r.status_code == 200:
            try:
                return r.json()
            except:
                return
        print r.text


    def sendMessage(self, text, id_r):
        data = {}
        data['chat_id'] = id_r
        data['text'] = text
        self.callApi("sendMessage", data)

def config(file="./config.yaml"):
    if os.path.exists(file):
        with open(file, 'r') as f:
            c = yaml.safe_load(f)
            return c
    else:
        Output().error("Error: configuration file not found")


def notify(text):
    conf = config()
    tbot = TelBot(conf['telegram']['api_key'])
    for i in conf['telegram']['receivers_ids']:
        tbot.sendMessage(id_r = conf['telegram']['receivers_ids'][i], text = text)


def worker(conf, db):
    f = Feed(feed_conf=conf, db=db, verbose=True)
    g = f.checkUpdatesReturnNews()
    while True:
        notify(g.next()['link'])
        notify(g.next()['title'])


if __name__ == "__main__":
    conf = config()
    threads = list()
    for i in conf['feeds']:
        t = threading.Thread(target=worker,args=(conf['feeds'][i], conf['path_db']), name=i)
        threads.append(t)
        t.start()
