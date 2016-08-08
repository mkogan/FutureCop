import mysql.connector
import csv
import pdb
import yaml
import numpy
from datetime import datetime, date
from datetime import timedelta
from yahoo_finance import Share
from collections import defaultdict


db = mysql.connector.connect(host="localhost",    # your host, usually localhost
                     user="mark",         # your username
                     password="root",  # your password
                     database="amibroker")        # name of the data base

cur = db.cursor()


def start():
    with open('config.yaml','r') as f:
        return yaml.load(f)

def dateListBuilder(symbols,sdate,edate):
    stockdates={}
    for stock in symbols:
        cur.execute("select min(date), max(date) from quotes where symbol='" + stock + "' and volume>0;")
        dbdate=cur.fetchall()[0]
        if dbdate[0] == None or dbdate[0]>sdate:
            cur.execute('delete from quotes where symbol="' + stock + '";')
            db.commit()
            stockdates[stock]=[sdate,edate]
        else:
            stockdates[stock]=[dbdate[1]+timedelta(days=1), edate]

    return stockdates

def yahooDownloader(stockdates):
    shareinfo={}

    lastWorkDay=date.today()
    while lastWorkDay.weekday() in [5,6]: lastWorkDay=lastWorkDay-timedelta(1)

    for key in stockdates.iteritems():
        if key[1][0]==lastWorkDay:
            continue
        shareinfo[key[0]]=Share(key[0]).get_historical(key[1][0].strftime('%Y-%m-%d') ,key[1][1].strftime('%Y-%m-%d'))
    return shareinfo

def addingFiller(data, ndays):
    for s in data:
        if len(data[s])==0: continue
        lastDataDay=datetime.strptime(data[s][0]['Date'],'%Y-%m-%d').date()
        lastDataClose=data[s][0]['Close']
        x=1
        while x < ndays:
            if (lastDataDay+timedelta(x)).weekday()==5: x+=2
            data[s].insert(0,{'Volume':0,'Symbol':s,'High':lastDataClose,'Low':lastDataClose,'Date':(lastDataDay+timedelta(x)).strftime('%Y-%m-%d'), 'Close':lastDataClose,'Open':lastDataClose})
            x+=1
    return data

def longDistanceLoading(data):
    q=''
    for s in data:
        if len(data[s])==0: continue
        for daydata in data[s]:
            q="('"+str(daydata['Date'])+"','"+str(daydata['Symbol'])+"',"+str(daydata['Open'])+","+str(daydata['High'])+","+str(daydata['Low'])+","+str(daydata['Close'])+","+str(daydata['Volume'])+")"+q
    q='replace into quotes (date, symbol, open, high, low, close, volume) values '+q.replace(')(','),(')+";"
    cur.execute(q)
    db.commit()


if __name__ == "__main__":
    config=start()
    stockdates=dateListBuilder(config['stocks'],config['dates'][0],config['dates'][1])
    quotedata=yahooDownloader(stockdates)
    quotedatafiller=addingFiller(quotedata,config['daysintofuture'][0])
    longDistanceLoading(quotedatafiller)
    print 'goodbye'
    cur.execute('delete from quotes where date in (select * from holidays) and volume = 0;')
    db.commit()
    cur.close()
    db.close()
    #flistp()
    #save_the_date(build_cyc(flistp()))
