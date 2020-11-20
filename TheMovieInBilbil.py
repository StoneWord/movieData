import requests
from bs4 import BeautifulSoup
import re
import time
import sqlite3

data_all = []
movieScores = []
dbpath = "moive.db"
def init_db(dbpath):
    sql = '''
        create table moivedata
        (
            theorder integer primary key,
            name text,
            movieScore real,
            time text,
            hot text
        )
    '''
    conn = sqlite3.connect(dbpath)
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()

print("***开始爬取数据***")
mainurl = "https://www.bilibili.com/v/popular/rank/movie?spm_id_from=333.851.b_62696c695f7265706f72745f6d6f766965.39"
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36'}
douban_data = requests.get(mainurl, headers=headers)
soup = BeautifulSoup(douban_data.text, 'lxml')

orders = soup.select('#app > div.rank-container > div.rank-list-wrap > ul > li > div.num')
names = soup.select('#app > div.rank-container > div.rank-list-wrap > ul > li > div.content > div.info > a')
times = soup.select('#app > div.rank-container > div.rank-list-wrap > ul > li > div.content > div.info > div.pgc-info')
hots = soup.select('#app > div.rank-container > div.rank-list-wrap > ul > li > div.content > div.info > div.pts > div')
peoples = soup.select('#bilibiliPlayer > div.bilibili-player-area.video-state-pause.video-state-blackside.video-control-show > div.bilibili-player-video-bottom-area > div > div.bilibili-player-video-info > div.bilibili-player-video-info-people > span.bilibili-player-video-info-people-number')

i = 1
for name in names:
    link = re.findall(r'href="([^"]+)"', str(name))
    parturl = "https:"+link[0]

    data = requests.get(parturl, headers=headers)
    soup1 = BeautifulSoup(data.text, 'lxml')
    scores = soup1.select("#media_module > div > div.media-rating > h4")
    peoples = soup1.select('#bilibiliPlayer > div.bilibili-player-area.video-state-pause.video-state-blackside.video-control-show > div.bilibili-player-video-bottom-area > div > div.bilibili-player-video-info > div.bilibili-player-video-info-people > span.bilibili-player-video-info-people-number')

    print("Progress {:2.1%}".format(i / 100))
    time.sleep(1)
    i += 1
    if(scores!=[]):
        for score in scores:
            if(score.get_text() == "暂无评分"):
                movieScores.append("1")
            else:
                movieScores.append(score.get_text())
    else:
        movieScores.append("0")

print("orders len:"+str(len(orders)))
print("names len:"+str(len(names)))
print("movieScores len:"+str(len(movieScores)))
print("times len:"+str(len(times)))
print("hots len:"+str(len(hots)))

conn = sqlite3.connect(dbpath)
c = conn.cursor()

init_db(dbpath)
for order, name,movieScore ,time, hot in zip(orders,names,movieScores,times,hots):

    sql = '''
        insert into moivedata (theorder, name, movieScore, time, hot) VALUES
        (?,?,?,?,?)
    '''
    #(int(order.get_text()), name.get_text(), movieScore, time.get_text(), hot.get_text()))
    c.execute(sql, (int(order.get_text()), name.get_text(), float(movieScore), time.get_text(), hot.get_text()))
    conn.commit()

    data = {
        'order': order.get_text(),
        'name': name.get_text(),
        'movieScore': movieScore,
        'time': time.get_text(),
        'hot': hot.get_text()
    }
    data_all.append(data)
conn.close()



for i in range(0,len(data_all)):
   print(data_all[i])


print("***爬取数据结束***")

