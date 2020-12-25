from flask import Flask
import time as tt
import datetime
import calendar

from werkzeug.serving import run_simple

import requests
import json
import csv
import pandas as pd
import json
from pandas.io.json import json_normalize
import re
from pygeocoder import Geocoder
import pycountry
import flatdict
import pandas as pd
import arcgis
from arcgis.gis import GIS
from IPython.display import display
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import re
import gzip
import shutil
import requests
from math import sin, cos, sqrt, atan2, radians
app = Flask(__name__)

@app.route('/display/<latitude>,<longitude>,<time>,<param>')


def display(latitude,longitude,time,param):
    gis=GIS()
    map1 = gis.map("US", zoomlevel = 8)
    def genericfun(latitude,longitude,time):
        try:
            #print("genericfun\n")
            string="https://api.darksky.net/forecast/68d70a9cf38161567cc4aec124be92ed/"+latitude+","+longitude+","+time+"?exclude=hourly,currently,minutely,alert,flags"

            response = requests.get(string)
            data=response.content
            if(len(data)<140):
                jdf1={}
                #print("genericfun_if\n")
                return jdf1
            else:
                #print("genericfun_else\n")
                parsed_data=pd.read_json(data.decode('utf-8'))

                new=parsed_data['daily'][0]
                new1=pd.DataFrame(new)
                latl=[]
                longl=[]
                for i in range(len(new1)):
                    latl.append(latitude)
                    longl.append(longitude)
                new1['lat']=latl
                new1['long']=longl
                #map1 = gis.map("US", zoomlevel = 8)
                df=new1
                light = gis.content.import_data(df)
                light.layer.layers = [dict(light.layer)]
                map1.add_layer(light)
                try1=light.query()
                dfn=((try1).df)
                del dfn['SHAPE']
                js=dfn.to_json(orient='index')
                jdf=json.loads(js)
                jdf1=jdf["0"]
                del (jdf1["lat"])
                del (jdf1["long"])
                del jdf1["summary"]
                return jdf1
        except:
                jdf1={}
                return jdf1
        #if(param==generic then return jdf1)before it was light...........changed name to generic
   
    #air
    def airfun(latitude,longitude,time):
        try:
            #print("airfun\n")
            response = requests.get("https://api.openaq.org/v1/measurements?coordinates="+latitude+","+longitude+"&radius=28000")
            variable=str(response.content)
            #print(variable)
            if(len(variable)<140):
                #print("airfun_if\n")
                jdf2={}
                return jdf2
            else:
                #print("airfun_else\n")
                xizz=variable[2:-1].encode('utf-8')
                xiz=xizz.decode('utf-8')
                st1=re.sub('"unit":".*?",', '', xiz)
                st=re.sub(',"city":".*?"}', '}', st1)
                data = json.loads(st)
                df = json_normalize(data, 'results')
                dict1=list(df['coordinates'])
                df2=pd.DataFrame(dict1)
                df['lat']=df2['latitude']
                df['long']=df2['longitude']
                del df['coordinates']
                dflist = df['date'].tolist()
                l_data=[]
                l_local=[]
                for i in dflist:
                    l_data.append(i['local'][:10])
                for i in dflist:
                    l_local.append(i['local'][20:])
                del df['date']
                del df['location']
                df4 = pd.DataFrame()
                df4['dates']=l_data
                df4['local']=l_local
                df['date']=df4['dates']
                df['local']=df4['local']
                epoch=[]
                for i in range(len(df)):
                    date_time = df['date'][i]
                    t = int(tt.mktime(tt.strptime(str(df['date'][i]), "%Y-%m-%d")))
                    epoch.append(t)
                df['epocht']=epoch
                df.to_csv("final.csv")
                gis=GIS()
                #map1 = gis.map("Amsterdam", zoomlevel = 8)
                df = pd.read_csv('final.csv')
                airpol = gis.content.import_data(df)
                airpol.layer.layers = [dict(airpol.layer)]
                map1.add_layer(airpol)
                try1=airpol.query().df
                del try1['SHAPE']
                del try1['Unnamed__0']
                time1=0
                time1=int(time)
                min1=99999999999
                key1=0
                newset=set(try1['parameter'])
                newsetl=list(newset)
                valuesl= [0] * len(newsetl)
                for i in range(len(try1['epocht'])):
                    for j in range(len(newsetl)):
                        if(newsetl[j]==try1['parameter'][i]):
                            if((abs(int(try1['epocht'][i])-time1))<=min1):
                                min1=abs(int(try1['epocht'][i])-time1)
                                valuesl[j]=try1['value'][i]
                                if(min1==0):
                                    break
                jdf2 = dict(zip(newsetl, valuesl))
                return jdf2
        except:
                jdf2={}
                return jdf2
            #if(param==air then return jdf2)
        
    #viirs
    '''
    req = Request("https://ngdc.noaa.gov/eog/viirs/download_flare_only_iframe.html")
    html_page = urlopen(req)
    soup = BeautifulSoup(html_page, "lxml")
    links = []
    for link in soup.findAll('a'):
        links.append(link.get('href'))
    sub = 'd201712'
    links_dec=[]
    links_dec=([s for s in links if sub in s])
    sub = 'only.csv.gz'
    links_dec_csv=[]
    links_dec_csv=([s for s in links_dec if sub in s])
    url =links_dec_csv[0]
    filename = "VNF_npp_d20171205_noaa_v21.flares_only.csv.gz"
    with open(filename, "wb") as f:
        r = requests.get(url)
        f.write(r.content)
    '''
    def lightfun(latitude,longitude,time):
        try:
            #print("lightfun\n")
            with gzip.open('VNF_npp_d20171205_noaa_v21.flares_only.csv.gz', 'rb') as f_in:
                with open('latest.csv', 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            chunksize = 10 ** 6
            viirs=pd.DataFrame()
            for chunk in pd.read_csv("latest.csv", chunksize=chunksize):
                viirs=viirs.append(chunk)
            dif=1000
            a=0
            b=0
            k=999999
            for i in range (len(viirs)):
                R = 6373.0
                lat1 = radians(float(viirs['Lat_GMTCO'][i]))
                lon1 = radians(float(viirs['Lon_GMTCO'][i]))
                lat2 = radians(float(latitude))
                lon2 = radians(float(longitude))
                dlon = lon2 - lon1
                dlat = lat2 - lat1

                a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
                c = 2 * atan2(sqrt(a), sqrt(1 - a))

                distance = R * c
                #print(str(i)+","+str(a+b))
                if(distance<=dif):
                    dif=distance
                    k=i
            if(k==999999):
                #print("lightfun_if\n")
                jdf3={}
                return jdf3
            else:
                #print("lightfun_else\n")
                viirsdf=viirs.loc[[k]]
                viirsdf1=viirsdf
                viirsdf1['latitude']=latitude
                viirsdf1['longitude']=longitude
                virlight=gis.content.import_data(viirsdf1)
                virlight.layer.layers = [dict(virlight.layer)]
                map1.add_layer(virlight)
                virdf=virlight.query()
                dfn=((virdf).df.head())
                js3=dfn.to_json(orient='index')
                jdf=json.loads(js3)
                jdf3={}
                jdf3["Rad_M07"]=jdf["0"]["Rad_M07"]
                jdf3["Rad_M08"]=(jdf["0"]["Rad_M08"])
                jdf3["Rad_M10"]=(jdf["0"]["Rad_M10"])
                jdf3["Rad_M12"]=(jdf["0"]["Rad_M12"])
                jdf3["Rad_M13"]=(jdf["0"]["Rad_M13"])
                jdf3["Rad_M14"]=(jdf["0"]["Rad_M14"])
                jdf3["Rad_M15"]=(jdf["0"]["Rad_M15"])
                jdf3["Rad_M16"]=(jdf["0"]["Rad_M16"])
                return jdf3
        except:
                jdf3={}
                return jdf3
            #if(param==light then return jdf3)
    def Merge(dict1, dict2):
        res = {**dict1, **dict2}
        return res
    final={}
    final["latitude"]=latitude
    final["longitude"]=longitude
    final["time"]=time
    if(param=="generic"):
        final["values"]=genericfun(latitude,longitude,time)
    if(param=="air"):
        
        final["values"]=airfun(latitude,longitude,time)
    if(param=="light"):
        
        final["values"]=lightfun(latitude,longitude,time)
    if(param=="all"):
        jdf1=genericfun(latitude,longitude,time)
        jdf2=airfun(latitude,longitude,time)
        jdf3=lightfun(latitude,longitude,time)
        dic={}
        dic=Merge(jdf1,jdf2)
        fdic={}
        fdic=Merge(dic,jdf3)
        final["values"]=fdic
    if(param=="airlight" or param=="lightair"):
        jdf2=airfun(latitude,longitude,time)
        jdf3=lightfun(latitude,longitude,time)
        dic={}
        dic=Merge(jdf2,jdf3)
        final["values"]=dic
    if(param=="airgeneric" or param=="genericair"):
        jdf1=genericfun(latitude,longitude,time)
        jdf2=airfun(latitude,longitude,time)
        dic={}
        dic=Merge(jdf1,jdf2)
        final["values"]=dic
    if(param=="genericlight" or param=="lightgeneric"):
        jdf1=genericfun(latitude,longitude,time)
        jdf3=lightfun(latitude,longitude,time)
        dic={}
        dic=Merge(jdf1,jdf3)
        final["values"]=dic
    fin=json.dumps(final)
    return fin
if __name__=='__main__':
	run_simple('0.0.0.0',80, app)
