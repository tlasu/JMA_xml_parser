#%%
import json
import requests as rq
import datetime as dt
import os
import io
import pandas as pd
import xml.etree.ElementTree as ET
from subprocess import call 

import datetime as dt
from pytz import timezone
from dateutil import parser
#%%

class bousai_Xml:
    def __init__(self,ent,old_data=pd.DataFrame()):
        self.id=""
        self.EventID=None
        self.Title=""
        self.new=None
        
        self.parse_xml(ent)
        try:
            self.get_data()
        except:
            pass

        self.check_new_data(old_data)
        
    def parse_xml(self,ent):
        #http://www.data.jma.go.jp/developer/xml/feed/other_l.xmlから取得する情報
        self.kind    = ent.find('{http://www.w3.org/2005/Atom}title').text
        self.id      = ent.find('{http://www.w3.org/2005/Atom}id').text
        self.updated = ent.find('{http://www.w3.org/2005/Atom}updated').text
        self.content = ent.find('{http://www.w3.org/2005/Atom}content').text
        
    def get_data(self):
        from dateutil import tz
        UTC = tz.gettz("UTC")
        #self.idから取得する情報
        data=rq.get(self.id).content
        root=ET.fromstring(data)

        cntl=root.find("{http://xml.kishou.go.jp/jmaxml1/}Control")
        head=root.find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Head")
        body=root.find("{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body")

        self.Title = head.find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Title").text
        self.EventID = head.find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}EventID").text
        self.Serial = head.find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Serial").text
        self.ReportDateTime = parser.parse(\
            head.find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}ReportDateTime").text
            ).astimezone(timezone('Asia/Tokyo'))
        self.PdfLink = f"https://www.jma.go.jp/bosai/flood/data/pdf/{self.EventID}_{self.ReportDateTime.astimezone(UTC).strftime('%Y%m%d%H%M%S')}_n00.pdf"
    def check_new_data(self,old_data):
        '''新規データかを確認する'''
        if(len(old_data)!=0):
            _base=old_data
            if (_base.id[_base.id.str.contains(self.id)].count()!=1):
                self.new=True
            else:
                self.new=False
        else:
            self.new=True
                
    def send(self,WEBHOOK_URL,data={"text": "Hello World_request moduleによるテスト"}):
        '''指定URLに投稿する'''
        
        data={"title":self.Title,
              "text" :self.updated+"<br>"+
                      self.content+"<br>"+
                      self.EventID}
        
        r=rq.post(WEBHOOK_URL,json=data,
                  headers={'Content-Type': 'application/json'})
        print(r.status_code)

def get_ids_old_data():
    """#前回のデータからid一覧のみを取得"""
    try:
        if is_aws_lambda:
            src_obj = s3.Object(bucket,'latest_extra.xml')
            body_in = src_obj.get()['Body'].read().decode('utf8')
            f = io.StringIO(body_in)
        else:
            f = target_file
        tree = ET.parse(f)
        root = tree.getroot()
        entrys=root.findall("{http://www.w3.org/2005/Atom}entry")
        target_ent=[]
        for ent in entrys:
            if (ent.find('{http://www.w3.org/2005/Atom}title').text=="指定河川洪水予報"):
                target_ent.append(ent.find('{http://www.w3.org/2005/Atom}id').text)
        return pd.DataFrame(target_ent,columns=["id"])
    except Exception as e:
        print("get_ids_old_data_error", e)
        return pd.DataFrame()
# %%
is_aws_lambda = False
f = "./sample/extra.xml"
tree = ET.parse(f)
root = tree.getroot()
entrys=root.findall("{http://www.w3.org/2005/Atom}entry")
target_ent=[]
for ent in entrys:
    if (ent.find('{http://www.w3.org/2005/Atom}title').text=="指定河川洪水予報"):
        target_ent.append(ent)

# %%
time_c=dt.datetime.now().strftime("%Y%m%d%H%M")
target_file="extra"
of1="latest_flood_{}.json".format(target_file)
of2="latest_flood_{}.csv".format(target_file)
of3=time_c+"flood_{}.json".format(target_file)
of4=time_c+"flood_{}.csv".format(target_file)
#%%
target_file = "./sample/extra.xml"
# %%
old_data=get_ids_old_data()
target_ent[0].find('{http://www.w3.org/2005/Atom}title').text
target_ent[0].find('{http://www.w3.org/2005/Atom}id').text
# data=rq.get(self.id).content
#%%
bousai_Xml(target_ent[0]).id
# %%
ent_class=[bousai_Xml(a) for a in target_ent]
#out_data=pd.DataFrame([vars(ent_c) for ent_c in ent_class])

# %%
s = out_data.iloc[1]
for i in out_data.sort_values("EventID").PdfLink:
    print(i)
#%%
out_data.sort_values("EventID")
#%%
from dateutil import tz
UTC = tz.gettz("UTC")
s.ReportDateTime.astimezone(UTC).strftime('%Y%m%d%H%M%S')
#dt.datetime.strptime("2023-07-10T06:35:00+09:00")
#%%
for i in out_data.id:
    print(i)
#%%
out_data.iloc[1].id
# %%
#'https://www.data.jma.go.jp/developer/xml/data/20230709235731_0_VXKO53_400000.xml'
data=rq.get("https://www.data.jma.go.jp/developer/xml/data/20230709213723_0_VXKO54_440000.xml").content

# %%
data
# %%
root=ET.fromstring(data)
head=root.find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Head")
hl = head.find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Headline")
ifs = hl.find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Information")
# %%
head
# %%
a = ifs.find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Item")
a.find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Kind")\
.find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Code").text
a.find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Kind")\
.find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Name").text
# %%
def get_VXKO_data(\
        url="https://www.data.jma.go.jp/developer/xml/data/20230709213723_0_VXKO54_440000.xml"\
    ):
    data=rq.get(url).content

    root=ET.fromstring(data)
    cntl = root.find("{http://xml.kishou.go.jp/jmaxml1/}Control")
    head = root.find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Head")
    body = root.find("{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body")

    hl = head.find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Headline")
    ifs = hl.find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Information")

    a = ifs.find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Item").find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Kind")
    Code = a.find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Code").text
    Name = a.find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Name").text
    return Code, Name, cntl, head, body

# %%
a,b,c,d,e = get_VXKO_data('https://www.data.jma.go.jp/developer/xml/data/20230709235031_0_VXKO70_440000.xml')
# %%
hl = d.find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Headline")
ifs = hl.findall("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Information")
# %%
for child in hl:
    print(child.tag, child.attrib, child.text)
# %%
for info in ifs:
    ss = info.find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Item").find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Kind")
    ss = info.find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Item")\
        .find("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Areas")\
        .findall("{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Area")
    
    for s in ss:
        for child in s:
            print(child.tag, child.attrib, child.text)
# %%
## Warning の詳細
for child in e:
    print(child.tag, child.attrib, child.text)
# %%
 202307102135

https://www.jma.go.jp/bosai/flood/data/pdf/890920000101_20230710063500_n00.pdf
https://www.jma.go.jp/bosai/flood/data/pdf/860604101300_20230713002000_n00.pdf
https://www.jma.go.jp/bosai/flood/data/pdf/890906000102_20230709235000_n00.pdf


hi.findReportDateTime

# %%
