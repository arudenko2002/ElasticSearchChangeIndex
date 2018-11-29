'''
Created on Jul 15, 2017

@author: Alexey
'''
import requests
import json
class ReceiveTotalCheckins:
    def requestURL(self,q,page_value,size_value):
        request=""
        if len(q)>0:
            request=self.getIndexContentQueue() % (page_value,size_value,q)
            print(request)
        else:
            request=self.getIndexContent() % (page_value*size_value,size_value)
            print(request)
        host="ec2-54-162-18-40.compute-1.amazonaws.com"
        port=9200
        index="alexey2_index"
        url="%s:%s/%s/business/_search" % ("http://"+host,port,index)
        return requests.request(method='get', url=url,data=request).text
        
    def getIndexContentQueue(self):
        request='{ \
            "from" : %s, "size" : %s, \
            "_source": [ "name", "business_id","full_address","total_checkins" ], \
            "query":{ \
                "multi_match" : { \
                "query":      "%s", \
                "type":       "best_fields", \
                "fields":     [ "name", "full_address" ], \
                "tie_breaker": 0.5 \
                } \
            }, \
            "aggs": { \
                "total": { \
                    "sum": { \
                        "field": "total_checkins" \
                    } \
                } \
            } \
            ,"sort": [ \
                 { "_score": { "order": "desc" }}, \
                { "total_checkins": { "order": "desc"} } \
            ] \
        }'
        return request
    
    def getIndexContent(self):
        request='{ \
            "from" : %s, "size" : %s, \
            "_source": [ "name", "business_id","full_address","total_checkins" ], \
            "query":{ \
               "match_all": {} \
            }, \
            "aggs": { \
                "total": { \
                    "sum": { \
                        "field": "total_checkins" \
                    } \
                } \
            } \
            ,"sort": [ \
                 { "_score": { "order": "desc" }}, \
                { "total_checkins": { "order": "desc"} } \
            ] \
        }'
        return request
    
    def getOutput(self,datastr):
        result={}
        data = json.loads(datastr)
        print("DATA="+str(data))
        result["total"]=data['aggregations']['total']['value']
        result['business']=[]
        for ii in data['hits']['hits']:
            ii['_source']['_score']=ii['_score']
            result['business'].append(ii['_source'])
        return str(result)
    
    def receiveTotalCheckins(self,q="",page="0",size="10"):
        try:
            page_value = int(page)
            if page_value < 0:
                return {"error","Invalid request"}
        except ValueError:
            return {"error","Invalid request"}
        
        try:
            size_value = int(size)
            if size_value < 0:
                return {"error","Invalid request"}
        except ValueError:
            return {"error","Invalid request"}

        datastr = self.requestURL(q,page_value,size_value)
        data = self.getOutput(datastr)
        return data

if __name__ == '__main__':
    rtc = ReceiveTotalCheckins()
    #data = rtc.receiveTotalCheckins("kuku",page="0",size="10")
    data = rtc.receiveTotalCheckins("kuku",page="-1")
    print(data)
    data = rtc.receiveTotalCheckins("kuku",page="aaa")
    print(data)
    data = rtc.receiveTotalCheckins("kuku",size="-1")
    print(data)
    data = rtc.receiveTotalCheckins(size="aaa")
    print(data)
    data = rtc.receiveTotalCheckins("Benztek Performance")
    print(data)
    data = rtc.receiveTotalCheckins("")
    print(data)
    data = rtc.receiveTotalCheckins(page="1")
    print(data)
    data = rtc.receiveTotalCheckins(q="Auto",size="20")
    print(data)
    pass