'''
Created on Jul 9, 2017

@author: Alexey
'''
import requests
import time
class ElasticSearch:
    
    def checkIndexMappingContent(self,url,port,index,dat):
        print("dat="+str(dat))
        #CHECK INDICES
        get_indices="%s:%s/_cat/indices?v" % (url,port)
        print("get_indices_url="+get_indices)
        data = requests.request(method='get', url=get_indices).text
        print(data)
        #CHECK MAPPING
        mapping="%s:%s/%s/_mapping" % (url,port,index)
        data = requests.request(method='get', url=mapping).text
        print("mapping_url="+mapping)
        print("mapping="+str(data))
        #CHECK CONTENT
        url="%s:%s/%s/business/_search" % (url,port,index)
        data = requests.request(method='get', url=url,data=dat).text
        print("search_url="+url)
        print("search="+data)
    
    def deleteOldIndex(self,url,port,aux_index):
        #DELETE OLD AUX INDEX
        putMapping="%s:%s/%s" % (url,str(port),aux_index)
        data = requests.request(method='delete', url=putMapping).text
        print(data)
        print("Old index deleted")
    
    def reindexESBulk(self,url,port,oldindex,newindex):
        import elasticsearch
        from elasticsearch import helpers

        elasticSource = elasticsearch.Elasticsearch([{"host": url, "port": port}])
        new_index_data=[]
        doc = helpers.scan(elasticSource, query={
            "query": {
            "match_all": {}
        },
        "size":1000 
        },index=oldindex, scroll='5m', raise_on_error=False)
        
        for x in doc:
            x['_index'] =newindex
            try:
                summ=0
                if 'checkin_info' in x['_source'] :
                    for n in x['_source']['checkin_info']:
                        summ = summ + x['_source']['checkin_info'][n]
                x['_source']['total_checkins']=summ
            except KeyError: pass
            
            new_index_data.append(x) 
        
        data = helpers.bulk(elasticSource,new_index_data)
        print("Reindexed: "+str(data))
        
    def getOldIndexContent(self):
        content='{ \
            "from" : 0, "size" : 10, \
            "_source": [ "name", "business_id","full_address" ], \
            "query":{ \
                "multi_match" : { \
                "query":      "Benztek Performance", \
                "type":       "best_fields", \
                "fields":     [ "name", "full_address" ], \
                "tie_breaker": 0.5 \
                } \
            } \
        } '
        
        return content
    
    def getNewIndexContent(self):
        content='{ \
            "from" : 0, "size" : 10, \
            "_source": [ "name", "business_id","full_address","total_checkins" ], \
            "query":{ \
                "multi_match" : { \
                "query":      "Benztek Performance", \
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
        return content
    
    def totalCheckinsIndex(self,host,port,oldindex,aux_index):
        self.deleteOldIndex("http://"+host,port,aux_index)
        self.reindexESBulk(host,port,oldindex,aux_index)

if __name__ == '__main__':
    print("Reindexing...")
    start = time.time()
    host="ec2-54-162-18-40.compute-1.amazonaws.com"
    port=9200
    index="alexey_rudenko_2017_07_10_index"
    aux_index="alexey2_index"
    es = ElasticSearch()
    es.totalCheckinsIndex(host,port,index,aux_index)
    data=es.checkIndexMappingContent("http://"+host,port,index,es.getOldIndexContent())
    data=es.checkIndexMappingContent("http://"+host,port,aux_index,es.getNewIndexContent())
    end = time.time()
    print("Reindexing is finished="+str(int(end-start))+"sec")
    pass