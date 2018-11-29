'''
Created on Jul 9, 2017

@author: Alexey
'''
import requests
class ElasticSearch: 
    def readURL2(self):
        import requests
        url = 'https://updates.opendns.com/nic/update?hostname='
        url = 'http://httpbin.org/status/418'
        username = 'username'
        password = 'password'
        data=open("query.txt","r").read()
        print(data)
        data=requests.get(url, data, auth=(username, password)).content
        print(data)
        data=requests.get(url, data, auth=(username, password)).text
        print(data)
        data = requests.request(method='get', url='http://httpbin.org/status/418', data=data).content
        print(data)
        return data
    
    def checkIndexMappingContent(self):
        import requests
        #CHECK INDICES
        get_indices="http://ec2-54-162-18-40.compute-1.amazonaws.com:9200/_cat/indices?v"
        data = requests.request(method='get', url=get_indices).text
        print(data)
        #return
        #CHECK MAPPING
        get_mappings2="http://ec2-54-162-18-40.compute-1.amazonaws.com:9200/alexey2_index/_mapping"
        data = requests.request(method='get', url=get_mappings2).text
        print(data)
        #CHECK CONTENT
        dat=open("dat2.txt","r").read()
        url="http://ec2-54-162-18-40.compute-1.amazonaws.com:9200/alexey2_index/business/_search?explain"
        data = requests.request(method='get', url=url,data=dat).text
        print(data)
        
    def createIndexMappingNative(self):
        #ADD INDEX AND MAPPING
        #DELETE OLD INDEX
        putMapping="http://ec2-54-162-18-40.compute-1.amazonaws.com:9200/alexey2_index"
        data = requests.request(method='delete', url=putMapping).text
        print(data)
        print("Index Deleted")
        #PUT INDEX MAPPING
        putMapping2=open("putIndexMapping.txt","r").read() 
        putMapping="http://ec2-54-162-18-40.compute-1.amazonaws.com:9200/alexey2_index"
        data = requests.request(method='put', url=putMapping,data=putMapping2).text
        print(data)
        
    def reindexNative(self):
        #REINDEX INTERNAL
        reindex2=open("reindex.txt","r").read() 
        print(reindex2)
        reindex="http://ec2-54-162-18-40.compute-1.amazonaws.com:9200/alexey2_index/business/_reindex?searchIndex=alexey_rudenko_2017_07_10_index&searchType=business"
        
        reindex="http://ec2-54-162-18-40.compute-1.amazonaws.com:9200/alexey2_index/business/_reindex"
        data = requests.post(url=reindex,data=reindex2).text
        print(data)
        
    def reindexES(self):
        # REINDEX ES
        import elasticsearch
        import elasticsearch.helpers
        elasticSource = elasticsearch.Elasticsearch([{"host": "ec2-54-162-18-40.compute-1.amazonaws.com", "port": 9200}])
        elasticDestination = elasticsearch.Elasticsearch([{"host": "ec2-54-162-18-40.compute-1.amazonaws.com", "port": 9200}])
        # Setup source and destinations connection to Elasticsearch. Could have been different clusters
        # Delete index so we know it doesn't exist.
        #elasticDestination.indices.delete(index="alexey2_index", ignore=[400, 404])
        # Create index with nothing in it.
        #elasticDestination.indices.create(index="alexey_rudenko_2017_07_10_index", ignore=[400, 404])
        elasticsearch.helpers.reindex(client=elasticSource, source_index="alexey_rudenko_2017_07_10_index", target_index="alexey2_index", target_client=elasticDestination)
        #data = requests.request(method='get', url=get_indices).text
        #CHECK INDICES
        get_indices="http://ec2-54-162-18-40.compute-1.amazonaws.com:9200/_cat/indices?v"
        data = requests.request(method='get', url=get_indices).text
        print(data)
        
    def reindexESBulk(self):
        import elasticsearch
        import elasticsearch.helpers
        from elasticsearch import Elasticsearch, helpers

        elasticSource = elasticsearch.Elasticsearch([{"host": "ec2-54-162-18-40.compute-1.amazonaws.com", "port": 9200}])

        new_index_data=[]
        count=0
        doc = helpers.scan(elasticSource, query={
        "query": {
        "match_all": {}
        
        },
        "size":1000 
        },index="alexey_rudenko_2017_07_10_index", scroll='5m', raise_on_error=False)
        
        for x in doc:
            x['_index'] ="alexey2_index"
            try:
                sum=0
                if 'checkin_info' in x['_source'] :
                    for n in x['_source']['checkin_info']:
                        sum = sum + x['_source']['checkin_info'][n]
                x['_source']['total_checkins']=sum
                print(sum)
            except KeyError: pass
            count = count + 1
            new_index_data.append(x) 
        
        helpers.bulk(elasticSource,new_index_data)

        
    def modifyTotalCheckins(self):
        #MODIFY CHECKINS INTERNAL
        update2=open("modify2.txt","r").read() 
        print(update2)
        update="http://ec2-54-162-18-40.compute-1.amazonaws.com:9200/alexey2_index/business/_update_by_queue"
        update3 = '{"script":{"inline": "ctx._source.total_info=0;for (int i = 0; i < ctx._source.checkin_info.size(); ++i) { ctx._source.total_info=ctx._source.total_info + ctx._source.checkin_info[i];}"}}'
        update4 = '{"script":{"inline":"ctx._source.total_checkins=0"}}'
        update5 = '{"script":{"inline":"ctx._source.review_count=0","lang":"painless"},"query":{"match":{"business_id":"bep9eG5OtD6-Q_H7O4Y9jQ"}}}'
        
        #          ,
        #"lang":"painless" }
        #}
        data = requests.post(url=update,data=update4).text
        print(data)
        
    def modifyES(self):
        import elasticsearch
        import elasticsearch.helpers
        #from elasticsearch import Elasticsearch, helpers

        elasticSource = elasticsearch.Elasticsearch([{"host": "ec2-54-162-18-40.compute-1.amazonaws.com", "port": 9200}])
        # Use the scan&scroll method to fetch all documents from your old index
        doc1 = {  "script": {  "inline": "ctx._source.review_count=0"}} 
        p = elasticSource.update_by_query("alexey2_index", "business", body={  "script": {  "inline": "ctx._source.business.review_count=4"}} , params={"match_all": {}})
        print("update_by_query="+str(p))
       
        
    def readURL(self):
        import requests
        url="http://ec2-54-162-18-40.compute-1.amazonaws.com:9200/alexey_rudenko_2017_07_10_index/_search?fields=name,business_id,full_address,total_checkins&offset=0&size=20"
        url="http://ec2-54-162-18-40.compute-1.amazonaws.com:9200/alexey_rudenko_2017_07_10_index/business/_search?offset=0&size=20"
        url="http://ec2-54-162-18-40.compute-1.amazonaws.com:9200/alexey_rudenko_2017_07_10_index/business/_search?" #q=name,full_address:Decatur"
        #
        get_indices="http://ec2-54-162-18-40.compute-1.amazonaws.com:9200/_cat/indices?v"
        get_mappings="http://ec2-54-162-18-40.compute-1.amazonaws.com:9200/alexey_rudenko_2017_07_10_index/business/_mapping"
        
        #dat=open("dat2.txt","r").read() 
        data = requests.request(method='get', url=get_mappings).text
        print(data)
        #return
        #ADD INDEX AND MAPPING
        #DELETE OLD INDEX
        putMapping="http://ec2-54-162-18-40.compute-1.amazonaws.com:9200/alexey2_index"
        data = requests.request(method='delete', url=putMapping).text
        print(data)
        #PUT INDEX MAPPING
        putMapping2=open("putIndexMapping.txt","r").read() 
        putMapping="http://ec2-54-162-18-40.compute-1.amazonaws.com:9200/alexey2_index"
        data = requests.request(method='put', url=putMapping,data=putMapping2).text
        print(data)
        #return
        # REINDEX ES
        import elasticsearch
        import elasticsearch.helpers
        elasticSource = elasticsearch.Elasticsearch([{"host": "ec2-54-162-18-40.compute-1.amazonaws.com", "port": 9200}])
        elasticDestination = elasticsearch.Elasticsearch([{"host": "ec2-54-162-18-40.compute-1.amazonaws.com", "port": 9200}])
        # Setup source and destinations connection to Elasticsearch. Could have been different clusters
        # Delete index so we know it doesn't exist.
        #elasticDestination.indices.delete(index="alexey2_index", ignore=[400, 404])
        # Create index with nothing in it.
        #elasticDestination.indices.create(index="alexey_rudenko_2017_07_10_index", ignore=[400, 404])
        elasticsearch.helpers.reindex(client=elasticSource, source_index="alexey_rudenko_2017_07_10_index", target_index="alexey2_index", target_client=elasticDestination)
        #data = requests.request(method='get', url=get_indices).text
        #CHECK INDICES
        get_indices="http://ec2-54-162-18-40.compute-1.amazonaws.com:9200/_cat/indices?v"
        data = requests.request(method='get', url=get_indices).text
        print(data)
        #return
        
        #CHECK MAPPING
        get_mappings2="http://ec2-54-162-18-40.compute-1.amazonaws.com:9200/alexey2_index/_mapping"
        data = requests.request(method='get', url=get_mappings2).text
        print(data)
        #return
        #REINDEX INTERNAL
        reindex2=open("reindex.txt","r").read() 
        reindex="http://ec2-54-162-18-40.compute-1.amazonaws.com:9200/alexey2_index/business/_reindex?searchIndex=alexey_rudenko_2017_07_10_index&searchType=business"
        data = requests.post(url=reindex,data=reindex2).text
        print(data)
        #CHECK MAPPING
        get_mappings2="http://ec2-54-162-18-40.compute-1.amazonaws.com:9200/alexey2_index/_mapping"
        data = requests.request(method='get', url=get_mappings2).text
        print(data)
        #CHECK CONTENT
        #dat=open("dat2.txt","r").read()
        url="http://ec2-54-162-18-40.compute-1.amazonaws.com:9200/alexey2_index/business/_search?"
        data = requests.request(method='get', url=url).text
        print(data)
        return data
    
    def group_by(self,es, fields, include_missing):
        current_level_terms = {'terms': {'field': fields[0]}}
        agg_spec = {fields[0]: current_level_terms}
    
        if include_missing:
            current_level_missing = {'missing': {'field': fields[0]}}
            agg_spec[fields[0] + '_missing'] = current_level_missing
    
        for field in fields[1:]:
            next_level_terms = {'terms': {'field': field}}
            current_level_terms['aggs'] = {
                field: next_level_terms,
            }
    
            if include_missing:
                next_level_missing = {'missing': {'field': field}}
                current_level_terms['aggs'][field + '_missing'] = next_level_missing
                current_level_missing['aggs'] = {
                    field: next_level_terms,
                    field + '_missing': next_level_missing,
                }
                current_level_missing = next_level_missing
    
            current_level_terms = next_level_terms

        agg_result = es.search(body={'aggs': agg_spec})['aggregations']
        return self.get_docs_from_agg_result(agg_result, fields, include_missing)


    def get_docs_from_agg_result(self,agg_result, fields, include_missing):
        current_field = fields[0]
        buckets = agg_result[current_field]['buckets']
        if include_missing:
            buckets.append(agg_result[(current_field + '_missing')])
    
        if len(fields) == 1:
            return [
                {
                    current_field: bucket.get('key'),
                    'doc_count': bucket['doc_count'],
                }
                for bucket in buckets if bucket['doc_count'] > 0
            ]
    
        result = []
        for bucket in buckets:
            records = self.get_docs_from_agg_result(bucket, fields[1:], include_missing)
            value = bucket.get('key')
            for record in records:
                record[current_field] = value
            result.extend(records)
    
        return result
    
    def readElasticSearch(self):
        data=""
        from elasticsearch import Elasticsearch
        try:
            es = Elasticsearch(
              #['iad1-10202-0.es.objectrocket.com', 'iad1-10202-1.es.objectrocket.com', 'iad1-10202-2.es.objectrocket.com', 'iad1-10202-3.es.objectrocket.com'],
              ['http://ec2-54-162-18-40.compute-1.amazonaws.com:9200/alexey_rudenko_2017_07_10_index/_search?offset=0&size=20'],
              http_auth=('YOUR_USERNAME', 'YOUR_PASSWORD'),
              port=80
            )
            #print(str(es))
            print ("Connected", es.info())
            data = self.group_by(es, ["business_id","name"], False)
        except Exception as ex:
            print ("Error:", ex)
        return data

if __name__ == '__main__':
    es = ElasticSearch()
    #data = es.createIndexMappingNative()
    #data = es.reindexES()
    #data = es.reindexESBulk()
    data=es.checkIndexMappingContent()
    ##data = es.reindexES()
    ##data = es.reindexNative()
    ##es.modifyES()
    #data = es.modifyTotalCheckins()
    pass