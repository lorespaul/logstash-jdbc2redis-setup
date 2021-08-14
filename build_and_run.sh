docker build -t logstash_with_redis .
docker run -d -it --name my-logstash --network=logstash-subnet -p 5044:5044 -v /home/lorenzo/hostmount/logstash/pipeline:/usr/share/logstash/pipeline -v /home/lorenzo/hostmount/logstash/persistence:/usr/share/logstash/persistence logstash_with_redis

# elasticsearch
docker run -d --name elasticsearch --network=logstash-subnet -p 9200:9200 -p 9300:9300 -v /home/lorenzo/hostmount/elasticsearch:/usr/share/elasticsearch/data -e "discovery.type=single-node" -e "http.cors.enabled=true" -e "http.cors.allow-origin=*" -e "http.cors.allow-headers=X-Requested-With,X-Auth-Token,Content-Type,Content-Length,Authorization" -e "http.cors.allow-credentials=true" docker.elastic.co/elasticsearch/elasticsearch:7.14.0

# dejavu
docker run -p 1358:1358 -d --name dejavu --network=logstash-subnet appbaseio/dejavu