FROM docker.elastic.co/logstash/logstash:7.14.0

LABEL authors="Lorenzo Daneo <lorenzo.daneo91@gmail.com>"

COPY ./postgresql-42.2.23.jar /usr/share/logstash/

USER root

RUN yum update -y && \
	yum install epel-release yum-utils nano -y && \
	yum update -y && \
	yum install redis -y

USER logstash

RUN /usr/share/logstash/bin/logstash-plugin install logstash-output-exec
RUN /usr/share/logstash/bin/logstash-plugin install logstash-filter-json_encode

RUN mkdir -p /usr/share/logstash/persistence/

VOLUME ["/usr/share/logstash/pipeline/", "/usr/share/logstash/persistence/"]
EXPOSE 5044 9600

ENTRYPOINT ["/usr/local/bin/docker-entrypoint"]