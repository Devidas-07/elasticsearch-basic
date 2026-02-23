# Use the official Elasticsearch image as the base
FROM docker.elastic.co/elasticsearch/elasticsearch:8.17.0

# (Optional) Install plugins, e.g., the analysis-icu plugin
RUN bin/elasticsearch-plugin install analysis-icu

# (Optional) Copy a custom configuration file
# COPY elasticsearch.yml /usr/share/elasticsearch/config/elasticsearch.yml

# Set environment variables for local single-node development
ENV discovery.type=single-node
ENV xpack.security.enabled=false

# Expose the standard Elasticsearch port
EXPOSE 9200