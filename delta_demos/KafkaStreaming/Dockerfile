## LINK https://community.snowflake.com/s/article/How-to-connect-to-Snowflake-using-AWS-MSK-and-Snowflake-Kafka-Connector

FROM confluentinc/cp-kafka-connect:latest

## install java and kafka
# this may be redundant because of the base image 
RUN sudo yum install java-1.8.0
RUN wget https://archive.apache.org/dist/kafka/2.6.2/kafka_2.12-2.6.2.tgz
RUN tar -xzf kafka_2.12-2.6.2.tgz

cd kafka_2.12-2.6.2
RUN cp /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.312.b07-1.amzn2.0.2.x86_64/jre/lib/security/cacerts kafka.client.truststore.jks


# Create working dir
RUN mkdir -p /kafka_connect

# Add Snowflake connector jar
RUN curl -sSL "https://repo1.maven.org/maven2/com/snowflake/snowflake-kafka-connector/1.8.0/snowflake-kafka-connector-1.8.0.jar" -o /kafka_connect/snowflake-kafka-connector-1.8.0.jar
# Add Snowflake JDBC connector jar
RUN curl -sSL "https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.10.3/snowflake-jdbc-3.10.3.jar" -o /kafka_connect/snowflake-jdbc-connector-3.10.3.jar
# Install the below jars for decrypting the RSA private key
RUN curl -sSL "https://repo1.maven.org/maven2/org/bouncycastle/bc-fips/1.0.1/bc-fips-1.0.1.jar" -o /kafka_connect/bc-fips-1.0.1.jar
RUN curl -sSL "https://repo1.maven.org/maven2/org/bouncycastle/bcpkix-fips/1.0.3/bcpkix-fips-1.0.3.jar" -o /kafka_connect/bcpkix-fips-1.0.3.jar
# copy the properties file
COPY sfdata.json /kafka_connect/configSF_connect.properties 




