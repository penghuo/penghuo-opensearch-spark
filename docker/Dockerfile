# Use the specified base image
FROM public.ecr.aws/emr-serverless/spark/emr-7.2.0:latest

# Define the Jackson version to use
ENV JACKSON_VERSION=2.17.2

# Switch to root user to perform administrative tasks
USER root

# Install curl if it's not already installed
RUN if ! command -v curl >/dev/null 2>&1; then \
        yum update -y && \
        yum install -y curl && \
        yum clean all; \
    fi

# Remove existing Jackson JARs with older versions
# RUN rm -f \
#     /usr/lib/spark/jars/jackson-annotations-2.15.2.jar \
#     /usr/lib/spark/jars/jackson-core-2.15.2.jar \
#     /usr/lib/spark/jars/jackson-databind-2.15.2.jar \
#     /usr/lib/spark/jars/jackson-dataformat-yaml-2.15.2.jar \
#     /usr/lib/spark/jars/jackson-datatype-jsr310-2.15.2.jar \
#     /usr/lib/spark/jars/jackson-module-scala_2.12-2.15.2.jar

RUN rm -f \
    /usr/lib/spark/jars/jackson-core-2.15.2.jar \
    /usr/lib/spark/jars/jackson-databind-2.15.2.jar \
    /usr/lib/spark/jars/jackson-module-scala_2.12-2.15.2.jar

# Download and add new Jackson JARs version 2.17.2
RUN curl -fSL https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/${JACKSON_VERSION}/jackson-core-${JACKSON_VERSION}.jar \
    -o /usr/lib/spark/jars/jackson-core-${JACKSON_VERSION}.jar && \
    curl -fSL https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/${JACKSON_VERSION}/jackson-databind-${JACKSON_VERSION}.jar \
    -o /usr/lib/spark/jars/jackson-databind-${JACKSON_VERSION}.jar && \
    curl -fSL https://repo1.maven.org/maven2/com/fasterxml/jackson/module/jackson-module-scala_2.12/${JACKSON_VERSION}/jackson-module-scala_2.12-${JACKSON_VERSION}.jar \
    -o /usr/lib/spark/jars/jackson-module-scala_2.12-${JACKSON_VERSION}.jar

USER hadoop 
