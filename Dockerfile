FROM nikolaik/python-nodejs:python3.11-nodejs20-slim
LABEL maintainer="Juha Karjalainen <jkarjala@broadinstitute.org>"

RUN apt-get update && apt-get install -y nginx libz-dev libbz2-dev liblzma-dev zlib1g-dev libpcre2-dev libpcre3-dev libssl-dev libcurl4-openssl-dev bzip2 gcc g++ make

# dev or prod
ARG DEPLOY_ENV 
# finngen or public
ARG DATA_SOURCE
ARG HTSLIB_VER=1.22

RUN pip install uv --upgrade

# htslib
WORKDIR /opt/htslib
RUN curl -LO https://github.com/samtools/htslib/releases/download/${HTSLIB_VER}/htslib-${HTSLIB_VER}.tar.bz2 && \
    tar -xvjf htslib-${HTSLIB_VER}.tar.bz2 && cd htslib-${HTSLIB_VER} && \
    ./configure && make && make install && cd .. && rm -rf htslib-${HTSLIB_VER}*

# gcloud sdk
WORKDIR /opt/gcloud
RUN curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz && \
    tar -xf google-cloud-cli-linux-x86_64.tar.gz && \
    ./google-cloud-sdk/install.sh -q --usage-reporting false

# init gcloud and run server at container start
COPY <<EOF /var/www/genetics-results-api/start.sh
#!/bin/bash
if [ -f /mnt/disks/data/phewas-development-ff565b237edf.json ]; then
    source /opt/gcloud/google-cloud-sdk/completion.bash.inc && \
    source /opt/gcloud/google-cloud-sdk/path.bash.inc && \
    gcloud auth activate-service-account --key-file=/mnt/disks/data/phewas-development-ff565b237edf.json
else
    echo "NOTE: No service account key file found, gcloud will not be initialized"
fi
source .venv/bin/activate
# if no command provided, run server
if [ -z "\$@" ]; then
    exec uvicorn app.server:app --host 0.0.0.0 --port 4000
else
    exec "\$@"
fi
EOF

RUN chmod +x /var/www/genetics-results-api/start.sh

WORKDIR /var/www/genetics-results-api

COPY . .
RUN uv venv && . .venv/bin/activate && uv pip install -r requirements.txt
COPY app/config/config.${DEPLOY_ENV}.${DATA_SOURCE}.py app/config/config.py

ENV ENVIRONMENT=${DEPLOY_ENV}

EXPOSE 4000

ENTRYPOINT ["/var/www/genetics-results-api/start.sh"]
