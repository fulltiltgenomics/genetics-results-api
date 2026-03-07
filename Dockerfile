FROM nikolaik/python-nodejs:python3.13-nodejs24-slim
LABEL maintainer="Juha Karjalainen <jkarjala@broadinstitute.org>"

RUN apt-get update && apt-get install -y \
    nginx \
    libz-dev \
    libbz2-dev \
    liblzma-dev \
    zlib1g-dev \
    libpcre2-dev \
    libssl-dev \
    libcurl4-openssl-dev \
	libdeflate-dev \
    bzip2 gcc g++ make

# dev or prod
ARG DEPLOY_ENV 
ARG HTSLIB_VER=1.22.1
ARG PORT=4000

ENV GOOGLE_APPLICATION_CREDENTIALS=""

RUN pip install uv --upgrade

COPY requirements.txt .
RUN uv pip install --system -r requirements.txt

# htslib
WORKDIR /opt/htslib
RUN curl -LO https://github.com/samtools/htslib/releases/download/${HTSLIB_VER}/htslib-${HTSLIB_VER}.tar.bz2 \
    && tar -xvjf htslib-${HTSLIB_VER}.tar.bz2 && cd htslib-${HTSLIB_VER} \
    && ./configure --enable-libcurl --enable-gcs --with-libdeflate \
	&& make && make install && cd .. && rm -rf htslib-${HTSLIB_VER}*

# gcloud sdk
WORKDIR /opt/gcloud
RUN curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz && \
    tar -xf google-cloud-cli-linux-x86_64.tar.gz && \
    ./google-cloud-sdk/install.sh -q --usage-reporting false

# init gcloud and run server at container start
COPY <<EOF /opt/genetics-results-api/start.sh
#!/bin/bash

source /opt/gcloud/google-cloud-sdk/completion.bash.inc && \
source /opt/gcloud/google-cloud-sdk/path.bash.inc
if [ -n "\$GOOGLE_APPLICATION_CREDENTIALS" ] && [ -f "\$GOOGLE_APPLICATION_CREDENTIALS" ]; then
    echo "Using GOOGLE_APPLICATION_CREDENTIALS: \$GOOGLE_APPLICATION_CREDENTIALS"
    gcloud auth activate-service-account --key-file="\$GOOGLE_APPLICATION_CREDENTIALS"
else
    echo "No GOOGLE_APPLICATION_CREDENTIALS, using Workload Identity / metadata server"
fi
export GCS_OAUTH_TOKEN=\$(gcloud auth print-access-token 2>/dev/null || true)

# if no command provided, run server
if [ -z "\$@" ]; then
    python3 run_server.py ${PORT}
    # exec uvicorn app.server:app --host 0.0.0.0 --port ${PORT}
else
    exec "\$@"
fi
EOF

RUN chmod +x /opt/genetics-results-api/start.sh

WORKDIR /opt/genetics-results-api

COPY . .

EXPOSE ${PORT}

ENTRYPOINT ["/opt/genetics-results-api/start.sh"]
