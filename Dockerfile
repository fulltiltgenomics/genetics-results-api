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

COPY pyproject.toml .
RUN uv pip install --system -r pyproject.toml

# htslib
WORKDIR /opt/htslib
RUN curl -LO https://github.com/samtools/htslib/releases/download/${HTSLIB_VER}/htslib-${HTSLIB_VER}.tar.bz2 \
    && tar -xvjf htslib-${HTSLIB_VER}.tar.bz2 && cd htslib-${HTSLIB_VER} \
    && ./configure --enable-libcurl --enable-gcs --with-libdeflate \
	&& make && make install && cd .. && rm -rf htslib-${HTSLIB_VER}*

# mint the startup GCS token and run the server at container start
COPY <<EOF /opt/genetics-results-api/start.sh
#!/bin/bash

# raise the open-file limit: the range path opens many concurrent GCS sockets
# (bounded by GCS_MAX_CONNECTIONS in code, but this is defense-in-depth against a
# default soft limit of 1024 that "Too many open files" can otherwise hit).
ulimit -n 65536 2>/dev/null || true

# htslib reads the GCS bearer token out of the environment, so it needs a value before the
# first request. GCloudTabixBase.ensure_gcs_token() overwrites it from google-auth default
# credentials on first use and owns it from then on, so this only bridges the startup window
# — and an empty value must not stop the server.
# no bare trailing backslash below: the Dockerfile parser strips one inside COPY <<EOF and
# silently splits the command into fragments (a line ending in && is fine, the shell
# continues it anyway).
GCS_OAUTH_TOKEN=\$(python3 -c 'import google.auth, google.auth.transport.requests as r; c, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"]); c.refresh(r.Request()); print(c.token)' 2>/dev/null || true)
export GCS_OAUTH_TOKEN
if [ -n "\$GCS_OAUTH_TOKEN" ]; then
    echo "GCS_OAUTH_TOKEN set for startup"
else
    echo "No startup GCS token; the app will mint one on first use"
fi

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
