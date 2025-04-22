FROM nikolaik/python-nodejs:python3.11-nodejs20-slim
LABEL maintainer="Juha Karjalainen <jkarjala@broadinstitute.org>"

RUN apt-get update && apt-get install -y nginx libz-dev libbz2-dev liblzma-dev zlib1g-dev libpcre2-dev libpcre3-dev libssl-dev libcurl4-openssl-dev bzip2 gcc g++ make

# dev or prod
ARG DEPLOY_ENV 
# finngen or public
ARG DATA_SOURCE
ARG HTSLIB_VER=1.21

WORKDIR /var/www/genetics-results-api

RUN curl -LO https://github.com/samtools/htslib/releases/download/${HTSLIB_VER}/htslib-${HTSLIB_VER}.tar.bz2 && \
    tar -xvjf htslib-${HTSLIB_VER}.tar.bz2 && cd htslib-${HTSLIB_VER} && \
    ./configure && make && make install && cd .. && rm -rf htslib-${HTSLIB_VER}*

COPY requirements.txt ./
RUN pip3 install -r requirements.txt

COPY . .
COPY app/config/config.${DEPLOY_ENV}.${DATA_SOURCE}.py app/config/config.py

EXPOSE 4000
CMD ["uvicorn", "app.server:app", "--host", "0.0.0.0", "--port", "4000"] 
