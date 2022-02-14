FROM apache/airflow:2.1.4
USER root

RUN apt update
RUN echo deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main >> /etc/apt/sources.list
RUN apt install wget gnupg unzip curl vim -y
RUN wget https://dl.google.com/linux/linux_signing_key.pub
RUN apt-key add linux_signing_key.pub
RUN apt update
RUN apt install google-chrome-stable -y
RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
RUN unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/

USER airflow
RUN pip install --no-cache-dir --user selenium beautifulsoup4
