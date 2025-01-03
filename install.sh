#!/bin/bash

CHROMEDRIVER_VERSION=$(curl -s https://chromedriver.storage.googleapis.com/LATEST_RELEASE)

set -e  # 에러 발생 시 즉시 종료

# 패키지 업데이트 및 필수 라이브러리 설치
apt update && apt install -y \
    wget curl unzip libglib2.0-0 libnss3 libgconf-2-4 libfontconfig1 \
    libxrender1 libxi6 libxtst6 libx11-xcb1 x11-utils git

# JAVA
#wget https://github.com/adoptium/temurin8-binaries/releases/download/jdk8u372-b07/OpenJDK8U-jdk_x64_linux_hotspot_8u372b07.tar.gz
#tar -xzf OpenJDK8U-jdk_x64_linux_hotspot_8u372b07.tar.gz -C /usr/local/
#mv /usr/local/jdk8u372-b07 /usr/local/java

# Google Chrome 설치
wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
apt install -y ./google-chrome-stable_current_amd64.deb
rm google-chrome-stable_current_amd64.deb

# Chromedriver 설치
wget -O /tmp/chromedriver.zip "https://chromedriver.storage.googleapis.com/${CHROMEDRIVER_VERSION}/chromedriver_linux64.zip"
unzip /tmp/chromedriver.zip -d /usr/local/bin/
chmod +x /usr/local/bin/chromedriver
rm /tmp/chromedriver.zip

# AWS CLI 설치
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
./aws/install
rm -rf awscliv2.zip aws

# 불필요한 파일 정리
apt-get clean
rm -rf /var/lib/apt/lists/*

