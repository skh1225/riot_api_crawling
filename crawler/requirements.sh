#!/bin/sh
sudo yum update -y
sudo yum install git -y
sudo yum install pip -y
pip install -r requirements.txt