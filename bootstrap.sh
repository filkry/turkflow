#!/bin/bash

apt-get update
apt-get install -y python-pip python-dev build-essential
pip install boto jinja2

ln -s -f /home/host/.boto /home/vagrant/.boto
