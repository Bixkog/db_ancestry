#!/bin/bash

psql -U wojtek -c "drop database student"
psql -U wojtek -c "create database student"

python3 db_entry.py test_init.json --init 
python3 db_entry.py test_normal.json