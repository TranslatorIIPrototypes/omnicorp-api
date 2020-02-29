#!/usr/bin/env bash

export $(egrep -v '^#' .env | xargs)

uvicorn server:app --host 0.0.0.0 --port 3210 --reload
