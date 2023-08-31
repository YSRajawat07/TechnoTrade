#!/bin/bash
PATH="/Library/Frameworks/Python.framework/Versions/3.8/bin:${PATH}"
export PATH
PYTHONPATH="${PYTHONPATH}:/Users/ajaysinghrajawat/PycharmProjects/techno-trade/venv/lib/python3.8/site-packages"
export PYTHONPATH
cd //Users/ajaysinghrajawat/PycharmProjects/techno-trade/StockSelection
python3 NseSectorAnalysis.py >> //Users/ajaysinghrajawat/PycharmProjects/techno-trade/StockSelection/logs/cron.log 2>&1
