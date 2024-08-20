@echo off
:: installo le librerie che servono
python -m pip install -r requirements.txt
:: lancio lo script
python cedolini_mensili.py
pause
