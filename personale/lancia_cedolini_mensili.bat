:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
:: BAT file per lanciare python script                                       ::
::                                                                           ::
:: Author: Roberto Marzocchi <roberto.marzocchi@amiu.genova.it>              ::          
:: Date: 2024/08/20                                                          ::          
:: Version: 1.0.0                                                            ::          
::                                                                           ::
:: Changelog                                                                 ::
:: 1.0.0                                                                     ::
:: - Created the script                                                      ::
:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
:: NOTES:                                                                    ::
::                                                                           ::
:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::


@echo off
:: installo le librerie che servono
python -m pip install -r requirements.txt
:: lancio lo script
python cedolini_mensili.py
pause
