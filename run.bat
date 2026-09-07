@echo off
chcp 65001 >nul
title Big Data ELT Pipeline Launcher
color 0B

echo =======================================================
echo          Loading Big Data ELT Control Panel...
echo =======================================================
python run.py
pause
