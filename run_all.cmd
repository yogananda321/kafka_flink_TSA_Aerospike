@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\run-all.ps1" %*
