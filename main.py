#!/usr/bin/env python3
"""
main.py
  python main.py           → полный парсинг всех категорий
  python main.py --update  → только обновление цен и наличия
"""
import sys

if "--update" in sys.argv:
    from updater import main
else:
    from scrapers.scraper import main

main()
