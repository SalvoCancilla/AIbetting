import sys
import os
import sqlite3
import pandas as pd
import numpy as np


from ..scripts.get_data import GetData






def main():
    get = GetData(season=2020, api_limit=5000)
    get.players_data()

if __name__ == "__main__":
    main()
