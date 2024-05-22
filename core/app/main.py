import sys
import os
import sqlite3
import pandas as pd
import numpy as np


from ..scripts.get_data import GetData






def main():
    season = "2021"
    
    get = GetData(season, api_limit=2000)
    get.venues_data()

if __name__ == "__main__":
    main()
