import pandas as pd
import multiprocessing as mp
import datetime as dt
import requests
import json
from tqdm import tqdm
from DAWA import LejlighedData, BBR
#Angiv placering for csvfilen den skal indlæse
Csvpath="Solgteboliger.csv"
#Angiv placering for csvfilen den skal gemme
Savefilepath='SolgteboligerFeatures.csv'
#Angiv chunksize for hver fil.
CHUNKSIZE = 10

if __name__ == '__main__':
    start_time = dt.datetime.now()
    print("Started at ", start_time)
    print('Indlæser data...')
    reader = pd.read_csv(Csvpath, chunksize=CHUNKSIZE,encoding="utf-8-sig")
    pool = mp.Pool(7) # use 7 processes

    funclist = []
    for df in reader:
        # process each data frame
        f = pool.apply_async(LejlighedData,[df])
        funclist.append(f)
    result = []
    for f in tqdm(funclist):
        result.append(f.get()) 

    #Sammensætter datasættene
    dfcon = pd.concat(result)

    end_time = dt.datetime.now()
    elapsed_time = end_time - start_time
    print ("Færdig.... Det tog : ", elapsed_time)
    dfcon.to_csv(Savefilepath,encoding="utf-8-sig")
