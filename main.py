import pandas as pd
import scipy.stats as st
import multiprocessing as mp
import datetime as dt
import requests
import requests
import json
import locale
from tqdm import tqdm
from BBRLight import BBR
from DAWA import LejlighedData
#Angiv placering for csvfilen den skal indlæse
Csvpath="Solgteboliger.csv"
#Angiv placering for csvfilen den skal gemme
Savefilepath='SolgteboligerFeatures.csv'
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
    training = pd.concat(result)

    end_time = dt.datetime.now()
    elapsed_time = end_time - start_time
    print ("Done. Det tog : ", elapsed_time)
    training.to_csv(Savefilepath,encoding="utf-8-sig")
