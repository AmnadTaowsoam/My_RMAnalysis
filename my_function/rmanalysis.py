import os
import datetime
import pandas as pd
import numpy as np
np.set_printoptions(suppress=True)

import warnings
warnings.filterwarnings("ignore")

from my_db.dbb import RMDBB
from my_db.db import RMDB

db = RMDB()
dbb = RMDBB()

class Processing():
    def __init__(self) -> None:
        pass
    
    def get_data_bufferDB(self):
        data = dbb.get_rmbuffer_tbl()
        data = data.drop(columns={'ID'})
        data = data.drop_duplicates()
        return data
    
    def get_data_rmDB(self):
        data = db.get_rmanalysis_tbl()
        data = data.drop(columns={'ID'})
        data = data.drop_duplicates()
        return data
    
    def check_exit_DB(self):
        data_a = self.get_data_rmDB()
        data_b = self.get_data_bufferDB()
        data = pd.concat([data_a,data_b]).drop_duplicates(keep=False)
        return data
            
    def update_rmanalysis(self):
        try:
            rma = self.check_exit_DB()
            for i in range(len(rma)):
                rmas = rma.values.tolist()
                rmas = rmas[i][0],rmas[i][1],rmas[i][2],rmas[i][3],rmas[i][4],rmas[i][5],rmas[i][6],rmas[i][7],rmas[i][8],rmas[i][9],rmas[i][10],\
                    rmas[i][11],rmas[i][12],rmas[i][13],rmas[i][14],rmas[i][15],rmas[i][16],rmas[i][17],rmas[i][18],rmas[i][19],rmas[i][20],\
                    rmas[i][21],rmas[i][22],rmas[i][23],rmas[i][24],rmas[i][25],rmas[i][26],rmas[i][27],rmas[i][28],rmas[i][29],rmas[i][30],\
                    rmas[i][31],rmas[i][32],rmas[i][33],rmas[i][34],rmas[i][35],rmas[i][36],rmas[i][37],rmas[i][38],rmas[i][39],rmas[i][40],\
                    rmas[i][41],rmas[i][42],rmas[i][43],rmas[i][44],rmas[i][45],rmas[i][46],rmas[i][47],rmas[i][48],rmas[i][49],rmas[i][50],\
                    rmas[i][51],rmas[i][52]
                db.insert_rmanalysis_tbl(rmas)
        except:
            print('This analysis is already available in the database.','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')