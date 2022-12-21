import os
import datetime
import pandas as pd
import numpy as np
np.set_printoptions(suppress=True)

import warnings
warnings.filterwarnings("ignore")

from my_db.db import RMDB

db = RMDB()

class Processing():
    def __init__(self) -> None:
        pass
    
    def columns_rename(self,input_data):
        try:
            data = input_data.rename(columns={
                            'Operation short text':'opt',
                            'Usage Decision Code':'ud',
                            'Sample No.':'sample',
                            'inspection Lot':'inslots',
                            'Code-SP':'vendor',
                            'SP-Name':'vendor_name',
                            'Old Code': 'oldcode',
                            'Code-RM':'material',
                            'RM-Name':'desc',
                            'DATE':'date',
                            'Material Doc.':'matdocs',
                            'F.F.A.':'FFA',
                            'U.A.':'UA',
                            'ETH.':'ETH',
                            'PV\n':'PV',
                            'AV\n':'AV',
                            'p-anisidine':'p_anisidine',
                            'Ac. Insol':'AcInsol',
                            'Gluten\n':'Gluten',
                            'Plant':'plant',
                            'Plant Origin Name':'plant_org',
                            'ผู้ผลิต':'mfg',
                            'ประเทศ':'country_org',
                            'Batch Origin':'batch_org',
                            'REMARK':'remark'
                            })
            data = data.drop(columns={
                'opt',
                'vendor_name',
                'desc',
                'oldcode',
                'matdocs',
                'plant_org'
                })
            data = data.fillna(0)
            return data
        except:
            print('Columns rename error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
    def columns_cleansing(self, input_data):
        try:
            data = input_data.replace("'", "")
            data = input_data.replace('"', "")
            return data
        except:
            print('Columns cleansing error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
    def columns_transform(self,input_data):
        try:
            data = input_data.copy()
            #fill na = 0
            data[['ud', 'sample', 'inslots', 'vendor', 'material', 'date', 'Batch']] = data[['ud', 'sample', 'inslots', 'vendor', 'material', 'date', 'Batch']]
            data = data.reset_index().rename(columns={'index':'runnings'})
            data['runnings'] = data['runnings'] +1
            data['sample2'] = np.where((data['sample'] == 0),data['runnings'], data['sample'])
            data = data.drop(columns={'sample','runnings'})
            data = data.rename(columns={'sample2':'sample'})

            
            data['inslots'] = np.where((data['ud'] == 0) ,'Recheck', data['inslots'])
            data['ud'] = np.where(((data['material'].str[:2] == 'WG') | (data['vendor'] == 0)| (data['Batch'] == 0)),'A', data['ud'])
            data['ud'] = np.where(((data['sample'] == 0) | (data['inslots'] == 0)),'A', data['ud'])
            data['inslots'] = np.where((data['vendor'] == 0),'Inprocess', data['inslots'])
            return data
        except:
            print('Columns transform data type error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
    def columns_detype(self, input_data):
        try:
            data = input_data.copy()
            data[['ud','sample','inslots','vendor','material','Batch']] = data[['ud','sample','inslots','vendor','material','Batch']].astype(pd.StringDtype())
            data[['plant','mfg','country_org','batch_org','remark']] = data[['plant','mfg','country_org','batch_org','remark']].astype(pd.StringDtype())
            data['inslots'] = data['inslots'].str[:11]
            data['sample'] = data['sample'].str[:11]
            data[['n_nut1','n_nut2','n_nut3','n_nut4','n_nut5','n_nut6','n_nut7','n_nut8','n_nut9','n_nut10']] = 0.0
            data = data[[
                        'ud', 
                        'sample', 
                        'inslots', 
                        'vendor', 
                        'material', 
                        'date', 
                        'Batch',
                        'MOIS', 
                        'ASH', 
                        'PROTEIN', 
                        'FAT', 
                        'FIBER', 
                        'P', 
                        'Ca', 
                        'INSOL', 
                        'NaCl',
                        'FFA', 
                        'UA', 
                        'KOHPS', 
                        'BRIX', 
                        'PEPSIN', 
                        'PEPSIN0002', 
                        'NDF', 
                        'ADF',
                        'ADL', 
                        'ETH', 
                        'T_FAT', 
                        'TVN', 
                        'NH3', 
                        'Starch', 
                        'IV', 
                        'PV', 
                        'AV',
                        'Totox', 
                        'p_anisidine', 
                        'Xanthophyll', 
                        'AcInsol', 
                        'Gluten',
                        'n_nut1',
                        'n_nut2',
                        'n_nut3',
                        'n_nut4',
                        'n_nut5',
                        'n_nut6',
                        'n_nut7',
                        'n_nut8',
                        'n_nut9',
                        'n_nut10', 
                        'plant',
                        'mfg', 
                        'country_org', 
                        'batch_org', 
                        'remark'    
            ]]
            data['date']
            return data
        except:
            print('Columns change data type error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
    def rma_transform(self, input_data):
        try:
            columns_rename = self.columns_rename(input_data)
            columns_cleansing = self.columns_cleansing(columns_rename)
            columns_transform = self.columns_transform(columns_cleansing)
            data = self.columns_detype(columns_transform)
            return data
        except:
            print('rm_processing error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
    def update_rmanalysis(self,data_input):
        try:
            rma = data_input.copy()
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
            print('Updatermanalysis error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
    def rma_processing(self):
        try:
            # Set the directory you want to start from
            rootDir = './documents/rmanalysis/'
            for dirName, subdirList, fileList in os.walk(rootDir):
                print('Found directory: %s' % dirName)
                for fname in fileList:
                    # Skip files that are not Excel
                    if not fname.endswith('.xlsx'):
                        continue
                    print('\t%s' % fname)
                    # Read the Excel file into a dataframe
                    rmanalysis_df = pd.read_excel(os.path.join(dirName, fname))
                    # Do something with the dataframe here
                    
                    rmanalysis = self.rma_transform(rmanalysis_df)
                    self.update_rmanalysis(rmanalysis)
                    print('\t%s' % fname,':Update Successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        except:
            print('filter_processing error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')