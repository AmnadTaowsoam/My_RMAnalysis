import os, shutil
import datetime
import pandas as pd
import numpy as np
np.set_printoptions(suppress=True)

import warnings
warnings.filterwarnings("ignore")

from my_db.dbb import RMDBB

dbb = RMDBB()

class Prepare():
    def __init__(self) -> None:
        pass
    
    def columns_rename(self,input_data):
        try:
            data = input_data.rename(columns={
                            'Operation short text':'c_opt',
                            'Usage Decision Code':'c_ud',
                            'Sample No.':'c_sample',
                            'inspection Lot':'c_inslots',
                            'Code-SP':'c_vendor',
                            'SP-Name':'c_vendor_name',
                            'Old Code': 'c_oldcode',
                            'Code-RM':'c_material',
                            'RM-Name':'c_desc',
                            'DATE':'d_date',
                            'Batch':'c_Batch',
                            'Material Doc.':'c_matdocs',
                            'MOIS':'n_MOIS',
                            'ASH':'n_ASH',
                            'PROTEIN':'n_PROTEIN',
                            'FAT':'n_FAT',
                            'FIBER':'n_FIBER',
                            'P':'n_P',
                            'Ca':'n_Ca',
                            'INSOL':'n_INSOL',
                            'NaCl':'n_NaCl',
                            'F.F.A.':'n_FFA',
                            'U.A.':'n_UA',
                            'KOHPS':'n_KOHPS',
                            'BRIX':'n_BRIX',
                            'PEPSIN':'n_PEPSIN',
                            'PEPSIN0002':'n_PEPSIN0002',
                            'NDF':'n_NDF',
                            'ADF':'n_ADF',
                            'ADL':'n_ADL',
                            'ETH.':'n_ETH',
                            'T_FAT':'n_T_FAT',
                            'TVN':'n_TVN',
                            'NH3':'n_NH3',
                            'Starch':'n_Starch',
                            'IV':'n_IV',
                            'PV\n':'n_PV',
                            'AV\n':'n_AV',
                            'Totox':'n_Totox',
                            'p-anisidine':'n_p_anisidine',
                            'Xanthophyll':'n_Xanthophyll',
                            'Ac. Insol':'n_AcInsol',
                            'Gluten\n':'n_Gluten',
                            'Plant':'c_plant',
                            'Plant Origin Name':'c_plant_org',
                            'ผู้ผลิต':'c_mfg',
                            'ประเทศ':'c_country_org',
                            'Batch Origin':'c_batch_org',
                            'REMARK':'c_remark'
                            })
            data = data.drop(columns={
                'c_opt',
                'c_vendor_name',
                'c_desc',
                'c_oldcode',
                'c_matdocs',
                'c_plant_org'
                })
            data = data.fillna(0)
            return data
        except:
            print('Columns rename error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
    def columns_cleansing(self, input_data):
        try:
            data = input_data.replace("'", "")
            data = input_data.replace('"', "")
            data = data[data['c_material'] !=0]
            data = data.fillna(0)
            return data
        except:
            print('Columns cleansing error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
    def columns_transform(self,input_data):
        try:
            data = input_data.copy()
            #fill na = 0
            data[['c_ud', 'c_sample', 'c_inslots', 'c_vendor', 'c_material', 'd_date', 'c_Batch']] = data[['c_ud', 'c_sample', 'c_inslots', 'c_vendor', 'c_material', 'd_date', 'c_Batch']]
            data = data.reset_index().rename(columns={'index':'n_runnings'})
            data['n_runnings'] = data['n_runnings'] +1
            data['c_sample2'] = np.where((data['c_sample'] == 0),data['n_runnings'], data['c_sample'])
            data = data.drop(columns={'c_sample','n_runnings'})
            data = data.rename(columns={'c_sample2':'c_sample'})

            
            data['c_inslots'] = np.where((data['c_ud'] == 0) ,'Recheck', data['c_inslots'])
            data['c_ud'] = np.where(((data['c_material'].str[:2] == 'WG') | (data['c_vendor'] == 0)| (data['c_Batch'] == 0)),'A', data['c_ud'])
            data['c_ud'] = np.where(((data['c_sample'] == 0) | (data['c_inslots'] == 0)),'A', data['c_ud'])
            data['c_inslots'] = np.where((data['c_vendor'] == 0),'Inprocess', data['c_inslots'])
            return data
        except:
            print('Columns transform data type error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
    def columns_detype(self, input_data):
        try:
            data = input_data.copy()
            data[['n_nut1','n_nut2','n_nut3','n_nut4','n_nut5','n_nut6','n_nut7','n_nut8','n_nut9','n_nut10']] = 0.0
            col_all = ['c_ud','c_sample','c_inslots','c_vendor','c_material','d_date','c_Batch','n_MOIS','n_ASH','n_PROTEIN',\
                                    'n_FAT','n_FIBER','n_P','n_Ca','n_INSOL','n_NaCl','n_FFA','n_UA','n_KOHPS','n_BRIX','n_PEPSIN','n_PEPSIN0002',\
                                    'n_NDF','n_ADF','n_ADL','n_ETH','n_T_FAT','n_TVN','n_NH3', 'n_Starch','n_IV','n_PV','n_AV','n_Totox',\
                                    'n_p_anisidine','n_Xanthophyll','n_AcInsol','n_Gluten','n_nut1','n_nut2','n_nut3','n_nut4',\
                                    'n_nut5','n_nut6','n_nut7','n_nut8','n_nut9','n_nut10', 'c_plant','c_mfg','c_country_org',\
                                    'c_batch_org','c_remark']
            col_str = ['c_ud','c_sample','c_inslots','c_vendor','c_material','c_Batch','c_plant','c_mfg','c_country_org','c_batch_org','c_remark']
            col_num = ['n_MOIS','n_ASH','n_PROTEIN','n_FAT','n_FIBER','n_P','n_Ca','n_INSOL','n_NaCl','n_FFA','n_UA','n_KOHPS','n_BRIX',\
                                    'n_PEPSIN','n_PEPSIN0002','n_NDF','n_ADF','n_ADL','n_ETH','n_T_FAT','n_TVN','n_NH3','n_Starch','n_IV','n_PV',\
                                    'n_AV','n_Totox','n_p_anisidine','n_Xanthophyll','n_AcInsol','n_Gluten','n_nut1','n_nut2','n_nut3',\
                                    'n_nut4','n_nut5','n_nut6','n_nut7','n_nut8','n_nut9','n_nut10']
            data[col_str] = data[col_str].astype(pd.StringDtype())
            data[col_str] = data[col_str].astype(pd.StringDtype())
            data['c_inslots'] = data['c_inslots'].str[:11]
            data['c_sample'] = data['c_sample'].str[:11]
            data[col_num] = data[col_num].astype(float)
            data = data[col_all]
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
            
    def rma_check_file_data(self):
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
                    rmanalysis.to_excel("./documents/rmanalysis_pending/"+'C_' + fname )
                    os.remove("./documents/rmanalysis/"+fname)
                    print('\t%s' % fname,':Check file Successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        except:
            print('processing error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
    
######### Upload data to buffer ###########################
    def columns_detype_buffer(self,input_data):
        try:
            data = input_data.copy()
            data[['n_nut1','n_nut2','n_nut3','n_nut4','n_nut5','n_nut6','n_nut7','n_nut8','n_nut9','n_nut10']] = 0.0
            col_all = ['c_ud','c_sample','c_inslots','c_vendor','c_material','d_date','c_Batch','n_MOIS','n_ASH','n_PROTEIN',\
                                    'n_FAT','n_FIBER','n_P','n_Ca','n_INSOL','n_NaCl','n_FFA','n_UA','n_KOHPS','n_BRIX','n_PEPSIN','n_PEPSIN0002',\
                                    'n_NDF','n_ADF','n_ADL','n_ETH','n_T_FAT','n_TVN','n_NH3', 'n_Starch','n_IV','n_PV','n_AV','n_Totox',\
                                    'n_p_anisidine','n_Xanthophyll','n_AcInsol','n_Gluten','n_nut1','n_nut2','n_nut3','n_nut4',\
                                    'n_nut5','n_nut6','n_nut7','n_nut8','n_nut9','n_nut10', 'c_plant','c_mfg','c_country_org',\
                                    'c_batch_org','c_remark']
            col_str = ['c_ud','c_sample','c_inslots','c_vendor','c_material','c_Batch','c_plant','c_mfg','c_country_org','c_batch_org','c_remark']
            col_num = ['n_MOIS','n_ASH','n_PROTEIN','n_FAT','n_FIBER','n_P','n_Ca','n_INSOL','n_NaCl','n_FFA','n_UA','n_KOHPS','n_BRIX',\
                                    'n_PEPSIN','n_PEPSIN0002','n_NDF','n_ADF','n_ADL','n_ETH','n_T_FAT','n_TVN','n_NH3','n_Starch','n_IV','n_PV',\
                                    'n_AV','n_Totox','n_p_anisidine','n_Xanthophyll','n_AcInsol','n_Gluten','n_nut1','n_nut2','n_nut3',\
                                    'n_nut4','n_nut5','n_nut6','n_nut7','n_nut8','n_nut9','n_nut10']
            data[col_str] = data[col_str].astype(pd.StringDtype())
            data[col_str] = data[col_str].astype(pd.StringDtype())
            data['c_inslots'] = data['c_inslots'].str[:11]
            data['c_sample'] = data['c_sample'].str[:11]
            data[col_num] = data[col_num].astype(float)
            data = data[col_all]
            return data
        except:
            print('Columns change data type error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
    
    def update_rmbuffer(self,input_data):
        try:
            rma = input_data.copy()
            for i in range(len(rma)):
                rmas = rma.values.tolist()
                rmas = rmas[i][0],rmas[i][1],rmas[i][2],rmas[i][3],rmas[i][4],rmas[i][5],rmas[i][6],rmas[i][7],rmas[i][8],rmas[i][9],rmas[i][10],\
                    rmas[i][11],rmas[i][12],rmas[i][13],rmas[i][14],rmas[i][15],rmas[i][16],rmas[i][17],rmas[i][18],rmas[i][19],rmas[i][20],\
                    rmas[i][21],rmas[i][22],rmas[i][23],rmas[i][24],rmas[i][25],rmas[i][26],rmas[i][27],rmas[i][28],rmas[i][29],rmas[i][30],\
                    rmas[i][31],rmas[i][32],rmas[i][33],rmas[i][34],rmas[i][35],rmas[i][36],rmas[i][37],rmas[i][38],rmas[i][39],rmas[i][40],\
                    rmas[i][41],rmas[i][42],rmas[i][43],rmas[i][44],rmas[i][45],rmas[i][46],rmas[i][47],rmas[i][48],rmas[i][49],rmas[i][50],\
                    rmas[i][51],rmas[i][52]
                dbb.insert_rmbuffer_tbl(rmas)
        except:
            print('Update rmanalysis error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
    
    def rma_upload_data_buffer(self):
        try:
            # Set the directory you want to start from
            rootDir = './documents/rmanalysis_pending/'
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
                    rmanalysis = self.columns_detype_buffer(rmanalysis_df)
                    self.update_rmbuffer(rmanalysis)
                    shutil.move("./documents/rmanalysis_pending/"+fname, "./documents/rmanalysis_complete/" + fname)
                    print('\t%s' % fname,':rma_upload_data_buffer Successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
                
        except:
            print('rma_upload_data_buffer error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')