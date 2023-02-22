import datetime
from my_db.apidb import APIRMDB

apirm = APIRMDB()

class RM_API():
    def __init__(self) -> None:
        pass
    
    def get_data_db(self,startdate,enddate,vendor,material,plant):
        try:
            data = apirm.get_rmanalysis_tbl(startdate,enddate,vendor,material,plant)
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
            data[col_num] = data[col_num].astype(float)
            
            col_sel = ['c_vendor','c_material','d_date','c_Batch','n_MOIS','n_ASH','n_PROTEIN',\
                        'n_FAT','n_FIBER','n_P','n_Ca','n_INSOL','n_NaCl','n_FFA','n_UA','n_KOHPS','n_BRIX','n_PEPSIN','n_PEPSIN0002',\
                        'n_NDF','n_ADF','n_ADL','n_ETH','n_T_FAT','n_TVN','n_NH3', 'n_Starch','n_IV','n_PV','n_AV','n_Totox',\
                        'n_p_anisidine','n_Xanthophyll','n_AcInsol','n_Gluten','c_plant','c_mfg','c_country_org',\
                        ]
            data = data[col_sel]
            return data
        except:
            print('get_data_db error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
    
    def columns_rename(self,startdate,enddate,vendor,material,plant):
        try:
            data = self.get_data_db(startdate,enddate,vendor,material,plant)
            data = data.rename(columns={
                                                'c_ud':'UD',
                                                'c_sample':'sampleNo',
                                                'c_inslots':'inspectionLot',
                                                'c_vendor':'vendor',
                                                'c_material':'material',
                                                'd_date':'date',
                                                'c_Batch':'batch',
                                                'n_MOIS':'MOIS',
                                                'n_ASH': 'ASH',
                                                'n_PROTEIN':'PROTEIN',
                                                'n_FAT':'FAT',
                                                'n_FIBER':'FIBER',
                                                'n_P':'P',
                                                'n_Ca':'Ca',
                                                'n_INSOL':'INSOL',
                                                'n_NaCl':'NaCl',
                                                'n_FFA':'FFA',
                                                'n_UA':'UA',
                                                'n_KOHPS':'KOHPS',
                                                'n_BRIX':'BRIX',
                                                'n_PEPSIN':'PEPSIN',
                                                'n_PEPSIN0002':'PEPSIN0002',
                                                'n_NDF':'NDF',
                                                'n_ADF':'ADF',
                                                'n_ADL':'ADL',
                                                'n_ETH':'ETH',
                                                'n_T_FAT':'T_FAT',
                                                'n_TVN':'TVN',
                                                'n_NH3':'NH3',
                                                'n_Starch':'Starch',
                                                'n_IV':'IV',
                                                'n_PV':'PV',
                                                'n_AV':'AV',
                                                'n_Totox':'Totox',
                                                'n_p_anisidine':'p_anisidine',
                                                'n_Xanthophyll':'Xanthophyll',
                                                'n_AcInsol':'AcInsol',
                                                'n_Gluten':'Gluten',
                                                'c_plant':'plant',
                                                'c_mfg':'mfg',
                                                'c_country_org':'country_org',
                                                'c_batch_org':'batch_org',
                                                'c_remark':'remark'
                                                })
            
            return data
        except:
            print('Columns rename error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
    def processing(self,startdate,enddate,vendor,material,plant):
        data = self.columns_rename(startdate,enddate,vendor,material,plant)
        rootDir = './APIs_data/'
        filename = f"RMAnalysis_{plant}_{material}_{vendor} between {startdate} & {enddate}"
        data.to_excel(rootDir + filename + '.xlsx')
        return data