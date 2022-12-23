import sys
import pyodbc
import configparser
import pandas as pd
import datetime

class RMDBB():
    parser = configparser.ConfigParser()
    parser.read('RMAnalysis.ini')
    SQL_driver = parser['myserv']['SQL_driver']
    Server = parser['myserv']['Server']
    Database = parser['myserv']['Database']
    uid = parser['myserv']['uid']
    pwd = parser['myserv']['pwd']

    def __init__(self):
        con_string = f'Driver={self.SQL_driver};Server={self.Server};Database={self.Database};UID={self.uid};pwd={self.pwd}'
        # con_string = f'Driver={self.SQL_driver};Server={self.Server};Database={self.Database};Trusted_Connection=yes;'
        try:
            self.conn = pyodbc.connect(con_string)
        except Exception as e:
            print(e)
            print('Task is terminate')
            sys.exit()
        else:
            self.cursor = self.conn.cursor()
            print('Conection successfully','(',datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),')')
            
    ########################################################################
    def create_rmbuffer_tbl(self):
        create_rmbuffer_sql = """CREATE TABLE rmbuffer (
            ID int NOT NULL IDENTITY(1,1),
            c_ud varchar(2), 
            c_sample varchar(15), 
            c_inslots varchar(15), 
            c_vendor varchar(15), 
            c_material varchar(15), 
            d_date datetime, 
            c_Batch varchar(15),
            n_MOIS decimal(10,2), 
            n_ASH decimal(10,2), 
            n_PROTEIN decimal(10,2), 
            n_FAT decimal(10,2), 
            n_FIBER decimal(10,2), 
            n_P decimal(10,2), 
            n_Ca decimal(10,2), 
            n_INSOL decimal(10,2), 
            n_NaCl decimal(10,2),
            n_FFA decimal(10,2), 
            n_UA decimal(10,2), 
            n_KOHPS decimal(10,2), 
            n_BRIX decimal(10,2), 
            n_PEPSIN decimal(10,2), 
            n_PEPSIN0002 decimal(10,2), 
            n_NDF decimal(10,2), 
            n_ADF decimal(10,2),
            n_ADL decimal(10,2), 
            n_ETH decimal(10,2), 
            n_T_FAT decimal(10,2), 
            n_TVN decimal(10,2), 
            n_NH3 decimal(10,2), 
            n_Starch decimal(10,2), 
            n_IV decimal(10,2), 
            n_PV decimal(10,2), 
            n_AV decimal(10,2),
            n_Totox decimal(10,2), 
            n_p_anisidine decimal(10,2), 
            n_Xanthophyll decimal(10,2), 
            n_AcInsol decimal(10,2), 
            n_Gluten decimal(10,2),
            n_nut1 decimal(10,2),
            n_nut2 decimal(10,2),
            n_nut3 decimal(10,2),
            n_nut4 decimal(10,2),
            n_nut5 decimal(10,2),
            n_nut6 decimal(10,2),
            n_nut7 decimal(10,2),
            n_nut8 decimal(10,2),
            n_nut9 decimal(10,2),
            n_nut10 decimal(10,2),
            c_plant varchar(10),
            c_mfg varchar(100), 
            c_country_org varchar(100), 
            c_batch_org varchar(100), 
            c_remark varchar(100)
            )
            """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(create_rmbuffer_sql)
        except Exception as e:
            self.cursor.rollback()
            print(e)
            print('create_rmbuffer_sql error')
        else:
            print('create_rmbuffer_sql successful')
            self.cursor.commit()
            self.cursor.close() 
            
    def insert_rmbuffer_tbl(self,data):
        insert_rmbuffer_sql = """INSERT INTO rmbuffer (
            c_ud, 
            c_sample, 
            c_inslots, 
            c_vendor, 
            c_material, 
            d_date, 
            c_Batch,
            n_MOIS, 
            n_ASH, 
            n_PROTEIN, 
            n_FAT, 
            n_FIBER, 
            n_P, 
            n_Ca, 
            n_INSOL, 
            n_NaCl,
            n_FFA, 
            n_UA, 
            n_KOHPS, 
            n_BRIX, 
            n_PEPSIN, 
            n_PEPSIN0002,
            n_NDF, 
            n_ADF,
            n_ADL, 
            n_ETH, 
            n_T_FAT, 
            n_TVN, 
            n_NH3, 
            n_Starch, 
            n_IV, 
            n_PV, 
            n_AV,
            n_Totox, 
            n_p_anisidine, 
            n_Xanthophyll, 
            n_AcInsol, 
            n_Gluten,
            n_nut1,
            n_nut2,
            n_nut3,
            n_nut4,
            n_nut5,
            n_nut6,
            n_nut7,
            n_nut8,
            n_nut9,
            n_nut10, 
            c_plant,
            c_mfg, 
            c_country_org, 
            c_batch_org, 
            c_remark
            ) 
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,\
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,\
                    ?,?,?,?,?,?,?,?,?,?,?,?,?
                    )
        """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(insert_rmbuffer_sql, data)
        except Exception as e:
            self.cursor.rollback()
            print(e)
            print('insert_rmbuffer_tbl error')
        else:
            self.cursor.commit()
            self.cursor.close()
            
    def get_rmbuffer_tbl(self):
        get_rmbuffer_sql = """SELECT * from rmbuffer
        """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(get_rmbuffer_sql)
            data = self.cursor.fetchall()
            data = pd.DataFrame((tuple(t) for t in data))
            data = data.rename(columns={
                0:'ID',
                1:'c_ud',
                2:'c_sample',
                3:'c_inslots',
                4:'c_vendor',
                5:'c_material',
                6:'d_date',
                7:'c_Batch',
                8:'n_MOIS',
                9:'n_ASH',
                10:'n_PROTEIN',
                11:'n_FAT',
                12:'n_FIBER',
                13:'n_P',
                14:'n_Ca',
                15:'n_INSOL',
                16:'n_NaCl',
                17:'n_FFA',
                18:'n_UA',
                19:'n_KOHPS',
                20:'n_BRIX',
                21:'n_PEPSIN',
                22:'n_PEPSIN0002',
                23:'n_NDF',
                24:'n_ADF',
                25:'n_ADL',
                26:'n_ETH',
                27:'n_T_FAT',
                28:'n_TVN',
                29:'n_NH3',
                30:'n_Starch',
                31:'n_IV',
                32:'n_PV',
                33:'n_AV',
                34:'n_Totox',
                35:'n_p_anisidine',
                36:'n_Xanthophyll',
                37:'n_AcInsol',
                38:'n_Gluten',
                39:'n_nut1',
                40:'n_nut2',
                41:'n_nut3',
                42:'n_nut4',
                43:'n_nut5',
                44:'n_nut6',
                45:'n_nut7',
                46:'n_nut8',
                47:'n_nut9',
                48:'n_nut10',
                49:'c_plant',
                50:'c_mfg',
                51:'c_country_org',
                52:'c_batch_org',
                53:'c_remark'
                })
        except Exception as e:
            self.cursor.rollback()
            print(e)
            print('get_rmbuffer_tbl error')
        else:
            self.cursor.commit()
            self.cursor.close()
            return  data
        
    def truncate_rmbuffer_tbl(self):
        truncate_rmbuffer_sql = """TRUNCATE TABLE rmbuffer"""
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(truncate_rmbuffer_sql)
        except Exception as ex:
            print(ex)
            print('truncate_rmbuffer error')
        else:
            self.cursor.commit()
            self.cursor.close()