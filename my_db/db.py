import sys
import pyodbc
import configparser
import pandas as pd
import datetime

class RMDB():
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
    def create_rmanalysis_tbl(self):
        create_rmanalysis_sql = """CREATE TABLE rmanalysis (
            ID int NOT NULL IDENTITY(1,1),
            ud varchar(2), 
            sample varchar(15), 
            inslots varchar(15), 
            vendor varchar(15), 
            material varchar(15), 
            date datetime, 
            Batch varchar(15),
            MOIS decimal(10,4), 
            ASH decimal(10,4), 
            PROTEIN decimal(10,4), 
            FAT decimal(10,4), 
            FIBER decimal(10,4), 
            P decimal(10,4), 
            Ca decimal(10,4), 
            INSOL decimal(10,4), 
            NaCl decimal(10,4),
            FFA decimal(10,4), 
            UA decimal(10,4), 
            KOHPS decimal(10,4), 
            BRIX decimal(10,4), 
            PEPSIN decimal(10,4), 
            PEPSIN0002 decimal(10,4), 
            NDF decimal(10,4), 
            ADF decimal(10,4),
            ADL decimal(10,4), 
            ETH decimal(10,4), 
            T_FAT decimal(10,4), 
            TVN decimal(10,4), 
            NH3 decimal(10,4), 
            Starch decimal(10,4), 
            IV decimal(10,4), 
            PV decimal(10,4), 
            AV decimal(10,4),
            Totox decimal(10,4), 
            p_anisidine decimal(10,4), 
            Xanthophyll decimal(10,4), 
            AcInsol decimal(10,4), 
            Gluten decimal(10,4),
            n_nut1 decimal(10,4),
            n_nut2 decimal(10,4),
            n_nut3 decimal(10,4),
            n_nut4 decimal(10,4),
            n_nut5 decimal(10,4),
            n_nut6 decimal(10,4),
            n_nut7 decimal(10,4),
            n_nut8 decimal(10,4),
            n_nut9 decimal(10,4),
            n_nut10 decimal(10,4),
            plant varchar(10),
            mfg varchar(100), 
            country_org varchar(100), 
            batch_org varchar(100), 
            remark varchar(100)
            )
            """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(create_rmanalysis_sql)
        except Exception as e:
            self.cursor.rollback()
            print(e)
            print('create_rmanalysis_sql error')
        else:
            print('create_rmanalysis_sql successful')
            self.cursor.commit()
            self.cursor.close() 
            
    def insert_rmanalysis_tbl(self,data):
        insert_rmanalysis_sql = """INSERT INTO rmanalysis (
            ud, 
            sample, 
            inslots, 
            vendor, 
            material, 
            date, 
            Batch,
            MOIS, 
            ASH, 
            PROTEIN, 
            FAT, 
            FIBER, 
            P, 
            Ca, 
            INSOL, 
            NaCl,
            FFA, 
            UA, 
            KOHPS, 
            BRIX, 
            PEPSIN, 
            PEPSIN0002,
            NDF, 
            ADF,
            ADL, 
            ETH, 
            T_FAT, 
            TVN, 
            NH3, 
            Starch, 
            IV, 
            PV, 
            AV,
            Totox, 
            p_anisidine, 
            Xanthophyll, 
            AcInsol, 
            Gluten,
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
            plant,
            mfg, 
            country_org, 
            batch_org, 
            remark
            ) 
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,\
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,\
                    ?,?,?,?,?,?,?,?,?,?,?,?,?
                    )
        """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(insert_rmanalysis_sql, data)
        except Exception as e:
            self.cursor.rollback()
            print(e)
            print('insert_copellet_tbl error')
        else:
            self.cursor.commit()
            self.cursor.close()
            
    def get_rmanalysis_tbl(self):
        get_rmanalysis_sql = """SELECT * from rmanalysis
        """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(get_rmanalysis_sql)
            data = self.cursor.fetchall()
            data = pd.DataFrame((tuple(t) for t in data))
            data = data.rename(columns={
                0:'ID',
                1:'ud', 
                2:'sample', 
                3:'inslots', 
                4:'vendor', 
                5:'material', 
                6:'date', 
                7:'Batch',
                8:'MOIS', 
                9:'ASH', 
                10:'PROTEIN', 
                11:'FAT', 
                12:'FIBER', 
                13:'P', 
                14:'Ca', 
                15:'INSOL', 
                16:'NaCl',
                17:'FFA', 
                18:'UA', 
                19:'KOHPS', 
                20:'BRIX', 
                21:'PEPSIN', 
                22:'PEPSIN0002', 
                23:'NDF', 
                24:'ADF',
                25:'ADL', 
                26:'ETH', 
                27:'T_FAT', 
                28:'TVN', 
                29:'NH3', 
                30:'Starch', 
                31:'IV', 
                32:'PV', 
                33:'AV',
                34:'Totox', 
                35:'p_anisidine', 
                36:'Xanthophyll', 
                37:'AcInsol', 
                38:'Gluten',
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
                49:'plant',
                50:'mfg', 
                51:'country_org', 
                52:'batch_org', 
                53:'remark' 
                })
        except Exception as e:
            self.cursor.rollback()
            print(e)
            print('get_rmanalysis_tbl error')
        else:
            self.cursor.commit()
            self.cursor.close()
            return  data
        
    def truncate_rmanalysis_tbl(self):
        truncate_rmanalysis_sql = """TRUNCATE TABLE rmanalysis"""
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(truncate_rmanalysis_sql)
        except Exception as ex:
            print(ex)
            print('truncate_rmanalysis error')
        else:
            self.cursor.commit()
            self.cursor.close()