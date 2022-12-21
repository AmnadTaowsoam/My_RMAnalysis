from my_db.db import RMDB

db = RMDB()

if __name__=="__main__":
        try:
            db.create_rmanalysis_tbl()
        except:
            print('create_copellet_tbl error')