from my_db.db import RMDB
from my_db.dbb import RMDBB

db = RMDB()
dbb = RMDBB()
if __name__=="__main__":
        try:
            db.create_rmanalysis_tbl()
            dbb.create_rmbuffer_tbl()
        except:
            print('create_copellet_tbl error')