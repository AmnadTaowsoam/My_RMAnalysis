import datetime
from my_function.rmanalysis import Processing
from my_db.dbb import RMDBB

dbb = RMDBB()
rmpc = Processing()

if __name__=="__main__":
        try:
            rmpc.update_rmanalysis()
            print('Upload rmanalysis to database successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        except:
            print('Upload rmanalysis to database error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
        try:
            dbb.truncate_rmbuffer_tbl()
            print('truncate_rmbuffer_tbl database successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        except:
            print('truncate_rmbuffer_tbl database error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')