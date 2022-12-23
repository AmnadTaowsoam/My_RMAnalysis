import datetime
from my_function.rmprepare import Prepare

rmap = Prepare()

if __name__=="__main__":
        try:
            rmap.rma_check_file_data()
            print('Check file rm analysis successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        except:
            print('Check file rm analysis error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
            
        try:
            rmap.rma_upload_data_buffer()
            print('rma_upload_data_buffer successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        except:
            print('rma_upload_data_buffer error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')