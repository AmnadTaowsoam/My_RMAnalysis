import datetime
from my_function.rmanalysis import Processing

rma = Processing()

if __name__=="__main__":
        try:
            rma.rma_processing()
            print('Update all rm analysis successfully','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')
        except:
            print('Update rm analysis error','(',datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),')')