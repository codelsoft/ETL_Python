from transformation import Transformations





if __name__ == '__main__':
     
     trans_Obj = Transformations("data.csv")
     
     df = trans_Obj.df_
     print(trans_Obj.chargeDataToCSV())
     

    
     