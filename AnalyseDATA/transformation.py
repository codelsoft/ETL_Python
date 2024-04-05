import datetime
from sqlite3 import Date
from numpy import datetime64
import pandas as pd

class Transformations():
    df_ =None
    
    def __init__(self,source) -> None:
        
        #Chargeons le data Frame csv
        self.df_ = pd.read_csv(source)

    # Remplcez les valeurs nan par zero, 
    def  transformPricePaidColumn(self):
        self.df_["tax"].fillna(0,inplace=True)
        dfcopy = self.df_.copy()
        dfcopy["price_paid"] = dfcopy["price_paid"].apply(lambda x : x.replace("$",""))
        dfcopy["price_paid"] = dfcopy["price_paid"].astype(float)

        return dfcopy

    #Ajoutons une colonne Tax_final = (price_paid * (1-tax)/100)
    def addTaxFinal(self):
        df=self.transformPricePaidColumn()
        t= 1 - df["tax"]/100
        df["tax_final"] = df["price_paid"] * t
        return df
    
    
    #Modifion la colonne gender: Male par "M"  et Femal par "F"
    # Remplacez les valeur nan de cette colonne par le sex qui paie plus d'impot en comparant leurs moyennes
     
    def transformColumnGender(self):
        df = self.addTaxFinal()
        dfcopy = df.copy()
        dfcopy["gender"] = dfcopy["gender"].astype(str)
        dfcopy["gender"] = dfcopy["gender"].apply(lambda x : x.replace("Male","M"))
        dfcopy["gender"] = dfcopy["gender"].apply(lambda x : x.replace("Female","F"))
        
        # Calcul  la moyenne de tax des Male:
        dfcopy_m = dfcopy[dfcopy["gender"]=="M"]
        moyenne_M = dfcopy_m["tax_final"].mean()

        dfcopy_f = dfcopy[dfcopy["gender"]=="F"]
        
        moyenne_F = dfcopy_f["tax_final"].mean() 
        #Comparaison des  moyennes
        
        if moyenne_M >= moyenne_F  :  
                 dfcopy["gender"] = dfcopy["gender"].apply(lambda x : x.replace("nan","M"))
                  
        else:
                 dfcopy["gender"] = dfcopy["gender"].apply(lambda x : x.replace("nan","F")) 
          
        return   dfcopy
    
    #TransfomColumnDate()
    def transformColumnDate(self):
        df=self.transformColumnGender()
        splite = df["date"].str.split('/', expand=True)
        new_date = splite[2] + "-" + splite[0] + "-" + splite[1]
      
        
        df["date"] = pd.to_datetime(new_date,infer_datetime_format=True)
        df["month"] = splite[0] 
        df["year"] = splite[2] 
        return df


    # Supprimons les colonnes qui ne servent à rien pour les analyses
    def DeleteUnituleColumns(self):
        listecolumns=["last_name","first_name","email","ip_address"]
        # recuperere les column  puis teser si la liste  existe  
        df=self.transformColumnDate()
        column_ = list(df.columns.values)

        new_c=[i for i in column_   if i in listecolumns]   
        if  new_c :
            df.drop(listecolumns, axis=1,inplace=True)
            del df["id"]
        
        
        
        return df
        
    # remplacer  le pays qui a  plus des population qui paye le taxe par les valeurs nan de la collonne   country 
    def   replceNanByCountry(self) :
        df = self.DeleteUnituleColumns().copy()
        df_group = df.groupby("country")
        df_group_tax_final = df_group[["tax_final"]].apply(lambda x : x.count())
 
        country_dic=dict(zip(list(df_group_tax_final.index),df_group_tax_final["tax_final"].to_list()))

        res=[ x for x,y in country_dic.items()  if y == max(df_group_tax_final["tax_final"].to_list()) ]
        
        if res[0]:
            df["country"] = df["country"].astype(str)
            df["country"] = df["country"].apply(lambda x : x.replace("nan",res[0]))

        return df
    
    
     

    #Chargeons les données  dans un fichier csv "data_loading.csv"

    def chargeDataToCSV(self):
        df_complete = self.replceNanByCountry()
        df_complete.to_excel("data_loading.xlsx")



         


