import pandas as pd 
import tarfile
from datetime import datetime
import pathlib 
import requests

def download_data(path_file):
    #Descarga el dataset salaries desde Github hasta la carpeta especificada por el usuario

    #Parametros:
    #path_file (str): ruta de almacenamiento del archivo, junto con su nombre
    
    #Regresa:
    #path_file (str): la ruta donde fue almacenado con éxito el archivo junto con el nombre dle mismo

    location = pathlib.Path(path_file)
    folder = location.parent
    pathlib.Path(folder).mkdir(exist_ok=True)
    url = "https://github.com/lola0206/datos_boyaca/raw/master/datos.tar.gz"
    r = requests.get(url, allow_redirects= True)
    with open(location, "wb") as f:
        f.write(r.content)
    return str(path_file)



def funcion_boyaca(municipio): 
  #Esta función nos ayudará a limpiar los datos de cada municipio

  #Parametros: 
  #municipio (str): nombre del archivo que será procesado

  #Returns: 
  #place: archivo csv guardado en la carpeta boyaca
   
  with tarfile.open("/home/laura/boyaca/boyaca_data/datos.tar.gz", "r:gz") as archivo:   
    archivo.extractall() 


  place = pd.read_csv(f"Profesional_data/{municipio}")  
  print(place.dtypes, place.columns, place.isnull().sum(), place.shape) 
  
  lista = list(place["Municipio"].value_counts().index[0:])  


  place = place[place['Municipio'] == (lista[0])]

  place = place.drop(["Unnamed: 0"], axis=1) 

  place['FechaMedicion'] = pd.to_datetime(place['FechaMedicion']) 

  place.replace({"ene": "01","feb": "02", "mar":"03", "abr" : "04" , "may": "05", "jun": "06", "jul":"07", "ago": "08", "sep": "09", "oct": "10" , "nov": "11", "dic": "12"}, regex=True, inplace=True)  
  place["Fecha Nto"] = pd.to_datetime(place["Fecha Nto"], infer_datetime_format= True) 

  place.to_csv(f"{municipio}", index = False) 
  return place


def concatenar(place1, place2, place3):
  #Concatena los dataframes de las ciudades de Boyacá que serán analizadas

  #Parametros: 
  #place1,place2,place3 (dataframes): archivos a concatenar 

  #Returns: 
  #aq_cum_cu (dataframe): Dataframe concatenado 
  
  place1 = pd.read_csv(place1)
  place2 = pd.read_csv(place2)
  place3 = pd.read_csv(place3)

  aq_cum_cu = pd.concat([place1,place2, place3], axis=0)
  aq_cum_cu.to_csv("aqui_cum_cub.csv", index = False)
  return(aq_cum_cu)

