import pandas as pd 
import numpy as np
import os
import unicodedata

from datetime import datetime
# from meteostat import Hourly, Point, Daily



# paris = Point(48.8566, 2.3522)

dossier = os.path.join('..', 'data')

l_float_prod = ['Prix ​​unitaire TTC', 'Remises', 'CA TTC', 'CA HT', 'Montant TVA']
l_int_prod = ['ID Etablissement', 'Quantité']
l_date = ['Date ouverture',	'Date fermeture']


def convert_to_float(df: pd.DataFrame,
                     l
                     ) -> pd.DataFrame:
    '''
        This function convert target into float
        Parameters
        ----------
        df: pd.DataFrame
        l: List[str]

        Returns
        df: pd.DataFrame
    '''

    df[l] = df[l].replace(',', '.', regex=True).astype('float')
    return df


def convert_to_int(df: pd.DataFrame,
                   l
                   )-> pd.DataFrame:
    '''
        This function convert target into int
        Parameters
        ----------
        df: pd.DataFrame
        l: List[str]

        Returns
        df: pd.DataFrame
    '''
    df[l] = df[l].astype('int')
    return df


def convert_to_date(df: pd.DataFrame,
                   l 
                   )->pd.DataFrame:
    '''
        This function convert target into datetime
        Parameters
        ----------
        df: pd.DataFrame
        l: List[str]

        Returns
        df: pd.DataFrame
    '''
    df[l] = df[l].apply(pd.to_datetime)
    return df



def concat_csv_files(directory):
    """
        Concatène tous les fichiers CSV dans un répertoire donné en un seul fichier CSV.
        Parameters
        ----------

            directory: Chemin du répertoire contenant les fichiers CSV.
            output_file: Chemin du fichier CSV de sortie.
    """
    # Liste pour stocker les DataFrames
    tracked_files = os.path.join(directory, "tracked_files.txt")
    combined_csv_path = os.path.join(directory, "combined.csv")

    if not os.path.exists(tracked_files):
        with open(tracked_files, "w") as file:
            pass 

    with open(tracked_files, "r") as file:
        processed_files = set(file.read().splitlines())

    dataframes = []
    new_files = []

    # Parcourir tous les fichiers dans le répertoire
    for file_name in os.listdir(directory):
        # Vérifier si le fichier est un CSV
        if file_name.endswith('.csv') and file_name not in processed_files:
            file_path = os.path.join(directory, file_name)
            # Lire le fichier CSV
            df = pd.read_csv(file_path, sep=';')
            dataframes.append(df)
            new_files.append(file_name)
            print(f"Fichier chargé : {file_name}")


    # Concaténer tous les DataFrames lors de la première exécution
    if not os.path.exists(combined_csv_path):
        if dataframes:
            combined_df = pd.concat(dataframes, ignore_index=True)
            print("Concaténation réussie des nouveaux fichiers.")
        else:
            combined_df = pd.DataFrame()  # DataFrame vide si aucun nouveau fichier
            print("Aucun nouveau fichier à concaténer.")
    else:
        combined_df = pd.read_csv(combined_csv_path, sep=';')
        if dataframes:
            combined_df = pd.concat([combined_df] + dataframes, ignore_index=True)
            print("Nouveaux fichiers concaténés au fichier existant.")

    combined_df.to_csv(combined_csv_path, index=False, sep=';')
    print(f"Fichier combiné sauvegardé dans : {combined_csv_path}")

    # Mettre à jour le fichier de suivi
    with open(tracked_files, "a") as file:
        for file_name in new_files:
            file.write(file_name + "\n")
    # Sauvegarder dans le fichier de sortie
    # combined_df.to_csv(output_file, index=False)
    #print(f"Fichiers combinés enregistrés dans : {output_file}")
    return combined_df


def concat_csv_folder(directory):
    """
    Concatène tous les fichiers CSV dans un répertoire donné en un seul DataFrame.
    
    Parameters
    ----------
    directory : str
        Chemin du répertoire contenant les fichiers CSV.
        
    Returns
    -------
    pd.DataFrame
        DataFrame combiné contenant les données de tous les fichiers CSV.
    """

    dataframes = []

    for file_name in os.listdir(directory):

        if file_name.endswith('.csv'):
            file_path = os.path.join(directory, file_name)
            try:

                df = pd.read_csv(file_path, sep=';')
                dataframes.append(df)
                print(f"Fichier chargé : {file_name}")
            except Exception as e:
                print(f"Erreur lors de la lecture de {file_name}: {e}")

    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
        print("Concaténation réussie de tous les fichiers.")
    else:
        combined_df = pd.DataFrame()
        print("Aucun fichier CSV trouvé dans le répertoire.")
    return combined_df


def read_prod():
    '''
        This function read the prod data
        and change the variable into the right type
        and create new temporal variables that are needed
        in the future. Also drop useless variables for the
        study.

        Parameters
        ----------
        l_float_pay: List[str], variable to convert
                    into float
        l_int_pay: List[str], variable to convert 
                into int
        l_date: List[str], variable to convert into
                datetime
        file_pay: str, the file payment to read
        
        Returns
        df_prod: pd.DataFrame
    '''
    combined_df = concat_csv_folder(dossier)

    df = combined_df.copy()
    df = convert_to_float(df, l_float_prod)
    df = convert_to_int(df, l_int_prod)
    df = convert_to_date(df, l_date)
    df.sort_values(by='Date ouverture', ascending=True, inplace=True)
    df.drop(['Etablissement', 'ID Etablissement', 'Devise', 'SKU', 'Code de la carte-cadeau'],
             axis=1,
            inplace=True
           )
    df.loc[:, 'Heure'] = df['Date ouverture'].dt.hour
    df.loc[:, 'Jour_semaine'] = df['Date ouverture'].dt.day_name()
    df.loc[:, 'Jour'] = df['Date ouverture'].dt.date
    df.loc[:, 'Mois'] = df['Date ouverture'].dt.month
    df.loc[:, 'Semaine'] = df['Date ouverture'].dt.isocalendar().week
    df.loc[:, 'Annee'] = df['Date ouverture'].dt.year

    df.loc[:, 'Heure_jour'] = df['Date ouverture'].dt.floor('h')
    df.loc[:, 'week_jour'] = df['Date ouverture'].dt.to_period('W')

    return df
