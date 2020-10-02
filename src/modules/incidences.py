"""
Incident generator module
"""
from time import struct_time, localtime
from collections import defaultdict
from pandas import DataFrame


def generate_incidences(df, incidences):
    """ Returns a copy of the dataframe df adding the indicated incidents.
    The provided dataframe is copied, the values ​​are modified according to the incidents
    and the column 'incidence' will be added, which will be True if that vector was modified.
   
    Input:
        df: Dataframe that has the vectors with the network values ​​every 5 minutes
        incidents: List of incidents where each incident is a dictionary with:
            - from (struct_time): Date the incident starts
            - until (struct_time): Date when the incident ends
            - proportion: Proportion of users affected by the incident
            - intensity: intensity of the incidence
            - col: Column of the dataframe affected by the incident
    
    """
    df = df.copy(deep=True)
    isincidence = create_identificador_de_incidences(incidences)

    column_incidences = []
    for index, row in df.iterrows():
        incidence_generated = False
        if isincidence(row):
            # We add the modifications of the incidences of this row
            for incidence in info_of_incidences(row, incidences):
                col = incidence['column']
                proportion = incidence['proportion']
                intensity = incidence['intensity']

                initial_value = row[col]
                if initial_value == 0:
                    initial_value = df[col].mean()
                if initial_value != 0:
                    df[col][
                        index] = initial_value * proportion * intensity + initial_value * (
                            1 - proportion)
                    incidence_generated = True
        column_incidences.append(incidence_generated)

    df['incidence'] = column_incidences
    return df


def create_identificador_de_incidences(incidences):
    """Returns a lambda that tells you whether or not an incidence should be added to a row
    """
    isincidence_lista = []
    for incidence in incidences:
        aux = lambda row: time_obj(row) >= incidence['from'] and time_obj(
            row) <= incidence['until']
        isincidence_lista.append(aux)

    isincidence = lambda row: any(aux(row) for aux in isincidence_lista)
    return isincidence


def info_of_incidences(row, incidences):
    """Returns a list with the incidences to add to the row
    Each item in the list has a dictionary with:
        - column: column that is subject to the modification
        - proportion: proportion of users affected by incidence
        - intensity: intensity of incidence
    """
    info = []
    for incidence in incidences:
        if time_obj(row) >= incidence['from'] and time_obj(
                row) <= incidence['until']:
            aux = {}
            aux['column'] = incidence['column']
            aux['proportion'] = incidence['proportion']
            aux['intensity'] = incidence['intensity']
            info.append(aux)
    return info


def time_obj(row) -> struct_time:
    """Gives the time structure of a row of a dataframe
    """
    return localtime(row['tref_start'] / 1000)
