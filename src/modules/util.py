"""
Utilities for:
1. Load data
2. Load algorithms
3. Keep a cache of the incidents that have already been generated previously
4. Clear data
5. Print result
"""
import json
import os
from modules.incidences import generate_incidences
from time import struct_time, strptime
import pandas as pd
import sys


def set_time_structs(incidences):
    """Add two fields to each initiation:
    - from: Object of type time with the moment it starts
    - up to: Self-explanatory
    """
    for ele in incidences:
        ele['from'] = strptime(ele['from'], "%H:%M:%S %d-%m-%Y")
        ele['until'] = strptime(ele['until'], "%H:%M:%S %d-%m-%Y")
    return incidences


def get_df_with_inc(df, incidence, incidence_name, incidenceH):
    """Returns the dataframe with the generated incidences
    If it is in cache it is directly retrieved and returned
    If not, then it is generated, cached and returned
    """
    id = incidenceH.getId(incidence)
    if id is not None:  # If it is cached
        cacheFile = incidenceH.getIncidenceDFFile(id, False)
        if cacheFile is not None:
            df = pd.read_csv(cacheFile)
            return df
    # If not is cached
    df = generate_incidences(df, incidence)
    incidenceH.addDF_toIncidence(incidence, incidence_name, df, False)
    return df


def get_df_difference(df, incidence, incidence_name, incidenceH):
    """Returns the dataframe of the difference of df
    It is passed inc to be able to check if the dataframe is cached
    If it is in cache, it is retrieved and returned.
    If it is not, then the difference is generated, saved and returned
    """
    id = incidenceH.getId(incidence)
    if id is not None:  # if it is cached
        cacheFile = incidenceH.getIncidenceDFFile(id, True)
        if cacheFile is not None:
            df = pd.read_csv(cacheFile)
            return df
    # If not cached
    df = generate_differences_by_ip(df)
    incidenceH.addDF_toIncidence(incidence, incidence_name, df, True)
    return df


def loadData(datafile, incfiles, incidenceH):
    """
   Load and generate incidents in a dataframe
    Input:
        - Datafile: File with the initial df on which they will be generated
                    incidences
        - Incfiles: List of pairs ->
                    (training_incidences_file, equal_validation)
    Output:
        - Dataframe without incidences
        - List of tuples of 4 dataframes:
            1. DF with training incidences
            2. DF with validation incidences
            3. Same as 1 but calculating the differences
            4. Same as 2 but calculating the differences
        - List of incident id
        There will be one item in the lists for each pair in incfiles
    """
    df = pd.read_csv(datafile)
    df = trim_dataframe(df)
    # We create the list of id_incidences and dataframess
    df_list, id_incidences = [], []
    for file_e, file_v in incfiles:
        inc_e = get_working_incidence(file_e)
        inc_v = get_working_incidence(file_v)
        # We generate incidents
        df_e = get_df_with_inc(df, inc_e, file_e, incidenceH)
        df_v = get_df_with_inc(df, inc_v, file_e, incidenceH)
        df_e_dif = get_df_difference(df_e, inc_e, file_e, incidenceH)
        df_v_dif = get_df_difference(df_v, inc_v, file_e, incidenceH)
        # We save the dataframes and ids of the generated incidents
        df_list.append((df_e, df_v, df_e_dif, df_v_dif))
        id_incidences.append(incidenceH.getId(inc_e))
    return df, df_list, id_incidences


def get_working_incidence(file):
    """Returns an incident with the appropriate structure for
    let the incident generator work
    """
    with open(file, 'r') as f:
        inc = set_time_structs(json.load(f))
    return inc


def trim_dataframe(df):
    """Eliminates columns that are not relevant or that are not numeric (except IP)
    """
    non_numerical_cols = [
        'proto', 'label', 'Unnamed: 0', 'dupAckPerc', 'fallPerc',
        'Unnamed: 0.1'
    ]
    non_useful_cols = [
        'noRespClientPerc', 'noRespServerPerc', 'numberCnxPerc',
        'resetClientPerc', 'resetServerPerc', 'rttPerCnxPerc', 'rtxPerc',
        'synPerc', 'ttl1Perc', 'win0Perc', 'metric'
    ]
    for col in non_numerical_cols + non_useful_cols:
        if col in df.columns:
            df = df.drop(col, axis=1)
    return df


def generate_differences_by_ip(
        df,
        no_dif_cols=['targetIP', 'incidences', 'tref_start', 'hour', 'wday']):
    """Given a dataframe it differentiates it by ips. The columns in
    'no_dif_cols' are not differentiated.
    """
    all_ips = set(df['targetIP'].values)
    rows = []
    for ip in all_ips:
        df_by_ip = df[df['targetIP'] == ip]
        df_by_ip.sort_values(by=['tref_start'])
        rows += dataframe_difference_by_rows(df_by_ip, no_dif_cols)
    return pd.DataFrame(rows)


def dataframe_difference_by_rows(
        df,
        no_dif_cols=['targetIP', 'incidences', 'tref_start', 'hour', 'wday']):
    """Generates a list of rows where each
    new row is the column-to-column difference of two rows
    consecutive 'df'. Columns in 'no_dif_cols' are kept
    the same, the difference is not applied to them.
    """
    rows = []
    prev = None
    no_dif_cols = set([col for col in no_dif_cols if col in set(df.columns)])
    dif_cols = [col for col in set(df.columns) if col not in no_dif_cols]
    for index, row in df.iterrows():
        if prev is not None:
            # For the columns that we do not differentiate
            dif_row = {col: row[col] for col in no_dif_cols}
            # For the columns that do so
            for col in dif_cols:
                dif_row[col] = row[col] - prev[col]
            # We save the row
            rows.append(dif_row)
        prev = row
    return rows


def existIncidence(df):
    """Returns True if there are any incidences in the dataframe
    """
    return len(df[df['incidences'] == True]) > 0
