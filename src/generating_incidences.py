import pandas as pd
"""Example of how the incident generator works
""""
from modules.util import get_working_incidence
from modules.incidences import generar_incidencias

# We load the metrics from a single server
df_file = 'data/oneServerMetrics.csv'
df = pd.read_csv(df_file)
# We load the incident
inc_file = 'data/incidences/indicendes_examples/down_bps_rcv_e.json'
inc = get_working_incidence(inc_file)
# We generate the dataframe with the incidencea
df_mod = generar_incidencias(df, inc)
df_mod.to_csv('df_with_incidences.csv')
"""
Two files are required:

- df_file: It is the csv file where
           it is saved network metrics
           from a specific server.
           Metrics must be
           collect added by
           server and each row
           will represent an interval
           of time.

- inc_file: It is the JSON file where
            incidence is defined.
            The incident consists of
            various "modifications"
            that can be overlapped.
            Modifications
            affect a metric
            server (one of the
            csv columns), with
            an intensity (factor
            multiplicative) and proportion
            fixed. See example.



NOTE:

The csv with the network metrics must have
a column that identifies the time instant
in which they are collected. In the example case that
this column is displayed is 'tref_start' and saves
the epochs in milliseconds.

If the csv that is used saves the time in a
different format, it will be necessary to change the function
* def time_obj (row) * of the file ./modules/incidencias.
This function receives a row from the csv and returns the
struct_time that responds to that record.
"""
