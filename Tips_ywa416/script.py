import matplotlib.pyplot as plt
import pandas as pd
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import time

from os import listdir

input = sys.argv[1]
output = sys.argv[2]

# join location analysis output with taxi zone look up
taxi_zone = "Tips_ywa416/results/taxi_zone.csv"

# filepath = ""
# for f in listdir(input):
#     if f.startswith("part"):
#         filepath = "%s/%s"%(input, f)
#         break 
filepath = input
df = pd.read_csv(filepath)
lookup_zone = pd.read_csv(taxi_zone)
joined = df.join(lookup_zone, lsuffix='locationID', rsuffix='LocationID')
joined = joined.sort_values(by='avg', ascending=False)
joined.to_csv('%s/full.csv'%output, index=False)
joined = joined[joined["count"]> 10000]
joined.head(20).to_csv('%s/top20.csv'%output, index=False)
joined.tail(20).sort_values(by='avg').to_csv('%s/tail20.csv'%output, index=False)