# import numpy as np
#
# edges = pd.read_csv("wiki-topcats.txt",sep=" ",names=["src","dst"])
#
# min_edges = 1500000
# min_vertices = 150000
#
# while True:
#     newdf = edges.sample(min_edges)
#     n_vertices = len(np.unique(newdf))
#     if n_vertices >= min_vertices:
#         break
#
# newdf.to_csv("wiki-topcats.csv",header = None,sep = ",",index=False)

import numpy as np
import pandas as pd

edges = pd.read_csv("wiki-topcats.txt", sep=" ", names=["src", "dst"])

min_edges = 1500000
min_vertices = 150000

samples = pd.Series(range(len(edges))).sample(min_edges).values
newdf = edges.iloc[samples]


newdf.to_csv("new_data.csv", header=None, sep=" ", index=False)
