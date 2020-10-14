#!/usr/bin/env python3

import pandas as pd

df = pd.read_csv("java2", delimiter="\t", header=None)
df.sort_values(by=1, ascending=False, inplace=True)
print(df.head(10))

print(df.loc[df[0] == "el"])
print(df.loc[df[0] == "dijo"])
