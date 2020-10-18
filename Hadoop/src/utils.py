#!/usr/bin/env python3

import pandas as pd

df = pd.read_csv("wc_modif", delimiter="\t", header=None)
df.sort_values(by=1, ascending=False, inplace=True)

print("Las 10 palabras más frecuentes son:")
print(df.head(10))
print("Número de ocurrencias de la palabra 'el':")
print(df.loc[df[0] == "el"])
print("Número de ocurrencias de la palabra 'dijo':")
print(df.loc[df[0] == "dijo"])
