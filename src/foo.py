import pandas as pd

df1 = pd.DataFrame(columns=['a', 'b'])
df1.loc[len(df1)] = [1, 2]
df1.loc[len(df1)] = [3, 4]
df1.loc[len(df1)] = [5, 6]

df2 = pd.DataFrame(columns=['a', 'b'])
df2.loc[len(df2)] = [1, 2]

df3 = pd.concat([df1, df2]).drop_duplicates(keep=False)
print(df1)
print()
print(df2)
print()
print(df3)
