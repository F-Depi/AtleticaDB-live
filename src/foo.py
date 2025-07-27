import pandas as pd

df = pd.DataFrame(columns=('a', 'b'))
df.loc[0, 'a'] = 3
df.loc[1, 'a'] = 4
df.loc[1, 'b'] = 5
print(df)

for ii, row in df.iterrows():
    print(f"Row {ii}:")
    print(row)
    row['a'] = 'sex'
    print(row)
