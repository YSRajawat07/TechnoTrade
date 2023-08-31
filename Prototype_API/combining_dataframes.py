import pandas as pd

# Example dataframes
df1 = pd.DataFrame({'index': [1, 2, 3],
                    'indexSymbol': ['ABC', 'DEF', 'GHI'],
                    'price': [100, 200, 300],
                    'time': ['09:00', '10:00', '11:00']})

df2 = pd.DataFrame({'index': [1, 2, 3],
                    'open': [95, 195, 295],
                    'high': [105, 205, 305],
                    'price': [100, 200, 300]})

df3 = pd.DataFrame({'index': [1, 2, 3],
                    'low': [90, 190, 290],
                    'volume': [1000, 2000, 3000],
                    'price': [100, 200, 300]})


# Example list of dataframes
dataframes = [df1, df2, df3]  # Add more dataframes as needed

# Merge the dataframes iteratively
merged_df = dataframes[0]

for i in range(1, len(dataframes)):
    merged_df = merged_df.merge(dataframes[i], on='index')

# Select the desired columns
result_df = merged_df[['index', 'indexSymbol', 'price_x', 'open', 'high', 'time', 'low', 'volume']]
result_df.columns = ['index', 'indexSymbol', 'price', 'open', 'high', 'time', 'low', 'volume']

print(result_df)
