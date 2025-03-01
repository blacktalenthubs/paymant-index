# from pyspark.pandas import spark
# #
# # df = spark.read.csv("transactions.csv", header=True, inferSchema=True)
# # df.show()
# # df_filtered = df.filter(df.transaction_amount >= 50)
# # df_filtered.show()
# #

import pandas as pd
df = pd.read_csv("transactions.csv")
df.head()
df_filtered = df[df["transaction_amount"] >= 50]
print(df_filtered.head())