import pandas as pd

df1 = pd.DataFrame({
    'ID': [1, 2, 3, 5, 9],
    'Col_1': [1, 2, 3, 4, 5],
    'Col_2': [6, 7, 8, 9, 10],
    'Col_3': [11, 12, 13, 14, 15],
    'Col_4': ['apple', 'orange', 'banana', 'strawberry', 'raspberry']
})

df2 = pd.DataFrame({
    'ID': [1, 1, 3, 5],
    'Col_A': [8, 9, 10, 11],
    'Col_B': [12, 13, 15, 17],
    'Col_4': ['apple', 'orange', 'banana', 'kiwi']
    # 'Col_4': [1,2,3,4]
})
# inner join with common colum values nas rows
inner = pd.merge(df1, df2)

#  Join based on On like SQL {On to many Join}

on_sql = pd.merge(df1, df2, on='ID')

#  Join based On multiple columns like SQL {On to many Join}
on_sql_multiple = pd.merge(df1, df2, on=['ID', 'Col_4'])

#  Join based On different column name like SQ based on left & right  data frame L {On to many Join}
different_column__name_on_sql_multiple = pd.merge(df1, df2, suffixes=['_l', '_r'], left_on='Col_2', right_on="Col_A")

#  Join based On INDEX SQ based on left & right  data frame  {On to many Join}
join_on_index = pd.merge(df1, df2, suffixes=['_l', '_r'], left_index=True, right_index=True)

# Join Type inner, outer , left, right

# outer join
outerjoin = pd.merge(df1, df2, on='Col_4', how='outer', suffixes=['_l', '_r'], )

# left join
leftjoin = pd.merge(df1, df2, on='Col_4', how='left', suffixes=['_l', '_r'], )

# right join
rightjoin = pd.merge(df1, df2, on='Col_4', how='right', suffixes=['_l', '_r'], )

print(df1)
print("################################")
print(df2)
print("################################")
print(rightjoin)
