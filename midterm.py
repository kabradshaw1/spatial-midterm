import pandas as pd

file_path = 'C:/Users/Owner/PycharmProjects/Midterm_Code/Data/ncvoter7.txt'

df = pd.read_csv(file_path, delimiter='\t')

df = pd.read_

active_voters_df = df[df['voter_status_desc'] == 'ACTIVE']

filtered_pile_path = 'C:/Users/Owner/PycharmProjects/Midterm_Code/filtered_active_voters.csv'