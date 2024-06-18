import pandas as pd

ncvoter7 = 'C:/Users/Owner/PycharmProjects/Midterm_Code/Data/ncvoter7.txt'

beaufort_voter = 'C:/Users/Owner/PycharmProjects/Midterm_Code/Data/beaufort_voter.csv'

df = pd.read_csv(ncvoter7, delimiter='\t')

df = pd.read_csv(beaufort_voter, delimiter=',')

active_voters_df = df[df['voter_status_desc'] == 'ACTIVE']

filtered_pile_path = 'C:/Users/Owner/PycharmProjects/Midterm_Code/filtered_active_voters.csv'