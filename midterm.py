import pandas as pd

ncvoter7 = '/Users/kylebradshaw/repos/midterm/Data/ncvoter7.txt'

beaufort_voter = '/Users/kylebradshaw/repos/midterm/Data/beaufort_voter.csv'

df = pd.read_csv(ncvoter7, delimiter='\t')

df = pd.read_csv(beaufort_voter, delimiter=',')

active_voters_df = df[df['voter_status_desc'] == 'ACTIVE']

filtered_file_path = '/Users/kylebradshaw/repos/midterm/filtered_active_voters.csv'
active_voters_df.to_csv(filtered_file_path, index=False)