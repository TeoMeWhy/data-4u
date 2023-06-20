import json
import pandas as pd

with open('data4u_igdb.json', 'r') as open_file:
    data = json.load(open_file)

pd.DataFrame(data['tasks'])