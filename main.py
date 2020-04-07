import pandas as pd
import requests

get_organizations_url = "https://api.github.com/organizations?since="

headers = {'Content-Type': 'application/json'}

def get_organizations():
    id = 0

    # NOTE: When acting as airflow operator this for should be removed...
    for n in range(3):

        # Extract the organizations JSON string to pandas object.
        resp = requests.get(get_organizations_url + str(id), headers = headers)

        # NOTE: https://github.com/joeyespo/grip/issues/48
        # 60 requests/hour when using API without authentication
        if resp.status_code == 403:
            print('API rate limit exceeded')
            break
        elif resp.status_code == 200:
            organizations=pd.read_json(resp.content)
            print(organizations[['id','repos_url']])
            # Get last result to be used on pagination.
            id = int(organizations['id'].tail(1))
        else:
            print(resp.content)
            break
            
if __name__== '__main__':
    get_organizations()

    


    
