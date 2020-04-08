import pandas as pd
import requests
import store

get_organizations_url = "https://api.github.com/organizations?since="

headers = {'Content-Type': 'application/json'}

def get_organizations(organization_id):

    # Extract the organizations JSON string to pandas object (since pagination).
    resp = requests.get(get_organizations_url + str(organization_id), headers = headers)

    if resp.status_code == 403:
        print('API rate limit exceeded')
    elif resp.status_code == 200:
        # Read data
        organizations=pd.read_json(resp.content)

        # Prepare data
        organizations['id'] = pd.to_numeric(organizations['id'])

        # Store data
        store.save_data(organizations[['id','login']], 'organizations')
    else:
        print(resp.content)
            
if __name__== '__main__':
    # Run steps
    get_organizations(store.get_last_organization_id())

    


    
