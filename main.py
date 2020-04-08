import pandas as pd
import requests
import store

get_organizations_url = "https://api.github.com/organizations?since="

headers = {'Content-Type': 'application/json'}

def get_organizations(organization_id):
    organizations = pd.DataFrame(columns = ['id', 'login'])    

    # Extract the organizations JSON string to pandas object (since pagination).
    resp = requests.get(get_organizations_url + str(organization_id), headers = headers)

    if resp.status_code == 403:
        print('API rate limit exceeded')
        pass
    elif resp.status_code == 200:
        # Read data
        organizations=pd.read_json(resp.content)
    else:
        print(resp.content)
        pass

    # Transform data
    organizations['id'] = pd.to_numeric(organizations['id'])

    return organizations
            
if __name__== '__main__':
    # Get organizations
    organizations = get_organizations(store.get_last_organization_id())

    # Store organizations
    store.save_data(organizations[['id','login']], 'organizations')


    


    
