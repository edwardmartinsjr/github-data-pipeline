import pandas as pd
import requests
import store

get_organizations_url = 'https://api.github.com/organizations?since='
get_repos_url = 'https://api.github.com/orgs/%(login)s/repos'

headers = {'Content-Type': 'application/json'}

def get_organizations(organization_id):
    organizations_columns = ['id', 'login']
    organizations = pd.DataFrame(columns = organizations_columns)    

    # Extract the organizations JSON string to pandas object (since pagination).
    resp = requests.get(get_organizations_url + str(organization_id), headers = headers)

    if resp.status_code == 403:
        print('API rate limit exceeded')
        pass
    elif resp.status_code == 200:
        # Read data
        organizations=pd.read_json(resp.content)[organizations_columns]
    else:
        print(resp.content)
        pass

    # Transform data
    organizations['id'] = pd.to_numeric(organizations['id'])
    organizations['login'] = organizations['login'].astype('str')

    return organizations

def get_repos(organizations):
    repos_columns = ['git_url', 'language', 'archived', 'forks_count', 'open_issues_count', 'watchers_count']
    repos = pd.DataFrame(columns = repos_columns)    

    # Extract the organizations repos JSON string to pandas object.
    for row in organizations.itertuples():
        resp = requests.get(get_repos_url % {'login': row.login}, headers = headers)
        
        if resp.status_code == 403:
            print('API rate limit exceeded')
            break
        elif resp.status_code == 200:
            # Read data
            if len(resp.json()) > 0:
                repos = repos.append(pd.read_json(resp.content)[repos_columns])
        else:
            print(resp.content)
            break

    # Transform data
    repos['git_url'] = repos['git_url'].astype('str')
    repos['language'] = repos['language'].astype('str')
    repos['forks_count'] = pd.to_numeric(repos['forks_count'])
    repos['open_issues_count'] = pd.to_numeric(repos['open_issues_count'])
    repos['watchers_count'] = pd.to_numeric(repos['watchers_count'])
    repos['archived'] = repos['archived'].astype('bool')

    return repos

            
if __name__== '__main__':
    # Get organizations
    organizations = get_organizations(store.get_last_organization_id())

    # Store organizations
    store.save_data(organizations, 'organizations')

    # Get repos
    repos = get_repos(organizations)

    # Store repos
    store.save_data(repos, 'repos')


    


    
