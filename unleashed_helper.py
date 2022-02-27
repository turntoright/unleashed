from Unleashed import Unleashed
import pandas as pd

def get_unleashed_service():
    api_id = '5abcd551-d6d6-4280-8c56-9e6add21a3ce'
    api_key = 'ij4DDwGQFGPtsys2pAeeQnLlkyh8ZnbdY9yFWyQsxUl7kVewjYJYKujLd2eVNTgdHBueF5Ruqj7CgSmuSJQ=='
    service = Unleashed.Client(api_key, api_id)
    return service

def get_unleashed_endpoit(endpoint, options=None, page=None, service=None):
    if service is None:
        service = get_unleashed_service()
    return service.request_endpoint(endpoint=endpoint, options=options, page=page)

def get_unleashed_pagination(endpoint, options=None, page=None, service=None):
    if service is None:
        service = get_unleashed_service()
    return get_unleashed_endpoit(endpoint, options=options, page=page, service=service)['Pagination']

def get_unleashed_items(endpoint, options=None, page=None, service=None):
    if service is None:
        service = get_unleashed_service()
    return get_unleashed_endpoit(endpoint, options=options, page=page, service=service)['Items']

# If the number of pages > 1, return all items
def get_unleashed_all_items(endpoint, options=None, service=None):
    if service is None:
        service = get_unleashed_service()
    pagination = get_unleashed_pagination(endpoint, options=options, service=service)
    number_of_pages, page_number = 0, 0
    if pagination is not None and 'NumberOfPages' in pagination:
        number_of_pages = pagination['NumberOfPages']
        page_number = pagination['PageNumber']
    items = []
    while page_number <= number_of_pages:
        items += get_unleashed_items(endpoint, options=options, page=page_number, service=service)
        page_number += 1
    return items

# Parse array element to another dataset, expand dict elements
# Suitable for data cascading level is not more than 2
def parse_data(data, endpoint):
    res = {endpoint: []}
    for item in data:
        row = {}
        for key in item:
            if isinstance(item[key], list):
                if len(item[key]) > 0:
                    for subItem in item[key]:
                        subItem['Parent_Guid'] = item['Guid']   # linked with a parent Guid
                        if key not in res:
                            res[key] = []
                        res[key].append(subItem)
            else:
                row[key] = item[key]
        res[endpoint].append(row)
    for key in res:
        res[key] = pd.json_normalize(res[key])
        print(key)
    return res

def extract_endpoint_to_csv(endpoint):
    data = get_unleashed_all_items(endpoint)
    data = parse_data(data, endpoint)
    for key in data:
        df = pd.DataFrame(data[key])
        df.to_csv(key + ".csv", index=None)