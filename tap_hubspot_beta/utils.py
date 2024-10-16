import json
import requests


def deep_merge(dict1, dict2):
    for key, value in dict2.items():
        if isinstance(value, dict) and key in dict1:
            dict1[key] = deep_merge(dict1[key], value)
        else:
            dict1[key] = value
    return dict1

def merge_responses(responses):
    merged_data = {}
    for response in responses:
        data = response.json()
        item_id = data['id']
        if item_id not in merged_data:
            merged_data[item_id] = data
        else:
            merged_data[item_id] = deep_merge(merged_data[item_id], data)
    merged_response = requests.Response()
    # this function is called inside the request for contacts_history_properties, so it will be only one id. 
    # the return is just one record/response, that's why the index 0.
    # and it needs to be inside list() to convert .values() onto a jsonable object
    merged_response._content = json.dumps(list(merged_data.values())[0]).encode('utf-8')
    merged_response.status_code = responses[0].status_code
    merged_response.headers = responses[0].headers
    return merged_response