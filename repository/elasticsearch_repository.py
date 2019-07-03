import requests
from config.application import config


def find_by_codigo_cvm(cvm):
    query = {
        "query": {
            "term": {
                "codigo_cvm": {
                    "value": cvm
                }
            }
        }
    }

    hits = requests.post('{}/teste/_search'.format(config['elasticsearch']), json=query).json()['hits']['hits']

    return hits


def persist_balanco(balanco):
    url = '{}/teste2/teste2/{}'.format(config['elasticsearch'], balanco['codigo_cvm'])
    return requests.post(url, json=balanco).status_code
