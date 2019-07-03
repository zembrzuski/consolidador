import collections
from service.helpers import my_flatmap
from repository import elasticsearch_repository


def extract_conta(conta, plano_contas):
    flattened = my_flatmap(list(map(lambda x: x[conta], plano_contas)))

    conta_dict = dict()

    for i in flattened:
        conta_dict['{}-{}'.format(i['year'], i['trimester'])] = i['value']

    sorted_x = sorted(conta_dict.items(), key=lambda kv: kv[1])

    return collections.OrderedDict(sorted_x)


def importa_balancos(cvm):
    hits = elasticsearch_repository.find_by_codigo_cvm(cvm)
    plano_contas = list(map(lambda hit: hit['_source']['plano_contas'], hits))

    balanco = {
        'nome_empresa': hits[0]['_source']['nome_empresa'],
        'codigo_cvm': hits[0]['_source']['codigo_cvm'],
        'patrimonio_liquido': extract_conta('patrimonio_liquido', plano_contas)
    }

    return elasticsearch_repository.persist_balanco(balanco)
