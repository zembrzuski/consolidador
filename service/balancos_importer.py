import collections
from service.helpers import my_flatmap
from repository import elasticsearch_repository


def extract_balanco(conta, plano_contas):
    flattened = my_flatmap(list(map(lambda x: x[conta], plano_contas)))

    conta_dict = dict()

    for i in flattened:
        conta_dict['{}-{}'.format(i['year'], i['trimester'])] = i['value']

    sorted_x = sorted(conta_dict.items(), key=lambda kv: kv[0])

    return collections.OrderedDict(sorted_x)


def calcula_ultimo_trimestre_ano(trimestre_final_do_ano, trimestre_intermediario_ano):
    outros_trimestres_do_ano = list(filter(lambda x: x['year'] == trimestre_final_do_ano['year'], trimestre_intermediario_ano))

    if len(outros_trimestres_do_ano) != 3:
        raise Exception('nao consigo ajudar o ultimo trimestre do ano')

    soma_dos_outros_trimestres_do_ano = sum(list(map(lambda x: x['value'], outros_trimestres_do_ano)))
    ultimo_trimestre_ajustado = trimestre_final_do_ano['value'] - soma_dos_outros_trimestres_do_ano

    return {
        'trimester': trimestre_final_do_ano['trimester'],
        'year': trimestre_final_do_ano['year'],
        'value': ultimo_trimestre_ajustado
    }


def extract_demonstrativo(conta, plano_contas):
    flattened = my_flatmap(list(map(lambda x: x[conta], plano_contas)))

    trimestre_intermediario_ano = list(filter(lambda x: x['trimester'] in [1, 2, 3], flattened))
    trimestre_final_do_ano = list(filter(lambda x: x['trimester'] == 4, flattened))

    trimestre_final_ano_ajustado = list(map(
        lambda x: calcula_ultimo_trimestre_ano(x, trimestre_intermediario_ano), trimestre_final_do_ano))

    todos_trimestres_ajustados = trimestre_intermediario_ano + trimestre_final_ano_ajustado

    conta_dict = dict()

    for i in todos_trimestres_ajustados:
        conta_dict['{}-{}'.format(i['year'], i['trimester'])] = i['value']

    sorted_x = sorted(conta_dict.items(), key=lambda kv: kv[0])

    return collections.OrderedDict(sorted_x)


def importa_balancos(cvm):
    hits = elasticsearch_repository.find_by_codigo_cvm(cvm)
    plano_contas = list(map(lambda hit: hit['_source']['plano_contas'], hits))

    balanco = {
        'nome_empresa': hits[0]['_source']['nome_empresa'],
        'codigo_cvm': hits[0]['_source']['codigo_cvm'],
        'patrimonio_liquido': extract_balanco('patrimonio_liquido', plano_contas),
        'lucro_liquido': extract_demonstrativo('lucro_liquido', plano_contas)
    }

    return elasticsearch_repository.persist_balanco(balanco)
