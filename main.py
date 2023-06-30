import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

def texto_para_lista(elemento, delimitador='|'):
    """
    Recebe um texto e um delimitador 
    Retorna uma lista de elementos pelo delimitador
    """
    return elemento.split(delimitador)

colunas_dengue = ['id', 'data_iniSE', 'casos', 'ibge_code', 'cidade', 'uf', 'cep', 'latitude', 'longitude']

def lista_para_dicionario(elemento, colunas):
    """
    Recebe duas listas e retorna um dicionario
    """
    return dict(zip(colunas, elemento))

def trata_datas(elemento):
    """
    Recebe um dicionario e cria um novo campo com ano-mes
    Retorna o mesmo dicionario com novo campo
    """
    elemento['ano-mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2])
    return elemento

def chave_uf(elemento):
    """
    Recebe um dicionario
    Retorna uma tupla com o estado(UF) e o elemento (UF, dicionario)
    """
    chave = elemento['uf']
    return (chave, elemento)

def casos_dengue(elemento):
    """
    Recebe uma tupla ('RS', [{}, {}])
    Retorna uma tupla contendo ('RS-2014-12', 8.0)
    """
    uf, registros = elemento
    for registro in registros:
        # print(registro['casos'])
        if bool(re.search(r'\d', registro['casos'])):
            yield (f"{uf}-{registro['ano-mes']}", float(registro['casos']))
        else:
            yield (f"{uf}-{registro['ano-mes']}", 0.0)
        


# dengue = (
#     pipeline 
#     | "Leitura do dataset de dengue" >> ReadFromText('casos_dengue.txt', skip_header_lines=1)
#     | "De texto para lista" >> beam.Map(texto_para_lista)
#     | "De lista para dicionario" >> beam.Map(lista_para_dicionario, colunas_dengue)
#     | "Criar campo ano-mes" >> beam.Map(trata_datas)
#     | "Criar chave pelo estado" >> beam.Map(chave_uf)
#     | "Agrupar pelo estado" >> beam.GroupByKey()
#     | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue)
#     | "Soma dos casos pela chave" >> beam.CombinePerKey(sum)
#     # | "Mostrar resultados" >> beam.Map(print)
# )

chuvas = (
    pipeline
    | "Leitura do dataset de chuvas" >> ReadFromText('chuvas.csv', skip_header_lines=1)
    | "Mostrar resultados" >> beam.Map(print)
)

pipeline.run()