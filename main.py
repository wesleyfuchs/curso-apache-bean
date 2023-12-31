import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
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
        
def chave_uf_ano_mes_de_lista(elemento):
    """
    Recebe uma lista de elementos
    Retorna uma tupla contendo uma chave e o valor de chuva em mm
    ('UF-ANO-MES', 1.3)
    """
    data, mm, uf = elemento
    ano_mes = '-'.join(data.split('-')[:2])
    chave = f'{uf}-{ano_mes}'
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return (chave, mm)

def arredonda(elemento):
    """
    Recebe uma tupla 
    Retorna uma tupla com valor arredondado
    """
    chave, mm = elemento
    return (chave, round(mm, 1))

def filtra_campos_vazios(elemento):
    """
    Remove elementos que tenham chaves vazias
    Recebe uma tupla ('CE-2015-04', {'chuvas': [55.0], 'dengue': [680.0]}) 
    Retorna uma tupla ('CE-2015-04', {'chuvas': [55.0], 'dengue': [680.0]}) 
    """
    chave, dados = elemento
    if all([
        dados['chuvas'],
        dados['dengue']
    ]):
        return True
    else:
        return False
    
def descompactar_elementos(elemento):
    """ 
    Recebe uma tupla ('CE-2015-01', {'chuvas': [85.8], 'dengue': [175.0]})
    Retorna uma tupla ('CE', '2015', '01', '85.8', '175.0')
    """    
    chave, dados = elemento
    chuva = dados['chuvas'][0]
    dengue = dados['dengue'][0]
    uf, ano, mes = chave.split('-')
    return uf, ano, mes, str(chuva), str(dengue)

    
def preparar_csv(elemento, delimitador=';'):
    """
    Receber uma tupla ('CE', '2015', '01', '85.8', '175.0')
    Retorna uma string delimitada "CE;2015;01;85.8;175.0"
    """
    return f"{delimitador}".join(elemento)
    
    
dengue = (
    pipeline 
    | "Leitura do dataset de dengue" >> ReadFromText('data/sample_casos_dengue.txt', skip_header_lines=1)
    | "De texto para lista" >> beam.Map(texto_para_lista)
    | "De lista para dicionario" >> beam.Map(lista_para_dicionario, colunas_dengue)
    | "Criar campo ano-mes" >> beam.Map(trata_datas)
    | "Criar chave pelo estado" >> beam.Map(chave_uf)
    | "Agrupar pelo estado" >> beam.GroupByKey()
    | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue)
    | "Soma dos casos pela chave" >> beam.CombinePerKey(sum)
    # | "Mostrar resultados" >> beam.Map(print)
)

chuvas = (
    pipeline
    | "Leitura do dataset de chuvas" >> ReadFromText('data/sample_chuvas.csv', skip_header_lines=1)
    | "De texto para lista (chuvas)" >> beam.Map(texto_para_lista, delimitador=',')
    | "Criando chave uf-ano-mes" >> beam.Map(chave_uf_ano_mes_de_lista)
    | "Soma dos casos pela chave (chuvas)" >> beam.CombinePerKey(sum)
    | "Arredondar resultados" >> beam.Map(arredonda)
    # | "Mostrar resultados de chuvas" >> beam.Map(print)
)

resultado = (
    # (chuvas, dengue)
    # | "Empilha as pcollections" >> beam.Flatten()
    # | "Agrupa as pcolls" >> beam.GroupByKey()
    ({'chuvas' : chuvas,'dengue' : dengue})
    | 'Mesclar pcols' >> beam.CoGroupByKey()
    | "Filtrar dados vazios" >> beam.Filter(filtra_campos_vazios)
    | "Descompactar elementos" >> beam.Map(descompactar_elementos)
    | "Preparar csv" >> beam.Map(preparar_csv)
    # | "Mostrar resultado da união das pcollections" >> beam.Map(print)
)

header = "UF;ANO;MES;CHUVA;DENGUE"

resultado | "Criar arquivo csv" >> WriteToText('data/resultado', file_name_suffix='.csv', header=header)

pipeline.run()