import requests as req
import os
from datetime import datetime
import json

def salvar_no_datalake(conteudo, categoria, nome_arquivo):
    hoje = datetime.now().strftime('%Y-%m-%d')

    diretorio = os.path.join('data','bronze','fipe',categoria,f'load_data={hoje}')

    os.makedirs(diretorio, exist_ok = True)

    caminho_final = os.path.join(diretorio, f'{nome_arquivo}.json')

    with open(caminho_final, 'w', encoding = 'utf-8') as f:
        json.dump(conteudo, f, ensure_ascii = False, indent = 4)

    print(f"Arquivo salvo com sucesso em {caminho_final}")

def get_referencias_meses():
    url = 'https://fipe.parallelum.com.br/api/v2/references'
    response = req.get(url, timeout = 20)

    if response.ok :
        dados = response.json()
        salvar_no_datalake(dados,'referencias','meses_referencias')
        return dados
    else :
        print(f'Erro na requisição : {response.status_code}')
        return None

def get_marcas(tipo_veiculo, codigo_referencia):    
    """
     São apenas 3 valores fixos (cars, motorcycles, trucks).
     Na engenharia de dados, chamamos isso de Tabelas de Domínio.
    """
    url = f'https://fipe.parallelum.com.br/api/v2/{tipo_veiculo}/brands'
    params = {'reference': codigo_referencia}

    response = req.get(url, params = params, timeout = 20)

    if response.ok :
        dados = response.json()
        nome_arq = f'marcas_{tipo_veiculo}_ref_{codigo_referencia}'
        salvar_no_datalake(dados,'marcas',nome_arq)
        return dados
    else :
        print(f'Erro na requisição : {response.status.code}')
        return None

def get_modelos(tipo_veiculo, id_marca, codigo_referencia):

    url = f'https://fipe.parallelum.com.br/api/v2/{tipo_veiculo}/brands/{id_marca}/models'
    params = {'reference': codigo_referencia }

    response = req.get(url, params = params, timeout = 20)

    if response.ok :
        dados = response.json()
        nome_arq = f'modelos_{tipo_veiculo}_marca_id={id_marca}_ref_{codigo_referencia}'
        salvar_no_datalake(dados, 'modelos', nome_arq)
        return dados
    else :
        print(f'Erro na requisição : {response.status.code}')
        return None

def get_anos(tipo_veiculo, id_marca, id_modelo, codigo_referencia):
    url = f'https://fipe.parallelum.com.br/api/v2/{tipo_veiculo}/brands/{id_marca}/models/{id_modelo}/years'
    params = {'reference': codigo_referencia}

    response = req.get(url, params=params, timeout=20)

    if response.ok:
        dados = response.json()
        nome_arq = f'anos_{tipo_veiculo}_id_marca_{id_marca}_id_modelo_{id_modelo}_ref_{codigo_referencia}'
        salvar_no_datalake(dados, 'anos', nome_arq)
        return dados
    else:
        print(f'Erro na requisição anos: {response.status_code}')
        return None

def get_precos(tipo_veiculo, id_marca, id_modelo, id_ano, codigo_referencia):
    
    url = f'https://fipe.parallelum.com.br/api/v2/{tipo_veiculo}/brands/{id_marca}/models/{id_modelo}/years/{id_ano}'
    params = {'reference' : codigo_referencia }

    response = req.get(url, params = params, timeout = 20)

    if response.ok :
        dados  = response.json()
        nome_arq = f'precos_{tipo_veiculo}_id_marca={id_marca}_id_modelo={id_modelo}_id_ano={id_ano}_ref_{codigo_referencia}'
        salvar_no_datalake(dados, 'precos',nome_arq)
        return dados
    else :
        print(f'Erro na requisição : {response.status_code}')
        return None


# 1. Referência
meses = get_referencias_meses()
if meses:
    ref = meses[0]['code'] # 332
    
    # 2. Marcas (opcional se você já sabe que VW é 59)
    get_marcas('cars', ref)
    
    # 3. Modelos da VW (59)
    get_modelos('cars', 59, ref)
    
    # 4. Anos da Amarok (ID 5940 na VW)
    # Aqui buscamos os anos disponíveis para esse modelo específico
    lista_anos = get_anos('cars', 59, 5940, ref)
    
    if lista_anos:
        # 5. Preço de um ano específico (ex: o primeiro da lista ou '2014-3')
        primeiro_ano_id = lista_anos[0]['code']
        get_precos('cars', 59, 5940, primeiro_ano_id, ref)
    



# get_tipo -> isso aqui na verdade tem que ser feito na mão (JSON)
# get_marca
# get_modelo

# Arrumar a questão da .env (entender)
# estrutura de pastas
# ver os imports necessarios