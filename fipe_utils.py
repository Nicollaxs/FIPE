import requests as req
import os
from datetime import datetime
import json
import time
import random

def _fazer_requisicao(url, params=None, timeout=20, max_tentativas=5):
    """Faz requisição com tolerância a falhas transitórias (429, 5xx e timeout)."""
    tentativa = 1

    while tentativa <= max_tentativas:
        try:
            response = req.get(url, params=params, timeout=timeout)

            if response.status_code == 429:
                espera = min(60, 2 ** tentativa) + random.uniform(0, 1)
                print(f'Limite atingido (429). Tentativa {tentativa}/{max_tentativas}. Dormindo {espera:.1f}s...')
                time.sleep(espera)
                tentativa += 1
                continue

            if 500 <= response.status_code <= 599:
                espera = min(30, 2 ** tentativa) + random.uniform(0, 1)
                print(f'Erro servidor ({response.status_code}). Tentativa {tentativa}/{max_tentativas}. Dormindo {espera:.1f}s...')
                time.sleep(espera)
                tentativa += 1
                continue

            return response

        except req.RequestException as exc:
            espera = min(30, 2 ** tentativa) + random.uniform(0, 1)
            print(f'Erro de rede: {exc}. Tentativa {tentativa}/{max_tentativas}. Dormindo {espera:.1f}s...')
            time.sleep(espera)
            tentativa += 1

    print('Falha após múltiplas tentativas.')
    return None

def _montar_caminho_datalake(categoria, nome_arquivo):
    hoje = datetime.now().strftime('%Y-%m-%d')
    diretorio = os.path.join('data', 'bronze', 'fipe', categoria, f'load_data={hoje}')
    caminho_final = os.path.join(diretorio, f'{nome_arquivo}.json')
    return diretorio, caminho_final

def _carregar_arquivo_local(categoria, nome_arquivo):
    _, caminho_final = _montar_caminho_datalake(categoria, nome_arquivo)

    if os.path.exists(caminho_final):
        with open(caminho_final, 'r', encoding='utf-8') as f:
            print(f"Arquivo já existe, carregando local: {caminho_final}")
            return json.load(f)

    return None

def salvar_no_datalake(conteudo, categoria, nome_arquivo):
    diretorio, caminho_final = _montar_caminho_datalake(categoria, nome_arquivo)

    os.makedirs(diretorio, exist_ok = True)

    with open(caminho_final, 'w', encoding = 'utf-8') as f:
        json.dump(conteudo, f, ensure_ascii = False, indent = 4)

    print(f"Arquivo salvo com sucesso em {caminho_final}")

def get_referencias_meses():
    nome_arq = 'meses_referencias'
    dados_locais = _carregar_arquivo_local('referencias', nome_arq)
    if dados_locais is not None:
        return dados_locais

    url = 'https://fipe.parallelum.com.br/api/v2/references'
    response = _fazer_requisicao(url, timeout=20)

    if response and response.ok :
        dados = response.json()
        salvar_no_datalake(dados, 'referencias', nome_arq)
        return dados
    else :
        status = response.status_code if response is not None else 'sem resposta'
        print(f'Erro na requisição : {status}')
        return None

def get_marcas(tipo_veiculo, codigo_referencia=None):    
    """
     São apenas 3 valores fixos (cars, motorcycles, trucks).
     Na engenharia de dados, chamamos isso de Tabelas de Domínio.
    """
    sufixo_referencia = f'_ref_{codigo_referencia}' if codigo_referencia is not None else ''
    nome_arq = f'marcas_{tipo_veiculo}{sufixo_referencia}'
    dados_locais = _carregar_arquivo_local('marcas', nome_arq)
    if dados_locais is not None:
        return dados_locais

    url = f'https://fipe.parallelum.com.br/api/v2/{tipo_veiculo}/brands'
    params = {'reference': codigo_referencia} if codigo_referencia is not None else None

    response = _fazer_requisicao(url, params=params, timeout=20)

    if response and response.ok :
        dados = response.json()
        salvar_no_datalake(dados,'marcas',nome_arq)
        return dados
    else :
        status = response.status_code if response is not None else 'sem resposta'
        print(f'Erro na requisição : {status}')
        return None
    

def get_modelos(tipo_veiculo, id_marca, codigo_referencia):
    nome_arq = f'modelos_{tipo_veiculo}_marca_id={id_marca}_ref_{codigo_referencia}'
    dados_locais = _carregar_arquivo_local('modelos', nome_arq)
    if dados_locais is not None:
        return dados_locais

    url = f'https://fipe.parallelum.com.br/api/v2/{tipo_veiculo}/brands/{id_marca}/models'
    params = {'reference': codigo_referencia }

    response = _fazer_requisicao(url, params=params, timeout=20)

    if response and response.ok :
        dados = response.json()
        salvar_no_datalake(dados, 'modelos', nome_arq)
        return dados
    else :
        status = response.status_code if response is not None else 'sem resposta'
        print(f'Erro na requisição : {status}')
        return None

def get_anos(tipo_veiculo, id_marca, id_modelo, codigo_referencia):
    nome_arq = f'anos_{tipo_veiculo}_id_marca_{id_marca}_id_modelo_{id_modelo}_ref_{codigo_referencia}'
    dados_locais = _carregar_arquivo_local('anos', nome_arq)
    if dados_locais is not None:
        return dados_locais

    url = f'https://fipe.parallelum.com.br/api/v2/{tipo_veiculo}/brands/{id_marca}/models/{id_modelo}/years'
    params = {'reference': codigo_referencia}

    response = _fazer_requisicao(url, params=params, timeout=20)

    if response and response.ok:
        dados = response.json()
        salvar_no_datalake(dados, 'anos', nome_arq)
        return dados
    else:
        status = response.status_code if response is not None else 'sem resposta'
        print(f'Erro na requisição anos: {status}')
        return None

def get_precos(tipo_veiculo, id_marca, id_modelo, id_ano, codigo_referencia):
    nome_arq = f'precos_{tipo_veiculo}_id_marca={id_marca}_id_modelo={id_modelo}_id_ano={id_ano}_ref_{codigo_referencia}'
    dados_locais = _carregar_arquivo_local('precos', nome_arq)
    if dados_locais is not None:
        return dados_locais
    
    url = f'https://fipe.parallelum.com.br/api/v2/{tipo_veiculo}/brands/{id_marca}/models/{id_modelo}/years/{id_ano}'
    params = {'reference' : codigo_referencia }

    response = _fazer_requisicao(url, params=params, timeout=20)

    if response and response.ok :
        dados  = response.json()
        salvar_no_datalake(dados, 'precos',nome_arq)
        return dados
    else :
        status = response.status_code if response is not None else 'sem resposta'
        print(f'Erro na requisição : {status}')
        return None
