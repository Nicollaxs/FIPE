# main_pipeline.py
from fipe_utils import get_referencias_meses, get_modelos, get_anos, get_precos
import time
import os


def _parse_int(value, default):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_float(value, default):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _carregar_marcas_padrao():
    return [
        59,   # VW - VolksWagen
        21,   # Fiat
        23,   # GM - Chevrolet
        22,   # Ford
        56,   # Toyota
        26,   # Hyundai
        25,   # Honda
        29,   # Jeep
        48,   # Renault
        238,  # BYD (Eletricos)
        240,  # GWM (Eletricos/Hibridos)
        245,  # Caoa Chery
        44,   # Peugeot (Grupo Stellantis)
        7     # BMW (Premium/Luxo)
    ]


def _carregar_marcas_env():
    marcas_env = os.getenv('FIPE_MARCAS_IDS', '').strip()
    if not marcas_env:
        return _carregar_marcas_padrao()

    marcas = []
    for item in marcas_env.split(','):
        item = item.strip()
        if not item:
            continue
        try:
            marcas.append(int(item))
        except ValueError:
            print(f"⚠️ Marca invalida ignorada: {item}")

    return marcas or _carregar_marcas_padrao()

def executar_carga_teste():
    print("🚀 Iniciando Pipeline FIPE...")
    
    # 1. Pega os meses de referência
    meses = get_referencias_meses()
    if not meses:
        print("❌ Não foi possível obter as referências.")
        return

    meses_offset = max(0, _parse_int(os.getenv('FIPE_MESES_OFFSET', '0'), 0))
    meses_limit = max(1, _parse_int(os.getenv('FIPE_MESES_LIMIT', '12'), 12))
    modelos_limit = max(0, _parse_int(os.getenv('FIPE_MODELOS_LIMIT', '0'), 0))
    sleep_segundos = max(0.0, _parse_float(os.getenv('FIPE_SLEEP_SECONDS', '1.0'), 1.0))

    recorte_meses = meses[meses_offset:meses_offset + meses_limit]
    marcas_alvo = _carregar_marcas_env()

    print(f"⚙️ Config: meses_offset={meses_offset}, meses_limit={meses_limit}, modelos_limit={modelos_limit or 'todos'}, sleep={sleep_segundos}s")
    print(f"⚙️ Marcas alvo: {marcas_alvo}")

    total_precos = 0
    total_modelos = 0

    tipo = 'cars'
    
    for mes in recorte_meses:
        ref_cod = mes['code']
        ref_nome = mes['month']
        print(f"\n📅 Processando Referência: {ref_nome} (Código: {ref_cod})")
        
        for marca_id in marcas_alvo:
            print(f"  🏢 Buscando modelos da marca ID: {marca_id}")
            
            # 4. Busca os modelos da marca
            lista_modelos = get_modelos(tipo, marca_id, ref_cod)
            
            if lista_modelos:
                modelos_iteracao = lista_modelos[:modelos_limit] if modelos_limit > 0 else lista_modelos
                for modelo in modelos_iteracao:
                    mod_id = modelo['code']
                    mod_nome = modelo['name']
                    print(f"    🚗 Processando Modelo: {mod_nome}")
                    total_modelos += 1
                    
                    # 5. Busca os anos disponíveis para este modelo
                    lista_anos = get_anos(tipo, marca_id, mod_id, ref_cod)
                    
                    if lista_anos:
                        # Pegamos o preço apenas do ano mais recente do modelo
                        ano_id = lista_anos[0]['code']
                        
                        # 6. Busca o Preço Final (A Fato)
                        preco = get_precos(tipo, marca_id, mod_id, ano_id, ref_cod)
                        if preco is not None:
                            total_precos += 1
                        
                        # Pausa de 1 segundo para ser gentil com a API
                        time.sleep(sleep_segundos)
            else:
                print(f"    ⚠️ Nenhum modelo encontrado para marca {marca_id} na ref {ref_cod}")

    print(f"\n✅ Pipeline finalizado com sucesso! Modelos processados: {total_modelos} | Precos coletados: {total_precos}")

if __name__ == "__main__":
    executar_carga_teste()