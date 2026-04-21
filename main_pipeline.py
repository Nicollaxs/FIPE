# main_pipeline.py
from fipe_utils import get_referencias_meses, get_marcas, get_modelos, get_anos, get_precos
import time

def executar_carga_teste():
    print("🚀 Iniciando Pipeline FIPE...")
    
    # 1. Pega os meses de referência
    meses = get_referencias_meses()
    if not meses:
        print("❌ Não foi possível obter as referências.")
        return

    # 2. Recorte: Vamos pegar apenas os 2 meses mais recentes para o primeiro teste
    # Depois que funcionar, você pode mudar para meses[:12]
    recorte_meses = meses[:2]
    
    # 3. Marcas Alvo: 59 (VW) e 21 (Fiat)
    marcas_alvo = [59, 21] 
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
                # LIMITADOR: Pegando apenas os 3 primeiros modelos para teste
                for modelo in lista_modelos[:3]:
                    mod_id = modelo['code']
                    mod_nome = modelo['name']
                    print(f"    🚗 Processando Modelo: {mod_nome}")
                    
                    # 5. Busca os anos disponíveis para este modelo
                    lista_anos = get_anos(tipo, marca_id, mod_id, ref_cod)
                    
                    if lista_anos:
                        # Pegamos o preço apenas do ano mais recente do modelo
                        ano_id = lista_anos[0]['code']
                        
                        # 6. Busca o Preço Final (A Fato)
                        get_precos(tipo, marca_id, mod_id, ano_id, ref_cod)
                        
                        # Pausa de 1 segundo para ser gentil com a API
                        time.sleep(1)
            else:
                print(f"    ⚠️ Nenhum modelo encontrado para marca {marca_id} na ref {ref_cod}")

    print("\n✅ Pipeline finalizado com sucesso!")

if __name__ == "__main__":
    executar_carga_teste()