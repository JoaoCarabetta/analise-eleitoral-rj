#!/usr/bin/env python3
"""
Script para migrar locais de votação de 2024
"""

import os
from migrate import DataMigrator
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configurações
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")
BIGQUERY_PROJECT_ID = "rj-crm-registry"

def main():
    """Migrar apenas locais de votação para 2024"""
    migrator = DataMigrator(SUPABASE_URL, SUPABASE_KEY, BIGQUERY_PROJECT_ID)
    
    print("=== Migrando locais de votação para 2024 ===")
    
    # Buscar locais de votação de 2024 especificamente
    query = f"""
    SELECT DISTINCT
        ano,
        id_municipio,
        zona,
        AVG(ST_Y(melhor_urbano)) as latitude,
        AVG(ST_X(melhor_urbano)) as longitude
    FROM `basedosdados.br_tse_eleicoes.local_secao`
    WHERE sigla_uf = 'RJ'
        AND ano = 2024
        AND melhor_urbano IS NOT NULL
    GROUP BY ano, id_municipio, zona
    ORDER BY ano, id_municipio, zona
    """
    
    try:
        df = migrator.execute_bigquery(query)
        print(f"Encontrados {len(df)} locais de votação para 2024")
        
        if len(df) > 0:
            locais_data = df.to_dict('records')
            migrator.insert_batch_supabase('locais_votacao', locais_data, batch_size=50)
            print(f"✅ {len(locais_data)} locais de votação de 2024 migrados")
        else:
            print("❌ Nenhum local de votação encontrado para 2024")
            
    except Exception as e:
        print(f"❌ Erro: {e}")

if __name__ == "__main__":
    main()