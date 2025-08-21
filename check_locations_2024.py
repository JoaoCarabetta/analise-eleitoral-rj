#!/usr/bin/env python3
"""
Script para verificar locais de votação de 2024
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
    """Verificar locais de votação para 2024"""
    migrator = DataMigrator(SUPABASE_URL, SUPABASE_KEY, BIGQUERY_PROJECT_ID)
    
    print("=== Verificando locais de votação 2024 no BigQuery ===")
    
    query = """
    SELECT DISTINCT
        ano,
        COUNT(*) as total_locais
    FROM `basedosdados.br_tse_eleicoes.local_secao`
    WHERE sigla_uf = 'RJ'
        AND melhor_urbano IS NOT NULL
    GROUP BY ano
    ORDER BY ano DESC
    LIMIT 10
    """
    
    df = migrator.execute_bigquery(query)
    print("Locais de votação por ano no BigQuery:")
    for _, row in df.iterrows():
        print(f"- {row['ano']}: {row['total_locais']} locais")
    
    # Verificar especificamente 2024
    print(f"\n=== Dados detalhados para 2024 ===")
    
    query_2024 = """
    SELECT DISTINCT
        ano,
        id_municipio,
        zona,
        COUNT(*) as seções
    FROM `basedosdados.br_tse_eleicoes.local_secao`
    WHERE sigla_uf = 'RJ'
        AND ano = 2024
        AND melhor_urbano IS NOT NULL
    GROUP BY ano, id_municipio, zona
    ORDER BY id_municipio, zona
    LIMIT 20
    """
    
    df_2024 = migrator.execute_bigquery(query_2024)
    print(f"Amostra de zonas 2024: {len(df_2024)} registros")
    for _, row in df_2024.iterrows():
        print(f"- Município {row['id_municipio']}, Zona {row['zona']}: {row['seções']} seções")

if __name__ == "__main__":
    main()