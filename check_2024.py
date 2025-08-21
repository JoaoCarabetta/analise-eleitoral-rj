#!/usr/bin/env python3
"""
Script para verificar dados de 2024
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
    """Verificar dados de 2024"""
    migrator = DataMigrator(SUPABASE_URL, SUPABASE_KEY, BIGQUERY_PROJECT_ID)
    
    print("=== Verificando candidatos de 2024 ===")
    
    # Verificar candidatos de 2024 no Supabase
    candidatos_2024 = migrator.supabase.table('candidatos').select('*').eq('ano', 2024).execute()
    print(f"Candidatos de 2024 no Supabase: {len(candidatos_2024.data)}")
    
    for candidato in candidatos_2024.data:
        print(f"- {candidato['nome_urna']} (sequencial: {candidato['sequencial']})")
    
    # Verificar resultados por zona para 2024
    resultados_2024 = migrator.supabase.table('resultados_zona').select('*').eq('ano', 2024).execute()
    print(f"\nResultados de zona para 2024: {len(resultados_2024.data)}")
    
    # Verificar locais de votação para 2024
    locais_2024 = migrator.supabase.table('locais_votacao').select('*').eq('ano', 2024).execute()
    print(f"Locais de votação para 2024: {len(locais_2024.data)}")
    
    # Verificar se há dados de voto para 2024 no BigQuery
    print("\n=== Verificando BigQuery para 2024 ===")
    
    query = """
    SELECT DISTINCT ano
    FROM `basedosdados.br_tse_eleicoes.resultados_candidato_municipio_zona`
    WHERE sigla_uf = 'RJ'
    ORDER BY ano DESC
    """
    
    df = migrator.execute_bigquery(query)
    print("Anos disponíveis no BigQuery:")
    for ano in df['ano'].tolist():
        print(f"- {ano}")

if __name__ == "__main__":
    main()