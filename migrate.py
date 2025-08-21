#!/usr/bin/env python3
"""
Script para popular o Supabase com dados eleitorais da Base dos Dados
Requer: pandas, supabase, google-cloud-bigquery
"""

import pandas as pd
import os
from supabase import create_client, Client
from google.cloud import bigquery
import logging
from typing import List, Dict, Any
import time

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configurações
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Forçar credenciais corretas
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/joaoc/Documents/service_accounts/dbt/rj-crm-registry-2ed85ad46936.json'

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")  # Use service key em produção
BIGQUERY_PROJECT_ID = "rj-crm-registry"  # Projeto para billing
BASEDADOS_PROJECT_ID = "basedosdados"  # Projeto da Base dos Dados

class DataMigrator:
    def __init__(self, supabase_url: str, supabase_key: str, bigquery_project: str):
        """Inicializar clientes Supabase e BigQuery"""
        self.supabase: Client = create_client(supabase_url, supabase_key)
        self.bigquery_client = bigquery.Client(project=bigquery_project)
        
    def execute_bigquery(self, query: str) -> pd.DataFrame:
        """Executar query no BigQuery e retornar DataFrame"""
        try:
            logger.info(f"Executando query BigQuery...")
            job = self.bigquery_client.query(query)
            df = job.to_dataframe()
            logger.info(f"Query executada com sucesso. {len(df)} registros retornados.")
            return df
        except Exception as e:
            logger.error(f"Erro ao executar query BigQuery: {e}")
            raise
    
    def insert_batch_supabase(self, table: str, data: List[Dict[str, Any]], batch_size: int = 100):
        """Inserir dados em lotes no Supabase"""
        total_records = len(data)
        logger.info(f"Inserindo {total_records} registros na tabela {table}")
        
        for i in range(0, total_records, batch_size):
            batch = data[i:i + batch_size]
            try:
                result = self.supabase.table(table).insert(batch).execute()
                logger.info(f"Lote {i//batch_size + 1}: {len(batch)} registros inseridos")
                time.sleep(0.1)  # Pequena pausa para evitar rate limiting
            except Exception as e:
                logger.error(f"Erro ao inserir lote na tabela {table}: {e}")
                raise
                        
    def migrate_municipios(self):
        """Migrar dados de municípios do RJ"""
        logger.info("=== Migrando municípios ===")
        
        query = """
        SELECT DISTINCT
            id_municipio,
            nome,
            'RJ' as sigla_uf
        FROM `basedosdados.br_bd_diretorios_brasil.municipio`
        WHERE sigla_uf = 'RJ'
        ORDER BY nome
        """
        
        df = self.execute_bigquery(query)
        municipios_data = df.to_dict('records')
        
        self.insert_batch_supabase('municipios', municipios_data)
        logger.info(f"✅ {len(municipios_data)} municípios migrados")
    
    def migrate_candidatos(self):
        """Migrar dados de candidatos baseados no CSV"""
        logger.info("=== Migrando candidatos do CSV ===")
        
        # Ler candidatos do CSV
        csv_path = 'data/Candidatas_RJ__corrigido_.csv'
        candidatos_df = pd.read_csv(csv_path)
        
        # Extrair nomes únicos dos candidatos
        candidatos_relevantes = candidatos_df['nome_urna'].unique().tolist()
        logger.info(f"Encontrados {len(candidatos_relevantes)} candidatos no CSV")
        
        # Criar condição para buscar os candidatos
        nomes_condition = "'" + "', '".join(candidatos_relevantes) + "'"
        
        query = f"""
        SELECT 
            nome_urna,
            numero,
            ano,
            cargo,
            sigla_partido as partido,
            sequencial,
            situacao as resultado
        FROM `basedosdados.br_tse_eleicoes.candidatos`
        WHERE sigla_uf = 'RJ'
            AND ano >= 2016
            AND nome_urna IN ({nomes_condition})
        ORDER BY nome_urna, ano
        """
        
        df = self.execute_bigquery(query)
        
        # Adicionar campos adicionais
        df['ativo'] = True
        df['cor_mapa'] = '#ff4444'  # Cor padrão
        
        candidatos_data = df.to_dict('records')
        
        self.insert_batch_supabase('candidatos', candidatos_data)
        logger.info(f"✅ {len(candidatos_data)} candidatos migrados")
    
    def migrate_locais_votacao(self):
        """Migrar locais de votação com coordenadas"""
        logger.info("=== Migrando locais de votação ===")
        
        query = f"""
        SELECT DISTINCT
            ano,
            id_municipio,
            zona,
            AVG(ST_Y(melhor_urbano)) as latitude,
            AVG(ST_X(melhor_urbano)) as longitude
        FROM `{BASEDADOS_PROJECT_ID}.br_tse_eleicoes.local_secao`
        WHERE sigla_uf = 'RJ'
            AND ano >= 2016
            AND melhor_urbano IS NOT NULL
        GROUP BY ano, id_municipio, zona
        ORDER BY ano, id_municipio, zona
        """
        
        df = self.execute_bigquery(query)
        locais_data = df.to_dict('records')
        
        self.insert_batch_supabase('locais_votacao', locais_data, batch_size=50)
        logger.info(f"✅ {len(locais_data)} locais de votação migrados")
    
    def migrate_resultados_zona(self):
        """Migrar resultados por zona eleitoral"""
        logger.info("=== Migrando resultados por zona ===")
        
        # Primeiro, buscar IDs dos candidatos no Supabase
        candidatos_response = self.supabase.table('candidatos').select('id, sequencial, ano').execute()
        candidatos_map = {
            (item['sequencial'], item['ano']): item['id'] 
            for item in candidatos_response.data
        }
        
        logger.info(f"Encontrados {len(candidatos_map)} candidatos no Supabase")
        
        # Buscar resultados no BigQuery
        sequenciais = [seq for seq, _ in candidatos_map.keys()]
        sequenciais_condition = "'" + "', '".join(sequenciais) + "'"
        
        query = f"""
        SELECT 
            sequencial_candidato,
            ano,
            id_municipio,
            zona,
            votos
        FROM `basedosdados.br_tse_eleicoes.resultados_candidato_municipio_zona`
        WHERE sigla_uf = 'RJ'
            AND sequencial_candidato IN ({sequenciais_condition})
        ORDER BY sequencial_candidato, zona
        """
        
        df = self.execute_bigquery(query)
        
        # Mapear sequencial para candidato_id e remover duplicatas
        resultados_data = []
        seen_keys = set()
        
        for _, row in df.iterrows():
            key = (row['sequencial_candidato'], row['ano'])
            if key in candidatos_map:
                # Criar chave única para evitar duplicatas
                unique_key = (candidatos_map[key], row['ano'], row['id_municipio'], row['zona'])
                
                if unique_key not in seen_keys:
                    seen_keys.add(unique_key)
                    resultados_data.append({
                        'candidato_id': candidatos_map[key],
                        'ano': row['ano'],
                        'id_municipio': row['id_municipio'],
                        'zona': row['zona'],
                        'votos': row['votos']
                    })
        
        logger.info(f"Inserindo {len(resultados_data)} resultados únicos")
        self.insert_batch_supabase('resultados_zona', resultados_data, batch_size=50)
        logger.info(f"✅ {len(resultados_data)} resultados por zona migrados")
    
    def update_votos_totais(self):
        """Atualizar votos totais dos candidatos"""
        logger.info("=== Atualizando votos totais ===")
        
        # Primeiro, buscar os sequenciais dos candidatos que temos no Supabase
        candidatos_response = self.supabase.table('candidatos').select('sequencial').execute()
        sequenciais_supabase = [item['sequencial'] for item in candidatos_response.data]
        
        if not sequenciais_supabase:
            logger.warning("Nenhum candidato encontrado no Supabase")
            return
            
        sequenciais_condition = "'" + "', '".join(sequenciais_supabase) + "'"
        logger.info(f"Atualizando votos para {len(sequenciais_supabase)} candidatos")
        
        # Buscar resultados do BigQuery apenas para nossos candidatos
        query = f"""
        SELECT 
            sequencial_candidato,
            SUM(votos) as votos_total
        FROM `{BASEDADOS_PROJECT_ID}.br_tse_eleicoes.resultados_candidato`
        WHERE sigla_uf = 'RJ'
            AND sequencial_candidato IN ({sequenciais_condition})
        GROUP BY sequencial_candidato
        """
        
        df = self.execute_bigquery(query)
        logger.info(f"Encontrados votos para {len(df)} candidatos no BigQuery")
        
        # Atualizar no Supabase
        updated_count = 0
        for _, row in df.iterrows():
            try:
                result = self.supabase.table('candidatos').update({
                    'votos_total': int(row['votos_total'])
                }).eq('sequencial', row['sequencial_candidato']).execute()
                updated_count += 1
                logger.info(f"Candidato {row['sequencial_candidato']}: {int(row['votos_total']):,} votos")
            except Exception as e:
                logger.error(f"Erro ao atualizar votos para {row['sequencial_candidato']}: {e}")
                
        logger.info(f"✅ Votos totais atualizados para {updated_count} candidatos")
    
    def clean_tables(self):
        """Limpar todas as tabelas antes da migração"""
        logger.info("🧹 Limpando tabelas existentes...")
        
        # Limpar tabelas usando gte (maior ou igual) que funciona melhor
        tables_to_clean = ['resultados_zona', 'locais_votacao', 'candidatos', 'municipios']
        
        for table in tables_to_clean:
            try:
                # Usar gte(0) para limpar todos os registros (funciona para IDs numéricos)
                if table == 'municipios':
                    result = self.supabase.table(table).delete().neq('id_municipio', '0').execute()
                else:
                    result = self.supabase.table(table).delete().gte('id', 0).execute()
                logger.info(f"✅ Tabela {table} limpa")
            except Exception as e:
                logger.warning(f"⚠️ Erro ao limpar tabela {table}: {e}")

    def run_partial_migration(self):
        """Executar apenas resultados por zona"""
        logger.info("🚀 Migrando apenas resultados por zona")
        
        try:
            # Migrar resultados por zona
            self.migrate_resultados_zona()
            
            # Atualizar votos totais
            self.update_votos_totais()
            
            logger.info("🎉 Migração de resultados finalizada com sucesso!")
            
        except Exception as e:
            logger.error(f"❌ Erro durante a migração: {e}")
            raise

    def run_full_migration(self):
        """Executar migração completa"""
        logger.info("🚀 Iniciando migração completa dos dados eleitorais")
        
        try:
            # 0. Limpar tabelas existentes
            self.clean_tables()
            
            # 1. Migrar municípios (dependência para outras tabelas)
            self.migrate_municipios()
            
            # 2. Migrar candidatos
            self.migrate_candidatos()
            
            # 3. Migrar locais de votação
            self.migrate_locais_votacao()
            
            # 4. Migrar resultados por zona
            self.migrate_resultados_zona()
            
            # 5. Atualizar votos totais
            self.update_votos_totais()
            
            logger.info("🎉 Migração completa finalizada com sucesso!")
            
        except Exception as e:
            logger.error(f"❌ Erro durante a migração: {e}")
            raise

def main():
    """Função principal"""
    # Verificar variáveis de ambiente
    supabase_url = os.getenv('SUPABASE_URL', SUPABASE_URL)
    supabase_key = os.getenv('SUPABASE_SERVICE_KEY', SUPABASE_KEY)
    bigquery_project = os.getenv('BIGQUERY_PROJECT_ID', BIGQUERY_PROJECT_ID)
    
    if not all([supabase_url, supabase_key]):
        logger.error("❌ Configure as variáveis SUPABASE_URL e SUPABASE_SERVICE_KEY")
        return
    
    # Verificar autenticação BigQuery
    if not os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
        logger.warning("⚠️ GOOGLE_APPLICATION_CREDENTIALS não definida. "
                      "Certifique-se de ter autenticação configurada para BigQuery")
    
    # Executar migração
    migrator = DataMigrator(supabase_url, supabase_key, bigquery_project)
    migrator.run_full_migration()  # Migração completa com dados do CSV

if __name__ == "__main__":
    main()