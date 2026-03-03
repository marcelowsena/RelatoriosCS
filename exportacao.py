import pandas as pd
import json
from datetime import datetime
import os
import glob
import subprocess
from cv_crm_simple import CVCRMSimple
from creds import mail, token
import time

class CVCRMToPowerBI:
    def __init__(self, subdomain, email, token, output_folder="powerbi_data", downloads_folder="downloads"):
        self.client = CVCRMSimple(subdomain, email, token)
        self.output_folder = output_folder
        self.downloads_folder = downloads_folder
        os.makedirs(output_folder, exist_ok=True)
        os.makedirs(downloads_folder, exist_ok=True)
        
        # Data de corte para acessos válidos
        self.data_corte_acessos = pd.to_datetime('2025-03-17')
    
    def update_access_report(self):
        """Executa rpateste.py para baixar relatório mais recente e limpa arquivos antigos"""
        print("Atualizando relatório de acessos...")
        
        try:
            # Executar script de download
            print("Executando rpateste.py...")
            result = subprocess.run(['python', 'rpateste.py'], 
                                  capture_output=True, 
                                  text=True, 
                                  timeout=300)  # 5 minutos timeout
            
            if result.returncode != 0:
                print(f"Erro ao executar rpateste.py: {result.stderr}")
                return None
            
            print("Download concluído!")
            print(f"Output do rpateste.py: {result.stdout}")
            
            # Encontrar todos os arquivos CSV na pasta downloads
            csv_files = glob.glob(os.path.join(self.downloads_folder, "*.csv"))
            
            if not csv_files:
                print("Nenhum arquivo CSV encontrado na pasta downloads")
                return None
            
            # Ordenar por data de modificação (mais recente primeiro)
            csv_files.sort(key=os.path.getmtime, reverse=True)
            
            # Arquivo mais recente
            latest_file = csv_files[0]
            print(f"Arquivo mais recente: {latest_file}")
            
            # Remover arquivos antigos (manter apenas o mais recente)
            if len(csv_files) > 1:
                print(f"Removendo {len(csv_files) - 1} arquivo(s) antigo(s)...")
                for old_file in csv_files[1:]:
                    try:
                        os.remove(old_file)
                        print(f"Removido: {old_file}")
                    except Exception as e:
                        print(f"Erro ao remover {old_file}: {e}")
            
            return latest_file
            
        except subprocess.TimeoutExpired:
            print("Timeout ao executar rpateste.py")
            return None
        except Exception as e:
            print(f"Erro ao atualizar relatório: {e}")
            return None
    
    def process_acessos_report(self, file_path=None):
        """Processa relatório de acessos para identificar primeiro acesso após 17/03/2025"""
        print("Processando relatório de acessos...")
        
        # Se não foi fornecido arquivo específico, buscar o mais recente na pasta downloads
        if file_path is None:
            csv_files = glob.glob(os.path.join(self.downloads_folder, "*.csv"))
            if not csv_files:
                print("Nenhum arquivo CSV encontrado na pasta downloads")
                return pd.DataFrame()
            
            # Pegar o mais recente
            file_path = max(csv_files, key=os.path.getmtime)
            print(f"Usando arquivo: {file_path}")
        
        try:
            if not os.path.exists(file_path):
                print(f"Arquivo de acessos não encontrado: {file_path}")
                return pd.DataFrame()
            
            # Baseado na amostra, o arquivo usa ; como separador e tem aspas
            df_acessos = pd.read_csv(file_path, 
                                   encoding='utf-8-sig',
                                   delimiter=';',
                                   quotechar='"')
            
            if df_acessos.empty:
                print("Arquivo de acessos está vazio")
                return pd.DataFrame()
            
            print(f"Colunas encontradas: {list(df_acessos.columns)}")
            print(f"Total de registros: {len(df_acessos)}")
            
            # Mapear colunas baseado na estrutura identificada
            cliente_col = 'Pessoa'
            data_col = 'Data de Acesso'
            emp_col = 'Empreendimentos'
            
            if cliente_col not in df_acessos.columns or data_col not in df_acessos.columns:
                print(f"Colunas esperadas não encontradas. Disponíveis: {list(df_acessos.columns)}")
                return pd.DataFrame()
            
            # Limpar caracteres especiais HTML nos nomes dos clientes
            def clean_html_entities(text):
                if pd.isna(text):
                    return text
                
                text = str(text)
                # Substituir entidades HTML comuns
                replacements = {
                    '&amp;': '&',
                    '&lt;': '<',
                    '&gt;': '>',
                    '&quot;': '"',
                    '&#39;': "'",
                    '&nbsp;': ' '
                }
                
                for html_entity, char in replacements.items():
                    text = text.replace(html_entity, char)
                
                return text.strip()
            
            # Aplicar limpeza nos nomes dos clientes
            df_acessos[cliente_col] = df_acessos[cliente_col].apply(clean_html_entities)
            
            # Processar data no formato brasileiro com hora
            def parse_date_br(date_str):
                try:
                    if pd.isna(date_str):
                        return None
                    
                    date_str = str(date_str).strip('"')
                    
                    if 'às' in date_str:
                        date_part, time_part = date_str.split(' às ')
                        time_part = time_part.replace('h', ':')
                        if ':' not in time_part:
                            time_part += ':00'
                        full_date = f"{date_part} {time_part}"
                    else:
                        full_date = date_str
                    
                    return pd.to_datetime(full_date, format='%d/%m/%Y %H:%M', errors='coerce')
                except:
                    return None
            
            df_acessos['data_acesso_parsed'] = df_acessos[data_col].apply(parse_date_br)
            
            # Filtrar apenas acessos após 17/03/2025
            print(f"Data de corte para acessos: {self.data_corte_acessos.strftime('%d/%m/%Y')}")
            df_acessos_filtrado = df_acessos[df_acessos['data_acesso_parsed'] >= self.data_corte_acessos]
            
            print(f"Registros antes do filtro de data: {len(df_acessos)}")
            print(f"Registros após filtro de data (>= 17/03/2025): {len(df_acessos_filtrado)}")
            
            # Remover linhas com dados inválidos
            df_acessos_filtrado = df_acessos_filtrado.dropna(subset=[cliente_col, 'data_acesso_parsed'])
            
            if df_acessos_filtrado.empty:
                print("Nenhum dado válido encontrado após filtros")
                return pd.DataFrame()
            
            print(f"Registros válidos após limpeza: {len(df_acessos_filtrado)}")
            
            # Identificar primeiro acesso por cliente (após data de corte)
            primeiro_acesso = df_acessos_filtrado.groupby(cliente_col)['data_acesso_parsed'].min().reset_index()
            primeiro_acesso.columns = ['cliente', 'primeiro_acesso']
            
            # Estatísticas de acesso com empreendimentos
            if emp_col in df_acessos_filtrado.columns:
                df_acessos_stats = df_acessos_filtrado.groupby(cliente_col).agg({
                    'data_acesso_parsed': ['count', 'min', 'max'],
                    emp_col: lambda x: ', '.join(sorted(x.unique()))
                }).reset_index()
                
                df_acessos_stats.columns = ['cliente', 'total_acessos', 'primeiro_acesso', 'ultimo_acesso', 'empreendimentos_acessados']
                
                # Adicionar empreendimentos ao primeiro acesso também
                emps_por_cliente = df_acessos_filtrado.groupby(cliente_col)[emp_col].apply(
                    lambda x: ', '.join(sorted(x.unique()))
                ).reset_index()
                emps_por_cliente.columns = ['cliente', 'empreendimentos']
                
                primeiro_acesso = primeiro_acesso.merge(emps_por_cliente, on='cliente', how='left')
            else:
                df_acessos_stats = df_acessos_filtrado.groupby(cliente_col).agg({
                    'data_acesso_parsed': ['count', 'min', 'max']
                }).reset_index()
                
                df_acessos_stats.columns = ['cliente', 'total_acessos', 'primeiro_acesso', 'ultimo_acesso']
            
            # Salvar dados processados com delimiter ;
            output_file = f"{self.output_folder}/primeiro_acesso_clientes.csv"
            primeiro_acesso.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
            print(f"Primeiro acesso salvo: {output_file} - {len(primeiro_acesso)} clientes únicos")
            
            output_file_stats = f"{self.output_folder}/estatisticas_acesso_clientes.csv"
            df_acessos_stats.to_csv(output_file_stats, index=False, encoding='utf-8-sig', sep=';')
            print(f"Estatísticas de acesso salvas: {output_file_stats}")
            
            # Análise por empreendimento (apenas acessos válidos)
            if emp_col in df_acessos_filtrado.columns:
                acessos_por_emp = df_acessos_filtrado.groupby(emp_col).agg({
                    cliente_col: 'nunique',
                    'data_acesso_parsed': 'count'
                }).reset_index()
                acessos_por_emp.columns = ['empreendimento', 'clientes_unicos', 'total_acessos']
                
                output_file_emp = f"{self.output_folder}/acessos_por_empreendimento.csv"
                acessos_por_emp.to_csv(output_file_emp, index=False, encoding='utf-8-sig', sep=';')
                print(f"Acessos por empreendimento salvos: {output_file_emp}")
            
            return primeiro_acesso
                
        except Exception as e:
            print(f"Erro ao processar arquivo de acessos: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
        
    def extract_atendimentos(self):
        """Extrai atendimentos formatados para Power BI"""
        print("Extraindo atendimentos...")
        atendimentos = self.client.listar_atendimentos()
        
        if not atendimentos:
            print("Nenhum atendimento encontrado")
            return pd.DataFrame()
        
        df = pd.DataFrame(atendimentos)
        
        # Usar campo correto: 'dataCad' em vez de 'data_cad'
        df['dataCad'] = pd.to_datetime(df['dataCad'], errors='coerce')
        df['ano'] = df['dataCad'].dt.year
        df['mes'] = df['dataCad'].dt.month
        df['dia_semana'] = df['dataCad'].dt.day_name()
        df['trimestre'] = df['dataCad'].dt.quarter
        
        # Outras datas importantes
        df['dataUltimaModificacaoSituacao'] = pd.to_datetime(df['dataUltimaModificacaoSituacao'], errors='coerce')
        df['ultimaInteracao'] = pd.to_datetime(df['ultimaInteracao'], errors='coerce')
        
        # Extrair dados do empreendimento
        df['empreendimento_id'] = df['empreendimento'].apply(
            lambda x: x.get('idempreendimento') if isinstance(x, dict) else None
        )
        df['empreendimento_nome'] = df['empreendimento'].apply(
            lambda x: x.get('nome') if isinstance(x, dict) else None
        )
        
        # Calcular SLA (assumindo 2 dias úteis como meta)
        df['tempoFinalizado'] = pd.to_numeric(df['tempoFinalizado'], errors='coerce')
        df['sla_ok'] = df['tempoFinalizado'] <= 48  # 48 horas = 2 dias úteis
        
        # Converter campos numéricos
        df['tempoResposta'] = pd.to_numeric(df['tempoResposta'], errors='coerce')
        df['slaWorkflow'] = pd.to_numeric(df['slaWorkflow'], errors='coerce')
        
        # Limpar dados complexos que não precisamos no Power BI
        columns_to_drop = ['empreendimento', 'respostas', 'arquivos', 'camposAdicionais']
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
        
        # Salvar com delimiter ;
        output_file = f"{self.output_folder}/atendimentos.csv"
        df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        print(f"Atendimentos salvos: {output_file} - {len(df)} registros")
        
        return df
    
    def extract_vendas(self):
        """Extrai TODAS as vendas formatadas para Power BI"""
        print("Extraindo vendas...")
        try:
            # Primeiro, descobrir quantas vendas existem
            vendas_response = self.client.listar_vendas(registros=30)
            total_registros = vendas_response.get('total_de_registros', 0)
            total_paginas = vendas_response.get('total_de_paginas', 1)
            
            print(f"Total de vendas: {total_registros} em {total_paginas} páginas")
            
            all_vendas = []
            
            # Buscar todas as páginas
            for pagina in range(1, total_paginas + 1):
                print(f"Buscando página {pagina}/{total_paginas}...")
                vendas_response = self.client.listar_vendas(pagina=pagina, registros=100)
                vendas_pagina = vendas_response.get('dados', [])
                all_vendas.extend(vendas_pagina)
                
                # Rate limiting
                if pagina < total_paginas:
                    import time
                    time.sleep(1.5)
            
            if not all_vendas:
                print("Nenhuma venda encontrada")
                return pd.DataFrame()
            
            df = pd.DataFrame(all_vendas)
            print(f"Total de vendas extraídas: {len(df)}")
            
            # Processar datas usando campos corretos
            df['data_venda'] = pd.to_datetime(df['data_venda'], errors='coerce')
            df['data_reserva'] = pd.to_datetime(df['data_reserva'], errors='coerce')
            df['data_historico'] = pd.to_datetime(df['data_historico'], errors='coerce')
            
            # Criar colunas derivadas de data
            df['ano_venda'] = df['data_venda'].dt.year
            df['mes_venda'] = df['data_venda'].dt.month
            df['trimestre'] = df['data_venda'].dt.quarter
            
            # Processar valor do contrato
            df['valor_contrato'] = pd.to_numeric(df['valor_contrato'], errors='coerce')
            
            # Criar categorias de valor
            if df['valor_contrato'].notna().any():
                df['faixa_valor'] = pd.cut(df['valor_contrato'], 
                                         bins=[0, 200000, 400000, 600000, float('inf')],
                                         labels=['Até 200k', '200k-400k', '400k-600k', 'Acima 600k'])
            
            # Processar outros campos numéricos
            df['area_privativa'] = pd.to_numeric(df['area_privativa'], errors='coerce')
            df['renda'] = pd.to_numeric(df['renda'], errors='coerce')
            df['idade'] = pd.to_numeric(df['idade'], errors='coerce')
            
            # Limpar campos que não precisamos
            columns_to_drop = ['associados']
            df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
            
            # Salvar com delimiter ;
            output_file = f"{self.output_folder}/vendas.csv"
            df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
            print(f"Vendas salvas: {output_file} - {len(df)} registros")
            
            return df
            
        except Exception as e:
            print(f"Erro ao extrair vendas: {e}")
            return pd.DataFrame()
    
    def extract_empreendimentos(self):
        """Extrai empreendimentos formatados para Power BI"""
        print("Extraindo empreendimentos...")
        empreendimentos = self.client.listar_empreendimentos()
        
        if not empreendimentos:
            print("Nenhum empreendimento encontrado")
            return pd.DataFrame()
        
        # Processar apenas dados principais dos empreendimentos
        emp_data = []
        
        for emp in empreendimentos:
            # Dados principais do empreendimento
            emp_row = {
                'idempreendimento': emp['idempreendimento'],
                'nome': emp['nome'],
                'cidade': emp['cidade'],
                'estado': emp['estado'],
                'data_entrega': emp['data_entrega'],
                'situacao_obra': emp['situacao_obra'],
                'quantidade_unidades_disponiveis': emp['quantidade_unidades_disponiveis'],
                'endereco': emp.get('endereco'),
                'area_construida': emp.get('area_construida'),
                'area_privativa': emp.get('area_privativa'),
                'descricao': emp.get('descricao')
            }
            emp_data.append(emp_row)
        
        # Criar DataFrame
        df_emp = pd.DataFrame(emp_data)
        
        # Processar data de entrega
        df_emp['data_entrega'] = pd.to_datetime(df_emp['data_entrega'], format='%d/%m/%Y', errors='coerce')
        df_emp['ano_entrega'] = df_emp['data_entrega'].dt.year
        
        # Converter campos numéricos
        df_emp['quantidade_unidades_disponiveis'] = pd.to_numeric(df_emp['quantidade_unidades_disponiveis'], errors='coerce')
        
        # Classificar empreendimentos
        today = pd.Timestamp.now()
        df_emp['status_entrega'] = df_emp['data_entrega'].apply(
            lambda x: 'Entregue' if pd.notna(x) and x < today else 
                     'Em construção' if pd.notna(x) else 'Indefinido'
        )
        
        # Salvar empreendimentos com delimiter ;
        output_file = f"{self.output_folder}/empreendimentos.csv"
        df_emp.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        print(f"Empreendimentos salvos: {output_file} - {len(df_emp)} registros")
        
        return df_emp
    
    def extract_clientes(self):
        """Extrai TODOS os clientes formatados para Power BI"""
        print("Extraindo clientes...")
        try:
            # Primeiro, descobrir quantos clientes existem
            clientes_response = self.client.listar_clientes(registros=30)
            total_registros = clientes_response.get('total_de_registros', 0)
            
            # Calcular total de páginas (30 registros por página)
            total_paginas = (total_registros + 29) // 30
            
            print(f"Total de clientes: {total_registros} em {total_paginas} páginas")
            
            all_clientes = []
            
            # Buscar todas as páginas
            for pagina in range(1, total_paginas + 1):
                print(f"Buscando página {pagina}/{total_paginas}...")
                clientes_response = self.client.listar_clientes(pagina=pagina, registros=30)
                clientes_pagina = clientes_response.get('dados', [])
                all_clientes.extend(clientes_pagina)
                
                # Rate limiting
                if pagina < total_paginas:
                    import time
                    time.sleep(1.5)
            
            if not all_clientes:
                print("Nenhum cliente encontrado")
                return pd.DataFrame()
            
            df = pd.DataFrame(all_clientes)
            print(f"Total de clientes extraídos: {len(df)}")
            
            # Formatar renda familiar
            df['renda_familiar'] = pd.to_numeric(df['renda_familiar'], errors='coerce')
            
            # Criar faixas de renda
            if df['renda_familiar'].notna().any():
                df['faixa_renda'] = pd.cut(df['renda_familiar'], 
                                         bins=[0, 3000, 6000, 10000, float('inf')],
                                         labels=['Até 3k', '3k-6k', '6k-10k', 'Acima 10k'])
            
            # Salvar com delimiter ;
            output_file = f"{self.output_folder}/clientes.csv"
            df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
            print(f"Clientes salvos: {output_file} - {len(df)} registros")
            
            return df
            
        except Exception as e:
            print(f"Erro ao extrair clientes (pode não ter permissão CVDW): {e}")
            return pd.DataFrame()
    
    def create_powerbi_ready_dataset(self):
        """Cria dataset completo para Power BI"""
        print("=== Extração CV CRM para Power BI ===")
        
        # 1. Atualizar relatório de acessos
        latest_access_file = self.update_access_report()
        
        # 2. Extrair dados do CV CRM
        df_atendimentos = self.extract_atendimentos()
        df_vendas = self.extract_vendas()
        df_empreendimentos = self.extract_empreendimentos()
        df_clientes = self.extract_clientes()
        
        # 3. Processar arquivo de acessos (usa o mais recente se disponível)
        df_acessos = self.process_acessos_report(latest_access_file)
        
        # 4. Criar análises específicas para CS
        self.create_cs_analysis(df_atendimentos, df_vendas, df_empreendimentos, df_clientes, df_acessos)
        
        # 5. Criar arquivo de metadados
        metadata = {
            "data_extracao": datetime.now().isoformat(),
            "data_corte_acessos": self.data_corte_acessos.isoformat(),
            "arquivo_acessos_usado": latest_access_file if latest_access_file else "Não encontrado",
            "tabelas": {
                "atendimentos": len(df_atendimentos),
                "vendas": len(df_vendas),
                "empreendimentos": len(df_empreendimentos),
                "clientes": len(df_clientes),
                "acessos": len(df_acessos)
            },
            "relacionamentos_sugeridos": [
                {
                    "tabela1": "atendimentos",
                    "campo1": "empreendimento_id",
                    "tabela2": "empreendimentos", 
                    "campo2": "idempreendimento"
                },
                {
                    "tabela1": "vendas",
                    "campo1": "empreendimento",
                    "tabela2": "empreendimentos",
                    "campo2": "nome"
                },
                {
                    "tabela1": "vendas",
                    "campo1": "cliente",
                    "tabela2": "primeiro_acesso_clientes",
                    "campo2": "cliente"
                }
            ]
        }
        
        with open(f"{self.output_folder}/metadata.json", 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        print(f"\n=== Extração Concluída ===")
        print(f"Arquivos salvos em: {self.output_folder}/")
        print(f"Atendimentos: {len(df_atendimentos)} registros")
        print(f"Vendas: {len(df_vendas)} registros")
        print(f"Empreendimentos: {len(df_empreendimentos)} registros")
        print(f"Clientes: {len(df_clientes)} registros")
        if not df_acessos.empty:
            print(f"Acessos processados: {len(df_acessos)} clientes únicos (após 17/03/2025)")
        
        return {
            'atendimentos': df_atendimentos,
            'vendas': df_vendas,
            'empreendimentos': df_empreendimentos,
            'clientes': df_clientes,
            'acessos': df_acessos
        }
    
    def create_cs_analysis(self, df_atendimentos, df_vendas, df_empreendimentos, df_clientes, df_acessos):
        """Cria análises específicas para Customer Success"""
        print("Criando análises de Customer Success...")
        
        # Dicionário de obras recentes (definir clientes ativos)
        obras_recentes = {
            'NAUT': 'ativo',
            'AMALUNA': 'ativo',
            'SOLLUS': 'ativo',
            'ESSENZA': 'novo_lancamento',
            'NEOON': 'novo_lancamento',
            'OCEAN VIEW': 'ativo',
            'SOUL': 'ativo',
            'OPERA': 'ativo',
            'COSTA CLUB RESIDENCIAL': 'ativo',
            'MORADA DE GAIA': 'ativo',
            'CASAS VIVAPARK': 'ativo',
            'CORA': 'ativo',
            'MASSIMO': 'ativo',
            'ARIUM': 'ativo',
            'PULSE': 'ativo'
        }
        
        print(f"Obras configuradas como recentes: {list(obras_recentes.keys())}")
        
        # 1. Análise de clientes ativos baseada nas obras recentes
        if not df_vendas.empty and not df_empreendimentos.empty:
            # Criar DataFrame com configuração de obras
            obras_config = pd.DataFrame([
                {'empreendimento': nome, 'categoria': categoria} 
                for nome, categoria in obras_recentes.items()
            ])
            
            # Merge vendas com configuração de obras
            vendas_categorized = df_vendas.merge(
                obras_config, 
                left_on='empreendimento', 
                right_on='empreendimento', 
                how='left'
            )
            
            # Preencher categoria para obras não configuradas
            vendas_categorized['categoria'] = vendas_categorized['categoria'].fillna('obra_antiga')
            
            # Merge com dados dos empreendimentos
            vendas_complete = vendas_categorized.merge(
                df_empreendimentos[['nome', 'status_entrega', 'data_entrega', 'situacao_obra']], 
                left_on='empreendimento', 
                right_on='nome', 
                how='left'
            )
            
            # Definir clientes ativos: compraram em obras recentes
            clientes_ativos = vendas_complete[
                vendas_complete['categoria'] == 'ativo'
            ].groupby(['empreendimento', 'situacao_obra']).agg({
                'cliente': 'nunique',
                'valor_contrato': ['sum', 'mean'],
                'data_venda': ['min', 'max']
            }).reset_index()
            
            # Flatten column names
            clientes_ativos.columns = [
                'empreendimento', 'situacao_obra', 'clientes_ativos', 
                'valor_total', 'ticket_medio', 'primeira_venda', 'ultima_venda'
            ]
            
            output_file = f"{self.output_folder}/clientes_ativos_por_empreendimento.csv"
            clientes_ativos.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
            print(f"Clientes ativos salvos: {output_file} - {len(clientes_ativos)} empreendimentos")
            
            # 2. Base consolidada de clientes com classificação
            base_clientes = vendas_complete.groupby('cliente').agg({
                'empreendimento': lambda x: ', '.join(x.unique()),
                'categoria': lambda x: ', '.join(x.unique()),
                'valor_contrato': 'sum',
                'data_venda': ['min', 'max'],
                'documento_cliente': 'first'
            }).reset_index()
            
            # Flatten column names
            base_clientes.columns = [
                'cliente', 'empreendimentos', 'categorias', 'valor_total_investido',
                'primeira_compra', 'ultima_compra', 'documento'
            ]
            
            # Classificar tipo de cliente
            base_clientes['tipo_cliente'] = base_clientes['categorias'].apply(
                lambda x: 'Ativo' if 'ativo' in x 
                         else 'Prospect Novo Lançamento' if 'novo_lancamento' in x
                         else 'Histórico'
            )
            
            # Merge com dados de primeiro acesso se disponível
            if not df_acessos.empty:
                base_clientes = base_clientes.merge(
                    df_acessos, 
                    on='cliente', 
                    how='left'
                )
                base_clientes['tem_acesso_app'] = base_clientes['primeiro_acesso'].notna()
            
            output_file = f"{self.output_folder}/base_consolidada_clientes.csv"
            base_clientes.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
            print(f"Base consolidada salva: {output_file} - {len(base_clientes)} clientes únicos")
            
            # 3. Salvar configuração de obras para referência
            output_file = f"{self.output_folder}/configuracao_obras_recentes.csv"
            obras_config.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
            print(f"Configuração de obras salva: {output_file}")
            
            # 4. Estatísticas por categoria
            stats_categoria = vendas_complete.groupby('categoria').agg({
                'cliente': 'nunique',
                'valor_contrato': ['sum', 'mean'],
                'empreendimento': lambda x: ', '.join(x.unique())
            }).reset_index()
            
            stats_categoria.columns = [
                'categoria', 'total_clientes', 'valor_total', 'ticket_medio', 'empreendimentos'
            ]
            
            output_file = f"{self.output_folder}/estatisticas_por_categoria.csv"
            stats_categoria.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
            print(f"Estatísticas por categoria salvas: {output_file}")
        
        print("Análises de CS concluídas")

# Script de automação para agendar extrações
class PowerBIDataRefresh:
    def __init__(self, config_file="cv_config.json"):
        # Usar credenciais do arquivo creds.py se config_file não existir
        if not os.path.exists(config_file):
            self.extractor = CVCRMToPowerBI(
                subdomain="halsten",  # Alterar conforme necessário
                email=mail,
                token=token
            )
        else:
            with open(config_file, 'r') as f:
                config = json.load(f)
            
            self.extractor = CVCRMToPowerBI(
                config['subdomain'],
                config['email'],
                config['token']
            )
    
    def daily_refresh(self):
        """Refresh diário dos dados"""
        print(f"Refresh diário iniciado: {datetime.now()}")
        self.extractor.create_powerbi_ready_dataset()
        print("Refresh concluído")
    
    def create_config_template(self):
        """Cria template de configuração"""
        config = {
            "subdomain": "halsten",
            "email": mail,
            "token": token,
            "refresh_schedule": "daily_8am"
        }
        
        with open("cv_config.json", 'w') as f:
            json.dump(config, f, indent=2)
        
        print("Template de configuração criado: cv_config.json")

def main():
    # Configurar extração usando credenciais do arquivo creds.py
    extractor = CVCRMToPowerBI(
        subdomain="halsten",
        email=mail,
        token=token
    )
    
    # Executar extração completa
    datasets = extractor.create_powerbi_ready_dataset()
    
    print("\n" + "="*60)
    print("ARQUIVOS GERADOS PARA POWER BI:")
    print("="*60)
    print("📊 DADOS PRINCIPAIS:")
    print("  1. atendimentos.csv - Dados de atendimento com SLA")
    print("  2. vendas.csv - Todas as vendas históricas") 
    print("  3. empreendimentos.csv - Dados dos empreendimentos")
    print("  4. clientes.csv - Base completa de clientes")
    print()
    print("🔍 ANÁLISES DE ACESSO:")
    print("  5. primeiro_acesso_clientes.csv - Primeiro acesso de cada cliente")
    print("  6. estatisticas_acesso_clientes.csv - Estatísticas de uso")
    print("  7. acessos_por_empreendimento.csv - Acessos agrupados por projeto")
    print()
    print("📈 ANÁLISES CUSTOMER SUCCESS:")
    print("  8. clientes_ativos_por_empreendimento.csv - Clientes por obra recente")
    print("  9. base_consolidada_clientes.csv - Base unificada com classificações")
    print(" 10. configuracao_obras_recentes.csv - Dicionário de obras")
    print(" 11. estatisticas_por_categoria.csv - Stats por tipo de obra")
    print()
    print("🎯 KPIs DISPONÍVEIS:")
    print("  - SLA de atendimento (meta: 2 dias úteis)")
    print("  - Taxa de primeiro acesso dos clientes")
    print("  - Clientes ativos (obras: NAUT, AMALUNA, SOLLUS)")
    print("  - Acessos por empreendimento")
    print()
    print("⚙️  CONFIGURAÇÃO:")
    print("  - Obras ATIVAS: NAUT, AMALUNA, SOLLUS, OCEAN VIEW, SOUL, OPERA,")
    print("                  COSTA CLUB RESIDENCIAL, MORADA DE GAIA, CASAS VIVAPARK,")
    print("                  CORA, MASSIMO, ARIUM, PULSE")
    print("  - Novos Lançamentos: ESSENZA, NEOON")
    print("  - Para alterar, edite o dicionário 'obras_recentes' no código")
    print("="*60)

if __name__ == "__main__":
    main()