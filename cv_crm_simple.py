import requests
import time
from typing import Dict, List, Optional
from creds import mail, token

class CVCRMSimple:
    """Cliente simples para CV CRM - apenas os endpoints que você precisa"""
    
    def __init__(self, subdomain: str, email: str, token: str):
        self.base_url = f"https://{subdomain}.cvcrm.com.br"
        self.headers = {
            'accept': 'application/json',
            'email': email,
            'token': token
        }
        self.last_request_time = 0
        
    def _wait_rate_limit(self):
        """Rate limiting simples - 3 segundos entre requests"""
        elapsed = time.time() - self.last_request_time
        if elapsed < 3:
            time.sleep(3 - elapsed)
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Faz request com rate limiting e retry em caso de 429"""
        url = f"{self.base_url}{endpoint}"
        for attempt in range(4):
            self._wait_rate_limit()
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429 and attempt < 3:
                wait = (attempt + 1) * 15  # 15s, 30s, 45s
                print(f"Rate limit 429 (tentativa {attempt + 1}/3), aguardando {wait}s...")
                time.sleep(wait)
                self.last_request_time = 0  # forçar wait completo na próxima
            else:
                raise Exception(f"Erro {response.status_code}: {response.text}")
    
    def listar_atendimentos(self) -> List[Dict]:
        """Lista todos os atendimentos"""
        response = self._make_request('/api/cvio/atendimento/listar')
        
        # Remove o campo 'codigo' e retorna apenas os atendimentos
        if isinstance(response, dict) and 'codigo' in response:
            atendimentos = {k: v for k, v in response.items() if k != 'codigo'}
            return list(atendimentos.values())
        
        return response
    
    def listar_tarefas(self, offset: int = 0, limit: int = 30) -> Dict:
        """Lista tarefas com paginação"""
        params = {'offset': offset, 'limit': limit}
        return self._make_request('/api/v1/cv/tarefas', params=params)
    
    def buscar_tarefa_por_id(self, id_tarefa_criada: int) -> Dict:
        """Busca tarefa específica por ID"""
        return self._make_request(f'/api/v1/cv/tarefas/{id_tarefa_criada}')
    
    def listar_vendas(self, pagina: int = 1, registros: int = 30) -> Dict:
        """Lista vendas com paginação"""
        params = {'pagina': pagina, 'registros': registros}
        return self._make_request('/api/v1/cvdw/vendas', params=params)
    
    def listar_empreendimentos(self) -> List[Dict]:
        """Lista todos os empreendimentos"""
        return self._make_request('/api/v1/cvbot/empreendimentos')
    
    def listar_unidades_empreendimento(self, id_empreendimento: int) -> List[Dict]:
        """Lista unidades disponíveis de um empreendimento"""
        return self._make_request(f'/api/v1/cvbot/empreendimentos/{id_empreendimento}/unidades')
    
    def listar_clientes(self, pagina: int = 1, registros: int = 30) -> Dict:
        """Lista clientes/pessoas (CVDW - requer permissão especial)"""
        params = {'pagina': pagina, 'registros': registros}
        return self._make_request('/api/v1/cvdw/pessoas', params=params)
    
    def buscar_cliente_por_documento(self, documento: str) -> Dict:
        """Busca cliente por documento (CPF/CNPJ)"""
        params = {'documento': documento}
        return self._make_request('/api/cvio/cliente', params=params)
    
    def buscar_cliente_por_email(self, email: str) -> Dict:
        """Busca cliente por email"""
        params = {'email': email}
        return self._make_request('/api/cvio/cliente', params=params)
    
    def buscar_cliente_por_telefone(self, telefone: str) -> Dict:
        """Busca cliente por telefone"""
        params = {'telefone': telefone}
        return self._make_request('/api/cvio/cliente', params=params)

# Exemplo de uso
if __name__ == "__main__":
    # Suas credenciais
    client = CVCRMSimple("halsten", mail, token)
    
    try:
        # Listar atendimentos
        print("=== ATENDIMENTOS ===")
        atendimentos = client.listar_atendimentos()
        print(f"Total de atendimentos: {len(atendimentos)}")
        
        if atendimentos:
            primeiro = atendimentos[0]
            print(f"Primeiro atendimento:")
            print(f"  ID: {primeiro.get('idatendimento')}")
            print(f"  Nome: {primeiro.get('nome')}")
            print(f"  Assunto: {primeiro.get('assunto')}")
            print(f"  Situação: {primeiro.get('situacao')}")
        
        # Listar tarefas
        print("\n=== TAREFAS ===")
        tarefas_response = client.listar_tarefas(limit=5)
        tarefas = tarefas_response.get('tarefas', [])
        total = tarefas_response.get('total', 0)
        
        print(f"Total de tarefas: {total}")
        print(f"Tarefas na página: {len(tarefas)}")
        
        if tarefas:
            primeira = tarefas[0]
            print(f"Primeira tarefa:")
            print(f"  ID: {primeira.get('idtarefa')}")
            print(f"  Situação: {primeira.get('situacao')}")
            print(f"  Responsável: {primeira.get('responsavel')}")
            
            # Buscar detalhes da primeira tarefa
            if primeira.get('idtarefa_criada'):
                print(f"\n=== DETALHES DA TAREFA {primeira['idtarefa_criada']} ===")
                tarefa_detalhes = client.buscar_tarefa_por_id(primeira['idtarefa_criada'])
                print(f"Observação: {tarefa_detalhes.get('observacao', 'Sem observação')}")
                
                atendimento = tarefa_detalhes.get('atendimento', {})
                if atendimento:
                    print(f"Atendimento relacionado:")
                    print(f"  ID: {atendimento.get('idatendimento')}")
                    print(f"  Cliente: {atendimento.get('cliente')}")
                    print(f"  Protocolo: {atendimento.get('protocolo')}")
        
        # Listar vendas
        print("\n=== VENDAS ===")
        try:
            vendas_response = client.listar_vendas(pagina=1, registros=3)
            vendas = vendas_response.get('dados', [])
            total_vendas = vendas_response.get('total_de_registros', 0)
            
            print(f"Total de vendas: {total_vendas}")
            print(f"Vendas na página: {len(vendas)}")
            
            if vendas:
                primeira_venda = vendas[0]
                print(f"Primeira venda:")
                print(f"  ID Reserva: {primeira_venda.get('idreserva')}")
                print(f"  Cliente: {primeira_venda.get('cliente')}")
                print(f"  Empreendimento: {primeira_venda.get('empreendimento')}")
                print(f"  Valor: R$ {primeira_venda.get('valor_contrato', 0):,.2f}")
                print(f"  Data Venda: {primeira_venda.get('data_venda')}")
        except Exception as e:
            print(f"❌ Vendas não disponíveis: {str(e)[:100]}")
            print("   (CVDW requer permissão especial)")
        
        # Listar empreendimentos
        print("\n=== EMPREENDIMENTOS ===")
        empreendimentos = client.listar_empreendimentos()
        
        print(f"Total de empreendimentos: {len(empreendimentos)}")
        
        if empreendimentos:
            primeiro_emp = empreendimentos[0]
            print(f"Primeiro empreendimento:")
            print(f"  ID: {primeiro_emp.get('idempreendimento')}")
            print(f"  Nome: {primeiro_emp.get('nome')}")
            print(f"  Cidade: {primeiro_emp.get('cidade')}")
            print(f"  Unidades disponíveis: {primeiro_emp.get('quantidade_unidades_disponiveis', 0)}")
            
            # Listar unidades do primeiro empreendimento
            id_emp = primeiro_emp.get('idempreendimento')
            if id_emp:
                print(f"\n=== UNIDADES DO EMPREENDIMENTO {id_emp} ===")
                unidades = client.listar_unidades_empreendimento(id_emp)
                
                print(f"Unidades encontradas: {len(unidades)}")
                
                if unidades:
                    primeira_unidade = unidades[0]
                    print(f"Primeira unidade:")
                    print(f"  ID: {primeira_unidade.get('idunidade')}")
                    print(f"  Nome: {primeira_unidade.get('nome')}")
                    print(f"  Área privativa: {primeira_unidade.get('area_privativa')} m²")
                    print(f"  Andar: {primeira_unidade.get('andar')}")
        
        # Listar clientes
        print("\n=== CLIENTES/PESSOAS ===")
        try:
            # Tenta listar via CVDW
            clientes_response = client.listar_clientes(pagina=1, registros=3)
            clientes = clientes_response.get('dados', [])
            total_clientes = clientes_response.get('total_de_registros', 0)
            
            print(f"Total de clientes (CVDW): {total_clientes}")
            print(f"Clientes na página: {len(clientes)}")
            
            if clientes:
                primeiro_cliente = clientes[0]
                print(f"Primeiro cliente:")
                print(f"  ID: {primeiro_cliente.get('idpessoa')}")
                print(f"  Nome: {primeiro_cliente.get('nome')}")
                print(f"  Documento: {primeiro_cliente.get('documento')}")
                print(f"  Situação: {primeiro_cliente.get('situacao')}")
                print(f"  Cidade: {primeiro_cliente.get('cidade')}")
                
        except Exception as e:
            print(f"❌ CVDW pessoas não disponível: {str(e)[:100]}")
            
            # Tenta buscar cliente específico pelo CVIO
            print("\n=== BUSCA CLIENTE ESPECÍFICO ===")
            try:
                # Usa CPF de um cliente das vendas se disponível
                if 'vendas' in locals() and vendas.get('dados'):
                    primeira_venda = vendas['dados'][0]
                    cliente_nome = primeira_venda.get('cliente')
                    
                    # Tenta buscar por documento fictício para testar
                    print(f"Tentando buscar cliente por documento...")
                    cliente_teste = client.buscar_cliente_por_documento('111.111.111-11')
                    print(f"Resultado: {cliente_teste}")
                    
            except Exception as e2:
                print(f"❌ Busca específica falhou: {str(e2)[:100]}")
                print("   (Endpoint /api/cvio/cliente requer documento/email/telefone específico)")
        
    except Exception as e:
        print(f"Erro: {e}")