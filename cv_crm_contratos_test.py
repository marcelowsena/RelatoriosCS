#!/usr/bin/env python3
"""
Script para testar o endpoint de contratos da reserva do CV CRM
GET /api/cvio/reserva/{id}/contratos
"""

import requests
import time
import json
from typing import Dict, Optional

class CVCRMContratosTest:
    """Teste específico para endpoint de contratos da reserva"""
    
    def __init__(self, subdomain: str, email: str, token: str):
        self.base_url = f"https://{subdomain}.cvcrm.com.br"
        self.headers = {
            'accept': 'application/json',
            'email': email,
            'token': token
        }
        
    def buscar_contratos_reserva(self, id_reserva: int) -> Dict:
        """
        Busca contratos de uma reserva específica
        
        Args:
            id_reserva: ID da reserva para buscar contratos
            
        Returns:
            Dict com dados dos contratos
        """
        endpoint = f"/api/cvio/reserva/{id_reserva}/contratos"
        url = f"{self.base_url}{endpoint}"
        
        print(f"Fazendo requisição: {url}")
        
        try:
            response = requests.get(url, headers=self.headers)
            
            print(f"Status Code: {response.status_code}")
            print(f"Headers de resposta: {dict(response.headers)}")
            
            if response.status_code == 200:
                data = response.json()
                print("Resposta da API:")
                print(json.dumps(data, indent=2, ensure_ascii=False))
                return data
                
            else:
                print(f"Erro na requisição:")
                print(f"Status: {response.status_code}")
                print(f"Resposta: {response.text}")
                return {"erro": response.status_code, "mensagem": response.text}
                
        except requests.exceptions.RequestException as e:
            print(f"Erro de conexão: {e}")
            return {"erro": "conexao", "mensagem": str(e)}
            
        except json.JSONDecodeError as e:
            print(f"Erro ao decodificar JSON: {e}")
            print(f"Resposta raw: {response.text}")
            return {"erro": "json", "mensagem": str(e)}
            
    def testar_multiplas_reservas(self, lista_ids: list) -> Dict:
        """
        Testa o endpoint com múltiplas reservas
        
        Args:
            lista_ids: Lista de IDs de reserva para testar
            
        Returns:
            Dict com resultados dos testes
        """
        resultados = {}
        
        print(f"Testando {len(lista_ids)} reservas...")
        
        for i, id_reserva in enumerate(lista_ids, 1):
            print(f"\n--- Teste {i}/{len(lista_ids)}: Reserva ID {id_reserva} ---")
            
            resultado = self.buscar_contratos_reserva(id_reserva)
            resultados[id_reserva] = resultado
            
            # Rate limiting - 3 segundos entre requests
            if i < len(lista_ids):
                print("Aguardando 3 segundos...")
                time.sleep(3)
                
        return resultados
        
    def analisar_estrutura_resposta(self, resposta: Dict):
        """
        Analisa a estrutura da resposta para entender o formato
        
        Args:
            resposta: Resposta da API para analisar
        """
        print("\n=== ANÁLISE DA ESTRUTURA ===")
        
        if "erro" in resposta:
            print(f"Erro detectado: {resposta}")
            return
            
        if isinstance(resposta, dict):
            print("Tipo: Objeto (Dict)")
            print(f"Chaves principais: {list(resposta.keys())}")
            
            for chave, valor in resposta.items():
                tipo_valor = type(valor).__name__
                
                if isinstance(valor, list):
                    print(f"  {chave}: Lista com {len(valor)} itens")
                    if len(valor) > 0:
                        primeiro_item = valor[0]
                        if isinstance(primeiro_item, dict):
                            print(f"    Chaves do primeiro item: {list(primeiro_item.keys())}")
                elif isinstance(valor, dict):
                    print(f"  {chave}: Objeto com chaves: {list(valor.keys())}")
                else:
                    valor_preview = str(valor)[:50] if valor else "null"
                    print(f"  {chave}: {tipo_valor} = {valor_preview}")
                    
        elif isinstance(resposta, list):
            print(f"Tipo: Lista com {len(resposta)} itens")
            if len(resposta) > 0:
                primeiro = resposta[0]
                print(f"Tipo do primeiro item: {type(primeiro).__name__}")
                if isinstance(primeiro, dict):
                    print(f"Chaves: {list(primeiro.keys())}")
        else:
            print(f"Tipo inesperado: {type(resposta).__name__}")
            print(f"Valor: {resposta}")

def main():
    """Função principal para executar os testes"""
    print("=== TESTE CONTRATOS DA RESERVA ===")
    print("Endpoint: GET /api/cvio/reserva/{id}/contratos")
    
    # Configurações (suas credenciais)
    tester = CVCRMContratosTest(
        subdomain="halsten",
        email="francisco.neto@halsten.com.br", 
        token="92ff848090bc87737548ba1f3686870e1c5cbd7c"
    )
    
    print("Configurações:")
    print(f"  Base URL: {tester.base_url}")
    print(f"  Email: {tester.headers['email']}")
    
    # IDs de reserva para testar
    # Baseado nos dados das suas vendas, vamos testar alguns IDs
    ids_para_testar = [
        3224,    # ID da primeira venda que apareceu no teste anterior
        ]
    
    print(f"\nIDs de reserva para testar: {ids_para_testar}")
    
    # Teste individual primeiro
    print(f"\n=== TESTE INDIVIDUAL ===")
    primeiro_id = ids_para_testar[0]
    resultado = tester.buscar_contratos_reserva(primeiro_id)
    
    # Analisa a estrutura da primeira resposta
    tester.analisar_estrutura_resposta(resultado)
    
    # Pergunta se quer continuar com mais testes
    continuar = input(f"\nDeseja testar os outros {len(ids_para_testar)-1} IDs? (s/n): ")
    
    if continuar.lower().startswith('s'):
        print(f"\n=== TESTE MÚLTIPLO ===")
        resultados = tester.testar_multiplas_reservas(ids_para_testar[1:])
        
        # Resumo dos resultados
        print(f"\n=== RESUMO DOS TESTES ===")
        sucessos = 0
        erros = 0
        
        for id_reserva, resultado in resultados.items():
            if "erro" not in resultado:
                print(f"✅ Reserva {id_reserva}: OK")
                sucessos += 1
            else:
                erro_tipo = resultado.get("erro", "desconhecido")
                print(f"❌ Reserva {id_reserva}: Erro {erro_tipo}")
                erros += 1
        
        print(f"\nResultado final: {sucessos} sucessos, {erros} erros")
        
    else:
        print("Teste finalizado.")
    
    # Sugestões baseadas no resultado
    if "erro" not in resultado:
        print(f"\n=== PRÓXIMOS PASSOS ===")
        print("✅ Endpoint funcionando! Você pode:")
        print("1. Adicionar este método ao cv_crm_simple.py")
        print("2. Integrar com análise de contratos")
        print("3. Usar para relatórios de vendas")
        
        print(f"\nCódigo para adicionar ao CVCRMSimple:")
        print("""
def buscar_contratos_reserva(self, id_reserva: int) -> Dict:
    \"\"\"Busca contratos de uma reserva específica\"\"\"
    return self._make_request(f'/api/cvio/reserva/{id_reserva}/contratos')
""")
    else:
        print(f"\n=== DIAGNÓSTICO ===")
        if resultado.get("erro") == 403:
            print("❌ Sem permissão para acessar contratos")
            print("   Contate o administrador para liberar acesso")
        elif resultado.get("erro") == 404:
            print("❌ Reserva não encontrada ou endpoint incorreto")
            print("   Verifique se os IDs de reserva existem")
        elif resultado.get("erro") == 401:
            print("❌ Erro de autenticação")
            print("   Verifique email e token")
        else:
            print("❌ Erro inesperado - veja detalhes acima")

if __name__ == "__main__":
    main()