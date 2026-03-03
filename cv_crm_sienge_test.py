#!/usr/bin/env python3
"""
Script para testar endpoint Sienge correto baseado na documentação
POST /api/v1/cvdw/reservasienge
"""

import requests
import json
from typing import Dict, Optional

class CVCRMSiengeFinal:
    def __init__(self, subdomain: str, email: str, token: str):
        self.base_url = f"https://{subdomain}.cvcrm.com.br"
        self.headers = {
            'accept': 'application/json',
            'content-type': 'application/json',
            'email': email,
            'token': token
        }
    
    def buscar_reservas_sienge(self, pagina: int = 1, registros_por_pagina: int = 30, 
                              a_partir_referencia: int = None, 
                              a_partir_data_referencia: str = None,
                              ate_data_referencia: str = None) -> Dict:
        """
        Busca reservas Sienge via POST conforme documentação
        
        Args:
            pagina: Página que deseja visualizar
            registros_por_pagina: Quantidade por página (máximo 500)
            a_partir_referencia: ID de referência para busca
            a_partir_data_referencia: Data de referência início (YYYY-MM-DD HH:MM:SS)
            ate_data_referencia: Data de referência fim (YYYY-MM-DD HH:MM:SS)
        """
        endpoint = "/api/v1/cvdw/reservasienge"
        url = f"{self.base_url}{endpoint}"
        
        # Corpo da requisição conforme documentação
        body = {
            "pagina": pagina,
            "registros_por_pagina": registros_por_pagina
        }
        
        if a_partir_referencia:
            body["a_partir_referencia"] = a_partir_referencia
        
        if a_partir_data_referencia:
            body["a_partir_data_referencia"] = a_partir_data_referencia
            
        if ate_data_referencia:
            body["ate_data_referencia"] = ate_data_referencia
        
        print(f"POST {url}")
        print(f"Headers: {self.headers}")
        print(f"Body: {json.dumps(body, indent=2)}")
        
        try:
            response = requests.post(url, headers=self.headers, json=body)
            
            print(f"\nStatus Code: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                
                print(f"Resposta recebida:")
                print(f"  Página: {data.get('pagina', 'N/A')}")
                print(f"  Registros: {data.get('registros', 'N/A')}")
                print(f"  Total de registros: {data.get('total_de_registros', 'N/A')}")
                print(f"  Total de páginas: {data.get('total_de_paginas', 'N/A')}")
                
                dados = data.get('dados', [])
                print(f"  Registros nesta página: {len(dados)}")
                
                if dados:
                    print(f"  Campos do primeiro registro:")
                    primeiro = dados[0]
                    for campo in primeiro.keys():
                        valor = primeiro[campo]
                        if isinstance(valor, str) and len(valor) > 50:
                            valor = valor[:50] + "..."
                        print(f"    {campo}: {valor}")
                
                return data
                
            else:
                print(f"Erro {response.status_code}:")
                print(response.text)
                return {"erro": response.status_code, "mensagem": response.text}
                
        except Exception as e:
            print(f"Erro: {e}")
            return {"erro": "exception", "mensagem": str(e)}
    
    def buscar_reserva_3224(self) -> Dict:
        """
        Tenta encontrar especificamente a reserva 3224
        """
        print("Procurando reserva 3224...")
        
        # Estratégia 1: Buscar por referência próxima
        print("\n--- Tentativa 1: Por referência próxima ---")
        resultado1 = self.buscar_reservas_sienge(a_partir_referencia=3220, registros_por_pagina=10)
        
        reserva_encontrada = self._procurar_reserva_nos_dados(resultado1, 3224)
        if reserva_encontrada:
            return reserva_encontrada
        
        # Estratégia 2: Buscar páginas sequenciais
        print("\n--- Tentativa 2: Páginas sequenciais ---")
        for pagina in range(1, 6):  # Testa primeiras 5 páginas
            print(f"Página {pagina}...")
            resultado = self.buscar_reservas_sienge(pagina=pagina, registros_por_pagina=50)
            
            if "erro" not in resultado:
                reserva_encontrada = self._procurar_reserva_nos_dados(resultado, 3224)
                if reserva_encontrada:
                    return reserva_encontrada
        
        # Estratégia 3: Por data (caso a reserva seja recente)
        print("\n--- Tentativa 3: Por data recente ---")
        resultado3 = self.buscar_reservas_sienge(
            a_partir_data_referencia="2024-01-01 00:00:00",
            registros_por_pagina=100
        )
        
        reserva_encontrada = self._procurar_reserva_nos_dados(resultado3, 3224)
        if reserva_encontrada:
            return reserva_encontrada
        
        return {"erro": "reserva_nao_encontrada"}
    
    def _procurar_reserva_nos_dados(self, resultado: Dict, id_procurado: int) -> Optional[Dict]:
        """
        Procura uma reserva específica nos dados retornados
        """
        if "erro" in resultado:
            return None
            
        dados = resultado.get('dados', [])
        
        for item in dados:
            # Campos possíveis que podem conter o ID da reserva
            campos_id = ['idreserva', 'id_reserva', 'idsienge_reserva', 'referencia']
            
            for campo in campos_id:
                valor = item.get(campo)
                if valor and str(valor) == str(id_procurado):
                    print(f"Reserva {id_procurado} encontrada!")
                    print(f"Campo: {campo} = {valor}")
                    print(json.dumps(item, indent=4, ensure_ascii=False))
                    return item
        
        return None

def main():
    print("=== TESTE FINAL SIENGE ===")
    print("Baseado na documentação oficial")
    
    tester = CVCRMSiengeFinal(
        subdomain="halsten",
        email="francisco.neto@halsten.com.br",
        token="92ff848090bc87737548ba1f3686870e1c5cbd7c"
    )
    
    # Teste 1: Busca básica
    print("\n=== TESTE 1: BUSCA BÁSICA ===")
    resultado = tester.buscar_reservas_sienge(pagina=1, registros_por_pagina=5)
    
    if "erro" not in resultado:
        print("Endpoint funcionando!")
        
        # Teste 2: Procurar reserva 3224
        print("\n=== TESTE 2: PROCURAR RESERVA 3224 ===")
        reserva_3224 = tester.buscar_reserva_3224()
        
        if "erro" not in reserva_3224:
            print("Reserva 3224 encontrada com sucesso!")
        else:
            print("Reserva 3224 não encontrada nos dados Sienge")
            
        # Mostrar estrutura dos dados
        dados = resultado.get('dados', [])
        if dados:
            print(f"\n=== ESTRUTURA DOS DADOS SIENGE ===")
            primeiro = dados[0]
            print("Campos disponíveis:")
            for campo, valor in primeiro.items():
                tipo = type(valor).__name__
                print(f"  {campo}: {tipo}")
        
        print(f"\n=== CÓDIGO PARA INTEGRAÇÃO ===")
        print("""
# Adicionar ao CVCRMSimple:
def buscar_reservas_sienge(self, pagina=1, registros=30):
    url = f"{self.base_url}/api/v1/cvdw/reservasienge"
    body = {"pagina": pagina, "registros_por_pagina": registros}
    response = requests.post(url, headers=self.headers, json=body)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Erro {response.status_code}: {response.text}")
        """)
        
    else:
        erro = resultado.get("erro", "desconhecido")
        print(f"Endpoint não funcionou - Erro: {erro}")
        
        if erro == 401:
            print("Problema de autenticação")
        elif erro == 403:
            print("Sem permissão para CVDW/Sienge")
        elif erro == 404:
            print("Endpoint não existe")
        else:
            print("Verifique se integração Sienge está habilitada")

if __name__ == "__main__":
    main()