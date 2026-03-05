import requests
import os
from msal import ConfidentialClientApplication

TENANT_ID = os.environ.get('SP_TENANT_ID')
CLIENT_ID = os.environ.get('SP_CLIENT_ID')
CLIENT_SECRET = os.environ.get('SP_CLIENT_SECRET')
DRIVE_ID = os.environ.get('SP_DRIVE_ID')
PASTA_DESTINO = os.environ.get('SP_PASTA_DESTINO', '1.7.TI/1.7.4 CONSULTAS/CS')

def gerar_token():
    """
    Autentica via MSAL e retorna o token de acesso.
    """
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    scope = ["https://graph.microsoft.com/.default"]

    app = ConfidentialClientApplication(
        CLIENT_ID,
        authority=authority,
        client_credential=CLIENT_SECRET
    )

    result = app.acquire_token_for_client(scopes=scope)

    if "access_token" in result:
        print("✅ Token gerado com sucesso!")
        return result['access_token']
    else:
        print("❌ Erro ao obter token:", result.get("error_description"))
        return None

token = gerar_token()

def listar_sites_e_drives(token, site_search="1.HALSTEN"):
    """
    Usa o token para buscar sites e listar os drives de cada site encontrado.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    graph_api_url = f"https://graph.microsoft.com/v1.0/sites?search={site_search}"
    response = requests.get(graph_api_url, headers=headers)

    if response.status_code == 200:
        sites = response.json().get("value", [])
        for site in sites:
            print(f"🔹 Site encontrado: {site['name']} (ID: {site['id']})")
            site_id = site['id']

            # Lista os drives (bibliotecas de documentos)
            drives_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
            drives_response = requests.get(drives_url, headers=headers)
            if drives_response.status_code == 200:
                drives = drives_response.json().get("value", [])
                for drive in drives:
                    print(f"📁 Drive: {drive['name']} (ID: {drive['id']})")
            else:
                print("❌ Erro ao listar drives:", drives_response.text)
    else:
        print("❌ Erro ao consultar sites:", response.text)

def upload_arquivo_sharepoint(token, caminho_local):
    """
    Sobe um arquivo local para a pasta definida no SharePoint.
    """
    nome_arquivo = os.path.basename(caminho_local)

    upload_url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/root:/{PASTA_DESTINO}/{nome_arquivo}:/content"
    headers = {
        "Authorization": f"Bearer {token}"
    }

    with open(caminho_local, "rb") as arquivo:
        response = requests.put(upload_url, headers=headers, data=arquivo)

    if response.status_code in [200, 201]:
        print(f"✅ Arquivo '{nome_arquivo}' enviado com sucesso para '{PASTA_DESTINO}'!")
    else:
        print(f"❌ Erro no upload: {response.status_code} - {response.text}")

def listar_drives_de_um_site(token, site_id):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        drives = response.json().get("value", [])
        for drive in drives:
            print(f"📁 Drive: {drive['name']} (ID: {drive['id']})")
    else:
        print(f"❌ Erro ao listar drives: {response.status_code} - {response.text}")

def buscar_site_id(token, site_path):
    """
    Retorna o site_id de um site SharePoint com base no path (ex: /sites/NOME).
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    url = f"https://graph.microsoft.com/v1.0/sites/grupoinvestcorp.sharepoint.com:{site_path}"

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        site = response.json()
        print(f"✅ Site encontrado: {site['name']} (ID: {site['id']})")
        return site["id"]
    else:
        print(f"❌ Erro ao buscar site ID: {response.status_code} - {response.text}")
        return None

def listaDiretorioHalsten():
    listar_drives_de_um_site(token, buscar_site_id(token, '/sites/1.HALSTEN'))

#upload_arquivo_sharepoint(token, 'semaforico1111.csv')