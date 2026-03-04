from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import ElementClickInterceptedException, TimeoutException
from datetime import datetime, date
import time
import json
import os
from dotenv import load_dotenv

def extrair_logs_acesso_cv(situacao="C"):
    load_dotenv()
    
    usuario = os.getenv('CV_EMAIL')
    senha = os.getenv('CV_SENHA')
    
    if not usuario or not senha:
        raise ValueError("Credenciais não encontradas no arquivo .env")
    
    ano_atual = date.today().year
    data_inicio = f"01/01/{ano_atual}"
    data_hoje = datetime.now().strftime("%d/%m/%Y")
    
    options = webdriver.ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-web-security')
    options.add_argument('--allow-running-insecure-content')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    download_dir = os.path.abspath("downloads")
    os.makedirs(download_dir, exist_ok=True)
    
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True
    }
    options.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    try:
        print(f"Iniciando automação...")
        print(f"Período: {data_inicio} até {data_hoje}")
        
        driver.get("https://halsten.cvcrm.com.br/gestor")
        
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "email")))

        try:
            select_painel = Select(driver.find_element(By.ID, "selectPainel"))
            select_painel.select_by_value("gestor")
        except Exception:
            pass  # elemento pode não existir dependendo da versão da página

        driver.find_element(By.ID, "email").send_keys(usuario)
        driver.find_element(By.ID, "senha").send_keys(senha)
        driver.find_element(By.CSS_SELECTOR, ".--btn-acessar").click()
        
        WebDriverWait(driver, 10).until(
            lambda d: "login" not in d.current_url.lower()
        )
        
        print("Login realizado com sucesso")
        print(f"URL pós-login: {driver.current_url}")

        # CV CRM pode redirecionar para meusdados em IPs novos — tentar até 3 vezes
        url_relatorio = "https://halsten.cvcrm.com.br/gestor/relatorios/pessoas_logs_acesso"
        for tentativa_nav in range(3):
            driver.get(url_relatorio)
            time.sleep(4)
            url_atual = driver.current_url
            print(f"Tentativa {tentativa_nav + 1} - URL: {url_atual}")
            if "relatorios" in url_atual or "pessoas_logs_acesso" in url_atual:
                break
            if "meusdados" in url_atual:
                print("Redirecionado para meusdados, aguardando e tentando novamente...")
                try:
                    driver.execute_script("window.onbeforeunload = null;")
                except:
                    pass
                time.sleep(2)
        else:
            raise Exception(f"CV CRM bloqueou acesso ao relatório — URL final: {url_atual}")

        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.ID, "situacao_pessoa")))
        
        select_situacao = Select(driver.find_element(By.ID, "situacao_pessoa"))
        select_situacao.select_by_value(situacao)
        
        campo_data_de = driver.find_element(By.ID, "form_data_cad_de")
        campo_data_de.clear()
        campo_data_de.send_keys(data_inicio)
        
        campo_data_ate = driver.find_element(By.ID, "form_data_cad_ate")
        campo_data_ate.clear()
        campo_data_ate.send_keys(data_hoje)
        
        print("Preenchimento realizado, aguardando botão ficar clicável...")
        
        botao_gerar = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "gerar_html"))
        )
        
        # Múltiplas tentativas de clique
        janelas_iniciais = len(driver.window_handles)
        clique_sucesso = False
        
        for tentativa in range(5):
            print(f"Tentativa {tentativa + 1} de clique...")
            
            try:
                # Scroll para o elemento
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", botao_gerar)
                time.sleep(0.5)
                
                # Tentar clique normal
                botao_gerar.click()
                time.sleep(2)
                
                # Verificar se nova janela abriu
                if len(driver.window_handles) > janelas_iniciais:
                    clique_sucesso = True
                    break
                    
            except ElementClickInterceptedException:
                print("Elemento interceptado, tentando JavaScript...")
                
            # Tentar JavaScript click
            try:
                driver.execute_script("arguments[0].click();", botao_gerar)
                time.sleep(2)
                
                if len(driver.window_handles) > janelas_iniciais:
                    clique_sucesso = True
                    break
                    
            except:
                print("JavaScript click falhou, tentando ActionChains...")
            
            # Tentar ActionChains
            try:
                ActionChains(driver).move_to_element(botao_gerar).click().perform()
                time.sleep(2)
                
                if len(driver.window_handles) > janelas_iniciais:
                    clique_sucesso = True
                    break
                    
            except:
                print(f"ActionChains falhou na tentativa {tentativa + 1}")
            
            time.sleep(1)
        
        if not clique_sucesso:
            print("FALHA: Não conseguiu clicar no botão após 5 tentativas")
            print("Aguarde 10 segundos para clicar manualmente...")
            time.sleep(10)
        
        # Aguardar nova janela (manual ou automatica)
        try:
            WebDriverWait(driver, 30).until(
                lambda d: len(d.window_handles) > janelas_iniciais
            )
        except TimeoutException:
            print("Timeout aguardando nova janela - verifique se abriu manualmente")
            return None
        
        janelas_atuais = driver.window_handles
        print(f"Nova janela detectada! Total de janelas: {len(janelas_atuais)}")
        
        driver.switch_to.window(janelas_atuais[-1])
        print(f"Mudou para nova janela: {driver.current_url}")
        
        WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='submit'][value='Baixar em planilha']"))
        )
        
        print("Página do relatório carregada, procurando botão de download...")
        
        botao_download = driver.find_element(By.CSS_SELECTOR, "input[type='submit'][value='Baixar em planilha']")
        
        if botao_download:
            print("Botão de download encontrado, iniciando download...")
            botao_download.click()
            time.sleep(3)
        else:
            print("Botão de download não encontrado")
        
        return {
            "dados": 'dados',
            "periodo": f"{data_inicio} até {data_hoje}",
            "total_registros": 'len(dados)',
            "download_realizado": botao_download is not None
        }
        
    except Exception as e:
        print(f"Erro na automação: {e}")
        print(f"URL atual: {driver.current_url}")
        print(f"Número de janelas: {len(driver.window_handles)}")
        return None
    finally:
        driver.quit()

def aguardar_download(diretorio="downloads", timeout=30):
    import glob
    tempo_inicial = time.time()
    
    print("Aguardando download do arquivo CSV...")
    while time.time() - tempo_inicial < timeout:
        arquivos_csv = glob.glob(os.path.join(diretorio, "*.csv"))
        if arquivos_csv:
            arquivo_mais_recente = max(arquivos_csv, key=os.path.getctime)
            print(f"Arquivo CSV baixado: {arquivo_mais_recente}")
            return arquivo_mais_recente
        time.sleep(1)
    
    print("Timeout: arquivo CSV não foi baixado")
    return None

if __name__ == "__main__":
    try:
        resultado = extrair_logs_acesso_cv()
        
        if resultado:
            print(f"\n=== RELATÓRIO CONCLUÍDO ===")
            print(f"Período: {resultado['periodo']}")
            print(f"Total de registros: {resultado['total_registros']}")
            print(f"Download realizado: {resultado['download_realizado']}")
            
            arquivo_csv = aguardar_download()
            if arquivo_csv:
                print(f"Planilha CSV: {arquivo_csv}")
            
            print("Automação concluída com sucesso!")
        else:
            print("Falha na extração dos dados")
            
    except Exception as e:
        print(f"Erro: {e}")