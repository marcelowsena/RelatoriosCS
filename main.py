from SharepointCRUD.main import upload_arquivo_sharepoint, token
from exportacao import main as salvaArq
import os

salvaArq()
csvFiles = os.listdir('powerbi_data')

for f in csvFiles:
    if '.csv' in f:
        upload_arquivo_sharepoint(token, os.path.join('powerbi_data', f))
