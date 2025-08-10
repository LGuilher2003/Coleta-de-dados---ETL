import pandas as pd
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
from io import StringIO
from datetime import datetime, timedelta

CREDENCIAIS = 'credencial.json'
PLANILHA_ID = 'idplanilha'
EMAIL = 'SeuEmail'

def pegar_sp500():
    print("Obtendo lista de empresas do S&P 500...")
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    html = requests.get(url).text
    df = pd.read_html(StringIO(html), header=0)[0] 
    lista_simbolos = df['Symbol'].tolist() 
    return lista_simbolos

def enviar_para_sheets(df):
    print("Conectando ao Google Sheets...")
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENCIAIS, scope)
    gc = gspread.authorize(creds)
    try:
        planilha = gc.open_by_key(PLANILHA_ID)
        planilha.share(EMAIL, perm_type='user', role='writer')
        
        worksheet = planilha.sheet1
        worksheet.clear()
        
        dados = df.fillna('').values.tolist()
        cabecalhos = df.columns.tolist()
        
        worksheet.update([cabecalhos] + dados)
        print("Dados enviados com sucesso!")
    except Exception as e:
        print(f"Erro ao enviar para o Google Sheets: {str(e)}")
        raise

def main():
    start_date = (datetime.today() - timedelta(days=365)).strftime('%Y-%m-%d')
    end_date = datetime.today().strftime('%Y-%m-%d')
    lista_simbolo2 = pegar_sp500()
    lista_dicionario = []
    for simbolo in lista_simbolo2:
        stock = yf.Ticker(simbolo)
        df_historico = stock.history(start = start_date, end=end_date)
        quantidade_acoes  = stock.info.get('sharesOutstanding')
        df_historico['Marketcap'] = quantidade_acoes * df_historico['Close']
        media_marketcap = df_historico['Marketcap'].mean()
        lista_dicionario.append({
            'Tickers': simbolo,
            'MediaMarketcap': media_marketcap
        })
    df_final = pd.DataFrame(lista_dicionario)
    df_final.sort_values(by='MediaMarketcap', ascending=False, inplace=True)
    df_final.reset_index(drop=True, inplace=True)
    df_final = df_final.head(10)
    df_final['MediaMarketcap'] = df_final['MediaMarketcap'].apply(lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    enviar_para_sheets(df_final)
if __name__ == "__main__":
    main()
