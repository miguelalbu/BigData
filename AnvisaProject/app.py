# import pandas as pd
# df = pd.read_csv(r"AnvisaProject\data\data.csv")


# print(df.columns)

# print(list(df.columns))

import pandas as pd

# Caminho para o arquivo original
df = pd.read_csv(r"C:\Users\migue\OneDrive\Área de Trabalho\BigData\AnvisaProject\data\data.csv")

# Converte colunas numéricas, se necessário (força números e ignora erros)
df["ANO_VENDA"] = pd.to_numeric(df["ANO_VENDA"], errors="coerce")
df["QTD_UNIDADE_FARMACOTECNICA"] = pd.to_numeric(df["QTD_UNIDADE_FARMACOTECNICA"], errors="coerce")

# Remove linhas com ano ausente ou anterior a 2018
df = df[df["ANO_VENDA"] >= 2018]

# Agrupa por ano e soma as quantidades
vendas_por_ano = df.groupby("ANO_VENDA")["QTD_UNIDADE_FARMACOTECNICA"].sum().reset_index()

# Renomeia colunas para clareza no Power BI
vendas_por_ano.columns = ["Ano", "Quantidade_Total_Vendida"]

# Exporta para novo CSV
vendas_por_ano.to_csv("vendas_por_ano.csv", index=False)

print("Arquivo 'vendas_por_ano.csv' gerado com sucesso!")
