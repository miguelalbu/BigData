import pandas as pd
import os

# Carregar o CSV
arquivo_villagers = "C:/Users/migue/OneDrive/Área de Trabalho/BigData/AnimalProject/dataAnimal/villagers.csv"

df_villagers = pd.read_csv(arquivo_villagers)

# Selecionar colunas relevantes
df_dim_villagers = df_villagers[[
    "Unique Entry ID", "Name", "Species", "Gender", "Personality", "Hobby",
    "Birthday", "Favorite Song", "Style 1", "Style 2",
    "Color 1", "Color 2"
]].copy()  # Criar cópia para evitar avisos



# Criar a pasta "processed" se ela não existir
os.makedirs("processed", exist_ok=True)

# Salvar o arquivo
df_dim_villagers.to_csv("processed/dim_villagers.csv", index=False)


# Renomear colunas
df_dim_villagers.rename(columns={
    "Unique Entry ID": "villager_id",
    "Name": "name",
    "Species": "species",
    "Gender": "gender",
    "Personality": "personality",
    "Hobby": "hobby",
    "Birthday": "birthday",
    "Favorite Song": "favorite_song",
    "Style 1": "style_1",
    "Style 2": "style_2",
    "Color 1": "color_1",
    "Color 2": "color_2"
}, inplace=True)

# Remover valores nulos
df_dim_villagers.dropna(inplace=True)

# Exibir a tabela dimensão
print(df_dim_villagers.head())

# Criar a tabela fato
df_fato_popularidade = df_dim_villagers.groupby(["species", "personality", "hobby"]).agg(
    qtd_villagers=("villager_id", "count")
).reset_index()

# Exibir o resultado
print(df_fato_popularidade.head())

# Salvar as tabelas processadas
df_dim_villagers.to_csv("processed/dim_villagers.csv", index=False)
df_fato_popularidade.to_csv("processed/fato_popularidade.csv", index=False)

print("✅ Dados salvos em 'processed/'")
