import pandas as pd  

# Nome do arquivo (ajuste se necessário)
arquivo_villagers = "dataAnimal/villagers.csv"

# Carregar o CSV
df_villagers = pd.read_csv(arquivo_villagers)

# Exibir informações básicas
print(df_villagers.head())
print(df_villagers.info())

# Selecionar colunas relevantes
df_dim_villagers = df_villagers[[
    "Unique Entry ID", "Name", "Species", "Gender", "Personality", "Hobby",
    "Birthday", "Catchphrase", "Favorite Song", "Style 1", "Style 2",
    "Color 1", "Color 2", "Wallpaper", "Flooring"
]]

# Renomear colunas para um formato padronizado
df_dim_villagers = df_dim_villagers.rename(columns={
    "Unique Entry ID": "villager_id",
    "Name": "name",
    "Species": "species",
    "Gender": "gender",
    "Personality": "personality",
    "Hobby": "hobby",
    "Birthday": "birthday",
    "Catchphrase": "catchphrase",
    "Favorite Song": "favorite_song",
    "Style 1": "style_1",
    "Style 2": "style_2",
    "Color 1": "color_1",
    "Color 2": "color_2",
    "Wallpaper": "wallpaper",
    "Flooring": "flooring"
})

# Verificar o resultado
print(df_dim_villagers.head())
