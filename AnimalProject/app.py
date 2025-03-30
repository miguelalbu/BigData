import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Carregar o dataset
df_villagers = pd.read_csv("C:\Users\migue\OneDrive\Área de Trabalho\BigData\AnimalProject\dataAnimal/villagers.csv")

# Título do painel
st.title('Análise de Dados dos Vilarejos')

# Mostrar as primeiras linhas do dataset
st.subheader('Primeiras Linhas dos Dados')
st.write(df_villagers.head())

# Exibir estatísticas descritivas
st.subheader('Estatísticas Descritivas')
st.write(df_villagers.describe())

# Mostrar a distribuição de idades
st.subheader('Distribuição de Idades')
fig, ax = plt.subplots()
sns.histplot(df_villagers['idade'], kde=True, ax=ax)
st.pyplot(fig)

# Exemplo de gráfico de barras
st.subheader('Contagem de Vilarejos por Tipo')
fig, ax = plt.subplots()
sns.countplot(data=df_villagers, x='tipo_vilarejo', ax=ax)
st.pyplot(fig)
