from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Iniciar a SparkSession
spark = SparkSession.builder \
    .appName("ETL Pipeline") \
    .getOrCreate()

# Carregar os dados (Extract) - Lendo o arquivo CSV
df_villagers_spark = spark.read.csv("C:/Users/migue/OneDrive/Área de Trabalho/BigData/dataAnimal/villagers.csv", header=True)



# Exibir as primeiras linhas do DataFrame carregado (apenas para verificação)
df_villagers_spark.show(5)

# Transformação dos dados (Transform) - Selecionando e renomeando as colunas
df_transformed = df_villagers_spark.select(
    col("Unique Entry ID").alias("villager_id"),
    col("Name").alias("name"),
    col("Species").alias("species"),
    col("Gender").alias("gender"),
    col("Personality").alias("personality"),
    col("Hobby").alias("hobby"),
    col("Birthday").alias("birthday"),
    col("Favorite Song").alias("favorite_song"),
    col("Style 1").alias("style_1"),
    col("Style 2").alias("style_2"),
    col("Color 1").alias("color_1"),
    col("Color 2").alias("color_2")
)

# Exibir as primeiras linhas do DataFrame transformado
df_transformed.show(5)

# Salvar os dados transformados (Load) - Em formato CSV
df_transformed.write.csv("processed/dim_villagers_spark.csv", header=True, mode="overwrite")

# Alternativa em Parquet (mais eficiente)
# df_transformed.write.parquet("processed/dim_villagers_spark.parquet", mode="overwrite")

# Fechar a sessão do Spark
spark.stop()
