from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, concat



def main():
    # 1. Configuração do Spark
    spark = SparkSession.builder \
        .appName("F1WinnersAnalysis") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    # 2. Extração dos dados
    print("Carregando arquivos CSV...")
    data_path = "F1Project/DB"
    
    drivers_df = spark.read.csv(f"{data_path}drivers.csv", header=True, inferSchema=True)
    races_df = spark.read.csv(f"{data_path}races.csv", header=True, inferSchema=True)
    results_df = spark.read.csv(f"{data_path}results.csv", header=True, inferSchema=True)

    # 3. Transformação dos dados
    print("Processando vitórias...")
    
    # Criar nome completo do piloto
    drivers_with_name = drivers_df.withColumn(
        "driver_name", 
        concat(col("forename"), col("surname"))
    )
    
    # Juntar todos os dados
    joined_data = results_df.join(
        races_df, 
        results_df.raceId == races_df.raceId
    ).join(
        drivers_with_name,
        results_df.driverId == drivers_with_name.driverId
    )
    
    # Filtrar apenas vitórias (position = 1)
    victories = joined_data.filter(col("position") == 1)
    
    # Contar vitórias por piloto
    winners_count = victories.groupBy(
        "driverId", 
        "driver_name"
    ).agg(
        count("*").alias("total_wins")
    ).orderBy(
        col("total_wins").desc()
    )

    # 4. Exibir e salvar resultados
    print("\nTop 10 pilotos com mais vitórias:")
    winners_count.show(10, truncate=False)
    
    # Salvar em CSV
    output_path = "output/winners_count"
    winners_count.write.csv(output_path, mode="overwrite", header=True)
    
    print(f"\nResultados salvos em: {output_path}")

if __name__ == "__main__":
    main()