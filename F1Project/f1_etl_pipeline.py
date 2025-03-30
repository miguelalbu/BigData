from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, concat
import os

def main():
    # 1. Configuração para Windows - Ignora o aviso do winutils
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--master local[2] pyspark-shell'
    
    # 2. Configuração do Spark
    spark = SparkSession.builder \
        .appName("F1WinnersAnalysis") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    # 3. Definir caminhos corretos - AJUSTE ESTE CAMINHO!
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, "data/")
    
    # Verificar se a pasta data existe
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Pasta 'data' não encontrada em {script_dir}")
    
    # 4. Carregar arquivos com verificação
    csv_files = {
        "drivers": "drivers.csv",
        "races": "races.csv",
        "results": "results.csv"
    }
    
    dfs = {}
    for name, file in csv_files.items():
        full_path = os.path.join(data_path, file)
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"Arquivo {file} não encontrado em {data_path}")
        dfs[name] = spark.read.csv(full_path, header=True, inferSchema=True)
    
    # 5. Processamento dos dados
    print("Processando dados...")
    
    # Criar nome completo do piloto
    drivers_with_name = dfs["drivers"].withColumn(
        "driver_name", 
        concat(col("forename"), col("surname"))
    )
    
    # Juntar todos os dados
    joined_data = dfs["results"].join(
        dfs["races"], 
        "raceId"
    ).join(
        drivers_with_name,
        "driverId"
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

    # 6. Exibir e salvar resultados
    print("\nTop 10 pilotos com mais vitórias:")
    winners_count.show(10, truncate=False)
    
    # Criar pasta de output se não existir
    output_path = os.path.join(script_dir, "output")
    os.makedirs(output_path, exist_ok=True)
    
    # Salvar em CSV único
    winners_count.coalesce(1).write.csv(
        os.path.join(output_path, "winners_count"),
        mode="overwrite",
        header=True
    )
    
    print(f"\nResultados salvos em: {os.path.join(output_path, 'winners_count')}")

if __name__ == "__main__":
    main()