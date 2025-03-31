# Pipeline de ETL - Análise de Vitórias na Fórmula 1

## 📋 Visão Geral
Pipeline de ETL que processa dados históricos da F1 (1950-2020) para identificar os pilotos mais vitoriosos. Desenvolvido em duas versões:

- **Versão Pandas**: Processamento local simples
- **Versão PySpark**: Pipeline distribuído escalável

## 🛠️ Tecnologias Utilizadas
- Python 3.12
- Pandas (para versão local)
- PySpark 3.5.5 (para versão distribuída)
- Hadoop (winutils para ambiente Windows)

## 📂 Estrutura do Projeto
```
F1Project/
├── data/
│ ├── drivers.csv
│ ├── races.csv
│ └── results.csv
├── src/
│ ├── local_etl.py # Versão Pandas
│ └── spark_etl.py # Versão PySpark
├── output/
│ └── winners_count/ # Resultados em CSV
└── README.md
```

## 🔍 Como Executar

### Versão Local (Pandas)
```bash
python src/local_etl.py
spark-submit src/spark_etl.py
```
# 📊 Resultados Esperados
Saída em output/winners_count:
```
driver_name,wins
Lewis Hamilton,105
Michael Schumacher,91
Max Verstappen,63
Sebastian Vettel,53
Alain Prost,51
[...]
```

# Funcionamento do Código

## Versão Pandas
- Extrai dados dos CSVs

- Filtra apenas posições = 1 (vitórias)

- Agrupa por piloto e conta vitórias

- Combina com dados de pilotos para obter nomes

## Versão PySpark
- Configura ambiente Spark

- Carrega dados com schema inference

- Realiza joins entre tabelas

- Aplica transformações distribuídas

- Salva resultados otimizados
