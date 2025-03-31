import pandas as pd

# Carregar os dados
results = pd.read_csv('F1Project/data/results.csv')
drivers = pd.read_csv('F1Project/data/drivers.csv')

# Filtrar apenas vitórias (position = 1)
victories = results[results['position'] == '1']

# Contar vitórias por driverId
victory_count = victories['driverId'].value_counts().reset_index()
victory_count.columns = ['driverId', 'wins']

# Adicionar nomes dos pilotos
victory_count = victory_count.merge(drivers, on='driverId')
victory_count['driver_name'] = victory_count['forename'] + ' ' + victory_count['surname']

# Ordenar pelos maiores vencedores
top_winners = victory_count[['driver_name', 'wins']].sort_values('wins', ascending=False)
print(top_winners.head(10))