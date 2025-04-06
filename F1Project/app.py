import pandas as pd

# Carregar os dados
results = pd.read_csv('F1Project/data/results.csv')
drivers = pd.read_csv('F1Project/data/drivers.csv')
races = pd.read_csv('F1Project/data/races.csv')

# Filtrar apenas vitórias (position = 1)
victories = results[results['position'] == '1']

# Juntar com os dados de corridas para pegar nome e ano
victories = victories.merge(races[['raceId', 'name', 'year']], on='raceId', how='left')

# Contar vitórias por driverId
victory_count = victories['driverId'].value_counts().reset_index()
victory_count.columns = ['driverId', 'wins']

# Adicionar nomes dos pilotos
victory_count = victory_count.merge(drivers, on='driverId')
victory_count['driver_name'] = victory_count['forename'] + ' ' + victory_count['surname']

# Ordenar pelos maiores vencedores
top_winners = victory_count[['driver_name', 'wins']].sort_values('wins', ascending=False)
print("Top 10 pilotos com mais vitórias:")
print(top_winners.head(10))

# ==============================
# Aqui entra a parte adicional
# ==============================

# Lista de vitórias com nomes dos pilotos e corridas
victories = victories.merge(drivers, on='driverId')
victories['driver_name'] = victories['forename'] + ' ' + victories['surname']

victory_details = victories[['driver_name', 'year', 'name']].sort_values(['driver_name', 'year'])

print("\nExemplo de vitórias por piloto e corrida:")
print(victory_details.head(20))
