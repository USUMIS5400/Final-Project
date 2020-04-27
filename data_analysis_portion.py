from pandas import DataFrame
import pymongo
import matplotlib.pyplot as plt

client = pymongo.MongoClient(
	"mongodb+srv://test_user:test_password@olympic-data-qfgy4.mongodb.net/test?retryWrites=true&w=majority")
db = client['MIS-5400-Data-Persistence']
collection_name = 'Olympic-Medalists'
olympic_medals_collection = db[collection_name]
suppressed_columns = {'_id': 0}

countries = [
	'China',
	'Hungary',
    'United States',
    'Soviet Union',
    'United Kingdom',
    'France',
    'Germany',
    'Italy',
    'Australia',
    'Sweden'
    ]

country_colors = {
	'United States': 'navy',
	'Soviet Union': "crimson",
	'Germany': 'forestgreen',
	'United Kingdom': 'lightsalmon',
	'France': 'cornflowerblue',
	'Italy': 'gold',
	'China': 'darkviolet',
	'Australia': 'mediumturquoise',
	'Sweden': 'bisque',
	'Hungary': 'saddlebrown',
}

country_colors.setdefault('gainsboro')
medals = ['Silver', 'Bronze', 'Gold']
medal_colors = {'Bronze': 'chocolate', 'Silver': 'silver', 'Gold': 'gold'}

keys = olympic_medals_collection.find_one({}, suppressed_columns).keys()
data = olympic_medals_collection.find({}, suppressed_columns)

df = DataFrame(data, columns=keys)
print(df['Country'])
dfGrouped = \
	df.groupby(['Year', 'Country', 'Medal']).size().reset_index(name="Count").sort_values(by='Year', ascending=True)
plt.ylabel('Number of Medals')
plt.xlabel('Year')

for x in countries:
	tmpGroup = dfGrouped[dfGrouped['Country'].str.contains(x)]
	for y in medals:
		plt.title(x)
		tmpGroup2 = tmpGroup[tmpGroup['Medal'].str.contains(y)]
		print(tmpGroup2)
		plt.plot(tmpGroup2['Year'], tmpGroup2['Count'], color=medal_colors[y])
		plt.legend(medals)
	plt.ylabel('Medals Earned')
	plt.xlabel('Year')
	plt.show()

for x in countries:
	dfGrouped = df.groupby(['Year', 'Country']).size().reset_index(name="Count")
	tmpGroup = dfGrouped[dfGrouped['Country'].str.contains(x)]
	plt.plot(tmpGroup['Year'], tmpGroup['Count'], color=country_colors[x])
plt.title('Medals by country')
plt.show()
