import csv
import json
from pprint import pprint
import pymongo
import requests
from itertools import islice
from pandas import DataFrame
import pymongo
import matplotlib.pyplot as plt

# Constants that set encoding, the path to our files, number of lines to preview, and the dataset to download
URL = 'divyansh22/summer-olympics-medals'
PATH = 'dataFiles'
DIRTY_FILENAME = 'dataFiles/Summer-Olympic-medals-1976-to-2008.csv'
CLEAN_FILENAME = 'dataFiles/cleaned_Summer-Olympic-medals-1976-to-2008.csv'
JSON_FILENAME = 'dataFiles/cleaned_Summer_Olympic_medals.json'
ENCODING = 'latin-1'
DEFAULT_PREVIEW = 5


#
# Encoding is important to this script--without changing it to latin-1, it breaks everything on MacOS due to the special
# characters in some foreign names in the data.
#
def cleanFile(filename, destination, enc='latin-1'):
	data = []
	# This clears out any entirely blank lines
	with open(filename, encoding=enc) as file:
		for line in file:
			# for x in range(1976, 2009, 4):
			# line = line.replace(str(x), str(x)+'-01-01 00:00:00')
			if ',,,,,,,,,,' not in line:
				data.append(line)
	file.close()

	file = open(destination, 'w', encoding=enc)
	file.write(''.join(data))
	file.close()
	print('Successfully cleaned and saved to {}. {:,.0f} lines found.'.format(CLEAN_FILENAME, len(data)))


#
# Names in the data are enclosed in quotation marks, but using the CSV library to view them removes the quotes.
# While there is a comma between firstName and lastName, it is formatted correctly for when we analyze the data.
#
def preview(fn, file_encoding='latin-1', number_of_objects=5):
	with open(fn, 'r', encoding=file_encoding) as file:
		comma = ''
		if 'csv' in fn:
			preview_text = 'CSV Preview'
			comma = ', '
			number_of_objects += 1
			allData = csv.reader(file, delimiter=',')
			data_rows = islice(allData, number_of_objects)

		elif 'json' in fn:
			preview_text = 'JSON Preview'
			# 13 lines per object, 1 for the opening bracket.
			number_of_objects *= 13
			number_of_objects += 1
			data_rows = islice(file, number_of_objects)

		print('\n' + '#' * (len(preview_text) + 6) + f'\n#  {preview_text}  #\n' + '#' * (len(preview_text) + 6), '\n')
		for row in data_rows:
			print(f'{comma}'.join(row))


def convertToJson(clean_csv_path, destination):
	dump = []
	with open(clean_csv_path, encoding=ENCODING) as csvFile:
		reader = csv.DictReader(csvFile)
		for row in reader:
			dump.append(row)

	with open(destination, "w", encoding=ENCODING) as jsonFile:
		jsonFile.write(json.dumps(dump, indent=4, ensure_ascii=False))


if __name__ == '__main__':
	try:
		requests.get('http://kaggle.com')
	except Exception as e:
		try:
			requests.get('http://google.com')
			print('Unable to establish a connection to kaggle. Please check to see if the site is down and try again.')
			print(e)
			exit(1)
		except Exception as err:
			print('\nThere is no internet connection.\nPlease check your device\'s connection and try again.\n')
			print(err)
			exit(1)

	try:
		import kaggle

		kaggle.api.authenticate()
	except OSError as e:
		print(e)
		print('\nYou must configure the kaggle API on your computer to run this script.')
		print('Visit kaggle.com/docs/api#getting-started-installation-&-authentication for instructions.')
		print('\nBasic summary:\n1.\tGo to kaggle.com/account\n2.\tScroll down to API and select \'Create New Token\'.')
		print('3.\tIf you are on Windows, make sure kaggle.json is located at C:/Users/%Username%/.kaggle/kaggle.json')
		print('\tOn MacOS, make sure it is located at ~/.kaggle/kaggle.json\n')
		exit(1)

	try:
		# downloads the dataset
		kaggle.api.dataset_download_files(URL, PATH, unzip=True)
		print(f'\nData successfully saved to {DIRTY_FILENAME}')

		# cleans the commas out of the dataset, resaves
		cleanFile(DIRTY_FILENAME, CLEAN_FILENAME)
	# preview(CLEAN_FILENAME)

	except Exception as e:
		print('Retrieval failed. Please try again.\n', e)
		exit(1)

	# This is where the mongoDB portion comes in
	# I used w3schools as a walkthrough on adding to the db
	# It also has good content on querying/modifying the db
	# https://www.w3schools.com/python/python_mongodb_find.asp
	try:
		# converts the CSV dataset to JSON and previews
		convertToJson(CLEAN_FILENAME, JSON_FILENAME)
		# preview(JSON_FILENAME, number_of_objects=1)

		client = pymongo.MongoClient(
			"mongodb+srv://test_user:test_password@olympic-data-qfgy4.mongodb.net/test?retryWrites=true&w=majority")
		db = client['MIS-5400-Data-Persistence']
		collection_name = 'Olympic-Medalists'
		olympic_medals_collection = db[collection_name]

		# ensures that if it is run multiple times, only the latest run is kept in the db
		olympic_medals_collection.drop()

		with open(JSON_FILENAME, 'r', encoding=ENCODING) as file:
			data = json.load(file)
		print('\nUploading to MongoDB...')
		x = olympic_medals_collection.insert_many(data)
		print('Done')

		# Tests the db to ensure it works
		query = {"Year": "1980"}
		suppressed = {'Event_gender': 0, '_id': 0, 'Country_Code': 0, 'Event': 0, 'Gender': 0, 'City': 0}
		returnedResults = olympic_medals_collection.find(query, suppressed)

	# slicedResults = islice(returnedResults,1)
	# for i in slicedResults:
	#    pprint(i)

	except Exception as e:
		import platform

		if platform.system() != 'Windows':
			try:
				import dnspython

				if platform.system() == 'Darwin':
					print('Ensure that Python\'s \'Install Certificates.command\' file has been run as')
					print('superuser from the command line.')

			except ImportError as err:
				print(f'{err}\nPlease pip install dnspython to run the script.')

		print(e)
		exit(1)

	try:
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
		dfGrouped = \
			df.groupby(['Year', 'Country', 'Medal']).size().reset_index(name="Count").sort_values(by='Year',
			                                                                                      ascending=True)
		plt.ylabel('Number of Medals')
		plt.xlabel('Year')

		for x in countries:
			tmpGroup = dfGrouped[dfGrouped['Country'].str.contains(x)]
			for y in medals:
				plt.title(x)
				tmpGroup2 = tmpGroup[tmpGroup['Medal'].str.contains(y)]
				plt.plot(tmpGroup2['Year'], tmpGroup2['Count'], color=medal_colors[y])
				plt.legend(medals)
			plt.ylabel('Medals Earned')
			plt.xlabel('Year')
			plt.show()

		'''
		for x in countries:
			dfGrouped = df.groupby(['Year', 'Country']).size().reset_index(name="Count")
			tmpGroup = dfGrouped[dfGrouped['Country'].str.contains(x)]
			plt.plot(tmpGroup['Year'], tmpGroup['Count'], color=country_colors[x])
		plt.title('Medals by country')
		plt.show()
		'''

	except Exception as e:
		print(e)
