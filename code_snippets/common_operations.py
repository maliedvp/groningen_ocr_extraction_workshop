import pandas as pd
import uuid
import re
import numpy as np
import multiprocessing as mp
from utils import Database_connector
from datetime import date
import time

class Common_operations():
	def __init__(self):
		pass


	def date_detection(self, string:str, extract=True, replace=True):
		"""
		Identifies (various representations) of dates from a string.

		Args:
			string (str): The string that includes the date(s)
			extract (bool): If True, the returned tuple includes the identified dates in a datetime.date-format
			replace (bool): If True, the date(s) is/are deleted from the returned string

		Returns:
			Tuple

		Example:
			Common_operations().date_detection(string='Mein Geburtstag ist am 3.8.1990. Der Meiner Nichte am 8./7. 2020', extract=True, replace=False)
			('Mein Geburtstag ist am 3.8.1990. Der Meiner Nichte am 8./7. 2020', [datetime.date(1990, 8, 3), datetime.date(2020, 7, 8)])
		"""

		dict_months = {r'[Jj][Aa][Nn]\.?[Uu]?[Aa]?[Rr]?':'1', r'[Ff][Ee][Bb]\.?[Rr]?[Uu]?[Aa]?[Rr]?':'2', r'[Mm][ÄAäa][Rr]\.?[Zz]?':'3', r'[Aa][Pp][Rr]\.?[Ii]?[Ll]?':'4', r'[Mm][Aa][Ii]':'5', r'[Jj][Uu][Nn]\.?[Ii]?[Oo]?':'6', r'[Jj][Uu][Ll]\.?[Ii]?':'7', r'[Aa][Uu][Gg]\.?[Uu]?[Ss]?[Tt]?':'8', r'[Ss][Ee][Pp]\.?[Tt]?\.?[Ee]?[Mm]?[Bb]?[Ee]?[Rr]?':'9', r'[Oo][Kk][Tt]\.?[Oo]?[Bb]?[Ee]?[Rr]?':'10', r'[Nn][Oo][Vv]\.?[Ee]?[Mm]?[Bb]?[Ee]?[Rr]?':'11', r'[Dd][Ee][Zz]\.?[Ee]?[Mm]?[Bb]?[Ee]?[Rr]': '12'}
		rgx_month = '|'.join(dict_months.keys())

		rgx1 = r'(\d?\d\.)\s?(%s)\s?(\d\d\d\d)' % (rgx_month)
		
		rgx2 = r'(\d?\d\.)[\s\/]*(0?1\.|0?2\.|0?3\.|0?4\.|0?5\.|0?6\.|0?7\.|0?8\.|0?9\.|10\.|11\.)[\s\/]*(\d\d\d\d)'
		
		rgx3 = r'()()(\d\d\d\d)'

		rgx_combi = '|'.join([rgx1, rgx2, rgx3])


		if extract == True:
			dates = [(tuple(x for x in _ if x)) for _ in re.findall(rgx_combi, string)]
			dates = [x if len(x)==3 else tuple((None, None, x[0])) for x in dates]

			# convert to dates
			if dates != []:
				# format
				df = pd.DataFrame(dates, columns=['day','month','year'])
				df['day'] = df['day'].str.replace(r'\D', '', regex=True).replace('', None).astype(float)
				df['month'] = df['month'].replace(dict_months, regex=True).replace('', None).astype(float)
				df['year'] = df['year'].astype(int)

				dates = list(df.itertuples(index=False, name=None))

				# convert to datetime if possible
				for idx, d in enumerate(dates):
					try:
						dates[idx] = date(int(d[2]), int(d[1]), int(d[0]))
					except ValueError:
						pass

		else:
			dates = []

		if replace==True:
			string = re.sub(r'\s+',' ', re.sub(rgx_combi, '', string).strip())

		return string, dates


	def string_decomposer(self, string:str,comp_regex:dict):
		"""
		Splits a string into a dictionary in whose keys are specified in the argument comp_regex

		Args:
			string (str): Any string
			comp_regex (dict): Keys are identical to the keys in returned dictionary. The values are a list of regexs that should be used for splitting the string

		Returns:
			dict

		Example:
			result = Common_operations().string_decomposer(
				string= 'Gewinn- u. Verlust-Konto: Debet: Item A 111, Item B 222. - Kredit: Item C 333, Item D 444. Sa. M. 777.', 
				comp_regex = {'debet':['Debet:'], 'kredit' : ['Kredit', 'Credit:'], 'sum':['Sa\.?\s?M\.?']}
				)

			result = {'debet': 'Item A 111, Item B 222. -', 'kredit': ': Item C 333, Item D 444.', 'sum': '777', 'begin': 'Gewinn- u. Verlust-Konto:'}
		"""
		
		existing_items = []
		part_start = {}
		part_end = {}
		
		### range of item-specific (copiled and joined) regex
		for item, rx_list in comp_regex.items():
			item_rx = re.compile('|'.join(rx_list))
			if re.search(item_rx, string) != None:
				existing_items.append(item)
				part_start[item] = re.search(item_rx, string).span()[0]
				part_end[item] = re.search(item_rx, string).span()[1]


		### range of whole string including item names
		item_range_substr = {}
		for item, rx_list in comp_regex.items():
			if item in existing_items:
				try: # for all but last element
					item_range_substr[item] = tuple([part_start[item], [element for element in sorted(part_start.values()) if element > part_start[item]][0]])
				except IndexError: # for last element
					item_range_substr[item] = tuple([part_start[item], len(string)-1])
					
		# add start part of the string
		if len(existing_items) > 0:
			item_range_substr['begin'] = tuple([0, min(part_start.values())])
		else:
			item_range_substr['begin'] = tuple([0, len(string)-1])
		
		### substring of whole string without item names
		item_substring = {}
		for item in item_range_substr.keys():
			try: # for all but first element
				item_substring[item] = string[part_end[item]:item_range_substr[item][1]].strip()
			except: # for first item
				item_substring[item] = string[item_range_substr[item][0]:item_range_substr[item][1]].strip()
			
			
		return item_substring


	def strip_extract_item(self, df:pd.DataFrame, col:str, rgx_dict:dict):
		"""
		Extracts substrings from a column in a pandas DataFrame and creates new columns that include the extracted substrings

		Args:
			df (pd.DataFrame): a pandas DataFrame that includes the column from which substrings are being extracted
			col (str): The column name of the DataFrame from which substrings are being extracted
			rgx_dict (dict): Keys are the names of the new columns. Values include the regexs of the substrings that are extracted

		Returns:
			pd.DataFrame

		Example:
			df = pd.DataFrame({'column1':
					[
						'This is a line',
						'This is a second line. One with a longer content.'
					]
				}
			)

			df_out = Common_operations().strip_extract_item(df, 'column1', {'a':'1', 'w[Ii]th': '2'})

			df_out:
													column1  1     2
				0                              This is line  a      
				1  This is second line. One longer content.  a  with
		"""
		df = df[[col]].copy()

		keep_cols=[col]
		
		for rgx, keep in rgx_dict.items():
			if keep == False:
				pass
			else:
				# extract
				if keep in list(df):

					df[keep + '_help'] = df[col].str.extract('(\s*' + rgx +'\s*)', expand=False).str.strip()

					df[keep] = df[keep].str.cat(df[keep + '_help'], sep='|', na_rep='')

					
					del df[keep+ '_help']

					df[keep] = df[keep].str.replace(r'^\||\|*$','', regex=True)
					
				else:
					df[keep] = df[col].str.extract('(\s*' + rgx +'\s*)', expand=False).str.strip()
					keep_cols.append(keep)
					df[keep] = df[keep].fillna('')
				
		
			# strip
			df[col] = df[col].str.replace(rgx, ' ', regex=True).str.strip().str.replace(r'\s{2,}',' ', regex=True)
			
		df = df[keep_cols]

		return df


	def df_city_generation(self, path_to_files:str='../res/geography/', file_basis:str='city_raw.json', file_manual_addition:str='city_add.xlsx', drop_duplicates:bool=True):
		"""
		Generates a pandas Dataframe that includes cities as well as their higher-level administration areas (1910)

		Args:
			path_to_files (str): Path to folder where the files are stored
			file_basis (str): Name of the dataset that serves as basis (the municipal data is kindly provided by [Uli Schubert](https://www.gemeindeverzeichnis.de/gem1900/gem1900.htm?gem1900_2.htm) as refers to a cross-section of 1910)
			file_manual_addition (str): Name of the file that includes manual additions (for the structure see the Readme.md in the stated directory)
			drop_duplicates (bool): If True, name duplicates (Gemeindename) are dropped and only the observation with the highest number of citizens is kept. This step might me necessary when merging based on city names as this avoids 1:m-matches

		Returns:
			pd.DataFrame

		Example:
			Common_operations().df_city_generation()
		"""

		# raw list
		df_basis = pd.read_json(path_to_files + file_basis)
		df_basis['Staat'] = 'Deutschland'
		df_basis['manually_added'] = 0

		# manual additions
		df_add = pd.read_excel(path_to_files + file_manual_addition)
		df_add['Staat'] = np.where(df_add['Staat'].isna(), 'Deutschland', df_add['Staat'])
		df_add['manually_added'] = 1
		df_add = pd.merge(df_add, df_basis[[col for col in df_basis if not col in ['Gemeindename','Staat', 'manually_added']]], on='idx', how='left', validate='m:1')

		# append
		df = pd.concat([df_basis, df_add], ignore_index=True, axis=0, join='outer')

		# harmonize formatting
		df['city_id'] = pd.to_numeric(df['idx'], errors='coerce')
		del df['idx']

		if drop_duplicates==True:
			# drop duplicates on Gemeindename (keep only largest)
			df = df.sort_values('Einwohner 1910', ascending=False)
			df = df.drop_duplicates(subset='Gemeindename', keep='first').reset_index(drop=True)

		return df



	def reshape(self, string:str, index:str, df_city:pd.DataFrame, delimiters:list=[':',';',','], guess_delimiter_order=True):
		
		if guess_delimiter_order == True:
			delimiter_count = {delimiter : len(re.split(delimiter, string))-1 for delimiter in delimiters}
			delimiter_order = {idx+1: delimiter for idx, delimiter in enumerate(sorted(delimiter_count, key=delimiter_count.get, reverse=False))}
		else:
			delimiter_order = {idx+1: delimiter for idx, delimiter in enumerate(delimiters)}

		
		# assumption: only most frequent used delimiter does not create an own category -> d_words)
		# other delimiters devide the string into parts -> d_parts

		#0
		string = re.sub(r'\.$', '', string)


		#1
		help_list1 = [re.split(delimiter_order[1], substring) for substring in [string]]


		#2
		help_list2 = []
		for sublist1 in help_list1:
			help_list2.append([re.split(delimiter_order[2], substring) for substring in sublist1])
			

		#3
		help_list3 = []
		for sublist2 in help_list2:
			for sublist1 in sublist2:
				help_list3.append([re.split(delimiter_order[3], substring) for substring in sublist1])

		
		city_list = df_city['Gemeindename'].tolist()
		city_list_re = ['^' + element + '\.?$' for element in city_list]
		df_dict = {}
		i=1
		for level1 in help_list3:
			for level2 in level1:
				if re.search('|'.join(city_list_re),level2[-1].strip()) != None:
					city = level2[-1].strip()
					help_dict = {'city':[city for i in level2[:-1]],'name':[name.strip() for name in level2[:-1]]}
				else:
					help_dict = {'city':['' for i in level2],'name':[name.strip() for name in level2]}

					
				df_dict[i] = pd.DataFrame(help_dict)
				i=i+1
					
		df = pd.concat(df_dict.values(), ignore_index=True)


		df['city_id_max'] = df['city'].replace(dict(zip(df_city['Gemeindename'], df_city['city_id']))).replace({'':np.nan})
		# df['city_id_list'] = 

		df['person_id'] = [uuid.uuid1().hex for _ in range(len(df.index))]
		df['index_orig'] = index
			
		return df

