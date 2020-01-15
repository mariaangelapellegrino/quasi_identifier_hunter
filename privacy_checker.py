import sys
import time
import pandas as pd
from time import strftime
from functools import reduce
import itertools 
import numpy as np
import os

def start_evaluation(file_path):
	results_directory = os.path.dirname(file_path)
	file_name = os.path.basename(file_path).split(".")[0]

	RESULT_PATH = "results/"+results_directory

	if not os.path.exists(RESULT_PATH):
		os.mkdir(RESULT_PATH)

	RESULT_PATH += "/"+file_name+"_results.txt"

	start_time = time.time()

	data_df = pd.read_csv(file_path)

	temp_result = optimized_columns_and_singletons_stats_and_quasi_identifies(data_df)

	end_time = time.time()
	duration = end_time - start_time

	output_file = open(RESULT_PATH, "w")

	output_file.write("Number of identifiers: " + str(len(temp_result['identifiers']))+"\n")
	output_file.write("Number of singletons: " + str(temp_result['absolute_value_quasi_identifier']) 
		+ "/" + str(temp_result['size_sample'])+"\n")
	output_file.write("Percentage of singletons: " + str(temp_result['percentage_quasi_identifiers'])+"\n")
	output_file.write("Best quasi-identifier: "+ ",".join(temp_result['quasi_identifiers'])+"\n")
	output_file.write("Distinct values: " + str(temp_result['distinct_values'])+"\n")

	output_file.write("Execution time: " + str(round(duration/1000, 4)) + " seconds\n")

	output_file.close()

	print("It requires " + str(round(duration/1000, 4)) + " seconds")

def optimized_columns_and_singletons_stats_and_quasi_identifies(data_df):

	all_statistics_for_combination = {}
	all_singleton_occurrences = {};

	identifiers = [];
	num_columns = data_df.shape[1]

	fieldKeys = list(data_df)
	dataset_size = data_df.shape[0]
	columns_to_check = fieldKeys

	for subset_size in range(1, num_columns+1): 
		combinations = [];
		data_list = [];

		combinations = get_combinations(columns_to_check, subset_size)
		temp_identifiers, still_to_check = split_in_identifiers_and_not(data_df, combinations)
		identifiers += temp_identifiers

		temp_statistics_for_combination, temp_singleton_occurrences = get_columns_and_singletons_stats(data_df, still_to_check)
		all_statistics_for_combination.update(temp_statistics_for_combination)
		all_singleton_occurrences.update(temp_singleton_occurrences)

		columns_to_check = list(set(reduce(lambda x,y: x+y,still_to_check)))

	temp_result = get_quasi_identifiers(all_statistics_for_combination)

	temp_to_return = {}

	identifiers += temp_result['identifiers']

	temp_to_return['identifiers'] = identifiers
	temp_to_return['percentage_quasi_identifiers'] = temp_result['percentage_quasi_identifiers']
	temp_to_return['absolute_value_quasi_identifier'] = temp_result['absolute_value_quasi_identifier']
	temp_to_return['size_sample'] = temp_result['size_sample']
	temp_to_return['quasi_identifiers'] = temp_result['quasi_identifiers']
	temp_to_return['distinct_values'] = temp_result['distinct_values']

	temp_to_return['statistics_for_combination'] = all_statistics_for_combination
	temp_to_return['statistics_for_combination'] = all_singleton_occurrences

	print("Number of identifiers: " + str(len(identifiers)))
	print("Number of singletons: " + str(temp_result['absolute_value_quasi_identifier']) 
		+ "/" + str(temp_result['size_sample']))
	print("Percentage of singletons: " + str(temp_result['percentage_quasi_identifiers']))
	print(temp_result['quasi_identifiers'])
	print("Best quasi-identifier: "+ ",".join(temp_result['quasi_identifiers']))
	print("Distinct values: " + str(temp_result['distinct_values']))

	return temp_to_return



def get_combinations(arr, r): 
	data = list(range(r)) 
	combinations = []
	combination_util(arr, len(arr), r, 0, data, 0, combinations)
	return combinations 

def combination_util(arr, n, r, index, data, i, combinations): 
	
	if(index == r): 
		mylist=[]
		for j in range(r):
			mylist.append(data[j])
		combinations.append(mylist)
		return

	if(i >= n): 
		return

	data[index] = arr[i] 
	combination_util(arr, n, r, 
					index + 1, data, i + 1, combinations) 

	combination_util(arr, n, r, index, 
					data, i + 1, combinations) 


def split_in_identifiers_and_not(data_df, columns):
	identifiers = [];
	still_to_check = [];

	for combination in columns:
		num_of_rows = data_df.shape[0]
		column_values = {}

		values = data_df[combination]
		values = values.drop_duplicates()

		if values.shape[0]==data_df.shape[0]:
			identifiers.append(combination);
            
		else:
			still_to_check.append(combination);

	return identifiers, still_to_check

def get_columns_and_singletons_stats(records, columns):
	records = records.dropna()

	singleton_occurrences = {}
	statistics_for_combination = {}

	dataset_size = records.shape[0]

	occurrences_for_combination = {};
	for combo in columns:

		counts = records.groupby(combo).size()

		singletons = counts[counts == 1]
		singletons = singletons.to_dict()

		occurrences_for_combination[",".join(combo)] = counts
		singleton_occurrences[",".join(combo)] = singletons

	for combo in singleton_occurrences:
		abs_value = len(singleton_occurrences[combo])
		percentage = round(abs_value/dataset_size*100)
		statistics_for_combination[combo] = {
			'singleton_occurrences_absolute_value': abs_value,
			'dataset_size':dataset_size,
			'percentage_of_singletons':percentage,
			'distinct_values':len(occurrences_for_combination[combo])
		}

	return statistics_for_combination,singleton_occurrences
	
def get_quasi_identifiers(stats):

	distinct_values = -1
	size_sample = -1
	max_absolute_value = -1
	max_percentage = -1
	min_size = 0
	quasi_identifiers = []

	identifiers = []

	for column_combination in stats:
		statistic = stats[column_combination]
		column_combination_as_array = column_combination.split(",")

		if statistic['percentage_of_singletons']==100:
			identifiers.append(column_combnation_as_array)

		elif statistic['percentage_of_singletons']>max_percentage:
			quasi_identifiers = []
			quasi_identifiers.append(column_combination)
			min_size = len(column_combination_as_array)
			max_absolute_value = statistic['singleton_occurrences_absolute_value']
			size_sample = statistic['dataset_size']
			max_percentage = statistic['percentage_of_singletons']
			distinct_values = statistic['distinct_values']

		elif statistic['percentage_of_singletons']==max_percentage:
			actual_size = len(column_combination_as_array)
			if actual_size<min_size:
				quasi_identifiers = []
				min_size = actual_size
	        
			elif actual_size==min_size:
				quasi_identifiers += column_combination_as_array

	temp_to_return = {'identifiers':identifiers,
		'absolute_value_quasi_identifier':max_absolute_value,
		'size_sample':size_sample,
	    'percentage_quasi_identifiers':max_percentage,
		'quasi_identifiers':quasi_identifiers,
		'distinct_values':distinct_values
	}
	return temp_to_return

arguments = len(sys.argv) - 1
if arguments < 1:
	print ("you must provide the file url")
else:
	file_path = sys.argv[1]
	if not os.path.exists("results"):
		os.mkdir("results")
	start_evaluation(file_path)