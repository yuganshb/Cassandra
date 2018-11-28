import json
from pprint import pprint
import os

files = os.listdir('./workshop_dataset1')
print(files)

i=0

for file in files:

	a = open('workshop_dataset1/' + file)

	fields_tab1 = ["datetime" , "author" , "tid" , "tweet_text" , "author_id" , "location" ,"lang"]

	data = json.load(a)

	

	for key,value in data.items():

		data2 = "'{"
		for field in fields_tab1:
			tmp = json.dumps(value[field])
			
			if(value[field] is not None):
				data2 = data2 + "\"" + field + "\"" + ":" + tmp + ","
			else:
				data2 = data2 + "\"" + field + "\"" + ":" + "null" + ","



		i=i+1
		l = len(data2)
		data3 = data2[:l-1] + "}'" 
		#print(data3)
		#print("\n")


print(i)