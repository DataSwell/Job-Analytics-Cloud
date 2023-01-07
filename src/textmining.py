import mongo_atlas
import pandas as pd

# extracting all the jobdescriptions from MongoDB in an iterable cursor object
gd_descriptions = mongo_atlas.get_jobdetails_description()

# concatenate the strings into one big string
total_descriptions = ''

for desc in gd_descriptions:
    total_descriptions = total_descriptions + desc['description']

# counting the ffrequency of each word and store them in a dict
splitted_string = total_descriptions.split()
words_dict = {}

for word in splitted_string:
    if word not in words_dict.keys():
        words_dict[word] = 0
    # increment the count for the word
    words_dict[word] = words_dict[word] + 1

#print(words_dict)

# Converting the dict to a Dataframe
words_df = pd.Series(data=words_dict)
print(words_df)


# Saving locally
words_df.to_excel(
    f'D:/Projekte/Job-Analytics-Cloud/data/gd_textmining.xlsx')

