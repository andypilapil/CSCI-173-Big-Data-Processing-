#!/usr/bin/python
# Reducer for the King James Bible
# wyu@ateneo.edu

import sys
import pandas as pd

# create a lookup table for the books
ref = open("biblebooks.txt").readlines()
book_nums = [line.split()[1] for line in ref]
book_names = [' '.join(line.split()[2::]) for line in ref]
book_ref = pd.DataFrame({
    'book name': book_names,
    'book number': book_nums,
    'count': 0
})

for input_line in sys.stdin:
    input_line = input_line.strip()
    key, value = input_line.split("\t", 1) 
    value = int(value)


    book_ref.loc[book_ref['book number'] == key, 'count'] += value 

    this_key = book_ref.loc[book_ref['book number'] == key]['book name'].to_list()[0] # gets the book name
    running_total = book_ref.loc[book_ref['book number']== key]['count'].to_list()[0] # gets the current count
    print("%s\t%d" % (this_key, running_total)) # displays the running total

top_books = book_ref.nlargest(n=5, columns=['count']).drop('book number', axis=1) # gets the top 5 books according to word count
print("\n\n\nThe Top Books of the King James Bible according to word count:")
print(top_books.to_string(index=False))

#Submitted by: Andrea Pilapil and Mari Valle