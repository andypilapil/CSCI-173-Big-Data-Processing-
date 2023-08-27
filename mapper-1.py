#!/usr/bin/python
# Mapper for the King James Bible
# wyu@ateneo.edu
import sys
import re

pattern = r"^\d\d:.*(?:\n.*\s[^\d\n\n\n]*)" # finds the verse pattern dd:ddd:ddd
lines=re.findall(pattern,sys.stdin,re.M) 
for line in lines:
    verse = re.sub(r'[^\w\x20\t]',' ',line[11::]) # removes punctuations and the book, chaptern, and verse numbers
    value=len(verse.split()) 
    key = line[:2] 
    print("%s\t%d" % (key, value))

# submitted by: Andrea Pilapil and Mari Valle