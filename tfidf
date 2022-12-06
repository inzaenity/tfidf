import math
import re
from collections import Counter
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env


class tfidf(MRJob):
	def mapper1(self, _, line):
		#Splits the line into word and date
		words = re.split("[ *$&#/\t\n\f\"\'\\,.:;?!\[\](){}<>~\-_]", line.lower())
		words = [word.strip('.,!;()[]') for word in words]
		words = [word.replace("'s",'') for word in words]
		date = words[0]
		year = date[0:4]
		
		#combines each word and its year into 1 key with the value 1. Allows us to count how many times the word appears
		for word in words[1:]:
			yield word+","+year,1
			
	def combiner1(self, WordYear,count):
		#splits each key into Word and year 
		wi,wj = WordYear.split(",",1)
		yield WordYear, sum(count)
		#for each unique word+year key we yield a special token after it to count how many years the word appears in	
		yield wi+",*",1	
	def reducer_init1(self):
		self.counter = 0
	def reducer1(self, WordYear,count):
		wi,wj = WordYear.split(",",1)
		if wj == "*":
			#counting num of years word appears in
			self.counter = sum(count)
		else:
			#We sum all the times the word has appears which is our term frequency
			tf = sum(count)
			#math to find tfidf
			tfidf = tf * math.log10(int(jobconf_from_env('myjob.settings.years'))/self.counter)
			#tfidf = tf * math.log10(3/self.counter)
			yield wi,";"+ wj +","+ str(tfidf)
	
	#this mapper joins together the values of the key and essentially appends them together so that we get 
	#the format we want for the assignment		
	def mapper2(self, Word, tfidf):
		output = "".join(tfidf)
		yield Word, output[1:]
	def steps(self):
		return [
			MRStep(
				mapper = self.mapper1,
				combiner = self.combiner1,
				reducer_init = self.reducer_init1,
				reducer = self.reducer1,
			),
			MRStep(
				reducer = self.mapper2,
			),
				
		
		]
if __name__ == '__main__':
	tfidf.run()
