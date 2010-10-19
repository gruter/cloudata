#!/usr/bin/python

import sys
import os.path
import xml.parsers.expat

class Configuration:
	map={}

	def __init__(self, parentDir, defaultFileName, siteFileName):
		self.parse(parentDir, defaultFileName)
		self.parse(parentDir, siteFileName)

	def parse(self, parentDir, fileName):
		file = open(parentDir + '/' + fileName, 'r')

		parser = xml.parsers.expat.ParserCreate()
		parser.StartElementHandler = self.start_element
		parser.EndElementHandler = self.end_element
		parser.CharacterDataHandler = self.char_data

		parser.ParseFile(file)

	def start_element(self, name, attrs):
		self.currentTagName = name

	def end_element(self, name):
		if self.currentTagName == 'value':
			self.currentConfName=None

		self.currentTagName=None

	def char_data(self, data):
		if self.currentTagName == 'name':
			self.currentConfName = data
		elif self.currentTagName == 'value':
			if self.currentConfName == None:
				print "shit : ", data
				exit(1)

			self.map[self.currentConfName] = data

	def get(self, name):
		return self.map[name]


	def print_all(self):
		maxLen=0
		keyList=self.map.keys()
		keyList.sort()
		for k in keyList:
			if len(k) > maxLen:
				maxLen=len(k)

		maxTabCount= maxLen / 8 + 1
		#print "KEY%sVALUE" % self.get_tabs(maxTabCount)
		#print ""

		for k in keyList:
			tabCount = maxTabCount - (len(k) / 8)
			print '%s%s%s' % (k, self.get_tabs(tabCount), self.map[k])

	def get_tabs(self, tabCount):
		tabs=''
		i=0
		while(i < tabCount):
			i=i+1
			tabs +='\t'

		return tabs;


if __name__ == "__main__":
	confPath = os.path.abspath(os.path.dirname(sys.argv[0])) + '/../conf'

	conf = Configuration(confPath, "cloudata-default.xml", "cloudata-site.xml")
	if len(sys.argv) == 1:
		conf.print_all()
	elif len(sys.argv) > 1:
		map(lambda f: sys.stdout.write(f + '\n'), map(lambda f: conf.get(f), sys.argv[1:]))
