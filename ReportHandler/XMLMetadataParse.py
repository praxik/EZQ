#This program takes a list with field names and returns a dictionary with values for those fields.
#Documentation for xml.etree.ElementTree
#https://docs.python.org/2/library/xml.etree.elementtree.html 
#See this for xPath notation: https://docs.python.org/2/library/xml.etree.elementtree.html#xpath-support
#Just use [@attrib] for finding the bounding box info, eg [@SouthLatitude]
#Bellevue Washington.xml
import xml.etree.ElementTree as ET
import subprocess as sub
import urllib

class bingImage:
	def __init__(self, bBox, APIKey = "AmtfiRWiXQCL5vSmMz0otxtRWEozSxf2PowsOQsRXvTYpKD189BOmg8t-8_3lcJl"):
		self.bBox = bBox
		self.APIKey = APIKey
		self.getBounds()
		self.setURLs()
	
	#Sets the bounds of the image.
	#This is assuming the coordinates are in lat/long and in the western hemisphere.
	def getBounds(self):
		buffer = 0.01
		bufDict = {'WestLongitude': buffer, 'EastLongitude': -1*buffer, 'NorthLatitude': buffer, 'SouthLatitude': -1*buffer}
		
		#Gdal doesn't seem to be thread safe, so we will run into problems if more than one instance tries to access a file.
		#So I'm going to generate filenames based off of the bounding box.
		self.fileName = ""
		
		for key in self.bBox:
			self.increaseSize(key, bufDict[key])
			self.fileName += key + str(self.bBox[key])
	
	#The bounds will be created by finding the extent of the vector layer, so I want a little extra for a buffer.
	def increaseSize(self, key, value):
		print self.bBox[key]
		print value
		print self.bBox[key] + value
		print " "
		self.bBox.setdefault(key, self.bBox[key] + value)
		
	def setURLs(self):
		baseURL= "http://dev.virtualearth.net/REST/v1/Imagery/Map/Aerial"
		mapArea = "mapArea=" + str(self.bBox['SouthLatitude']) + "," + str(self.bBox['WestLongitude']) + "," + str(self.bBox['NorthLatitude']) + "," + str(self.bBox['EastLongitude'])
		#http://msdn.microsoft.com/en-us/library/ff701724.aspx
		#Docs say max size is 900, 834 pixels. I can retrieve larger images though, so I'm not quite sure what the actual limit is,
		# or if it is the same for every image. Minimum size is 80,80.
		mapSize = "mapSize=830,830"
		metaDataRequest = "mapMetadata=1&o=xml"
		mapFormat = "format=png"
		
		self.imageURL = baseURL + "?" + mapArea + "&" + mapSize + "&" + mapFormat + "&key=" + self.APIKey
		self.imageMetaData = self.imageURL + "&" + metaDataRequest
	
	def getURLs(self):
		print self.imageURL
		urllib.urlretrieve(self.imageURL, self.fileName + ".png")
		urllib.urlretrieve(self.imageMetaData, self.fileName + ".xml")
		
	def returnPath(self):
		return self.fileName
		
		
		
class xmlDoc:
	def __init__(self, filePath = None):
		if (filePath != None):
			self.getFileInfo(filePath)
			self.getData() 
	
	#Gets the file.
	def getFileInfo(self, filePath):
		self.namespace = '{http://schemas.microsoft.com/search/local/ws/rest/v1}'
		self.tree = ET.parse(filePath)
		self.data = self.tree.find(self.namespace + "ResourceSets")[0][1][0][0]
		
	#Parses the xml file and creates the dictionaries with the data.
	def getData(self):
		self.bounds = {}
		for num in range(0, 4):
			self.getValue(num)
			
		#print self.bounds.values()
		#print self.bounds.keys()
		#print self.bounds['SouthLatitude']
		#print self.bounds['WestLongitude']
		#print self.bounds['NorthLatitude']
		#print self.bounds['EastLongitude']
			
	def getValue(self, num):
		self.bounds.setdefault(self.data[num].tag.replace(self.namespace, ""), self.data[num].text)
		
	#Returns the dictionary.
	def returnData(self):
		return self.bounds

#EPSG is assumed to be 4326 for Bing Maps.
#Note actual bound box is slightly different than requested bounding box.
#  It gives about the same area, but I don't want to use the requested bounding box for georeferenceing.
class geoReferenceBing:
	def __init__(self, inputFile, outputFile, bounds):
		self.inputFile = inputFile
		self.outputFile = outputFile
		self.bounds = bounds
		
		self.createString()
		
	def createString(self):
		self.gdalString = "gdal_translate -of GTiff -a_srs EPSG:4326 -a_ullr " 
		self.gdalString += self.bounds['WestLongitude'] + " " + self.bounds['NorthLatitude'] + " " +self.bounds['EastLongitude'] + " " + self.bounds['SouthLatitude'] + " " 
		self.gdalString += self.inputFile + " " + self.outputFile
		
		self.gdalList = ["gdal_translate"]
		self.gdalList.append("-of GTiff")
		self.gdalList.append("-a_srs")
		self.gdalList.append("EPSG:4326")
		self.gdalList.append("-a_ullr ")
		self.gdalList.append(self.bounds['WestLongitude'] + " " + self.bounds['NorthLatitude'] + " " +self.bounds['EastLongitude'] + " " +self.bounds['SouthLatitude'] + " ")
		self.gdalList.append(self.inputFile)
		self.gdalList.append(self.outputFile)
		
		print self.gdalString
		
	def callGdal(self):
		#call([self.gdalList])
		proc = sub.Popen(self.gdalString, stdout=sub.PIPE, stderr=sub.PIPE)
		output, errors = proc.communicate()
		print output
		
		

class getRaster:
	def __init__(self, boundBox):
		#Get image and metadata.
		pic = bingImage(boundBox)
		pic.getURLs()
		#Parse XML.
		data = xmlDoc(pic.returnPath() + ".xml")
		#Georeference image.
		gdal = geoReferenceBing(pic.returnPath() + ".png", pic.returnPath() + ".tiff", data.returnData())
		gdal.callGdal()
		
		self.geoImagePath = pic.returnPath() + ".tiff"
		
	def getImage(self):
		return self.geoImagePath()
		
		
#jsonBoundBox = {'WestLongitude':-93.096323014,'NorthLatitude':42.9762446555,'EastLongitude':-93.0836200721,'SouthLatitude':42.953916321}
#boundBox = {'WestLongitude':-93.126,'NorthLatitude':42.938,'EastLongitude':-93.124,'SouthLatitude':42.936}
#test = xmlDoc("Bellevue Washington.xml", list)


#pic = bingImage(jsonBoundBox)
#pic.getURLs()

#data = xmlDoc(pic.returnPath() + ".xml")

#gdal = geoReferenceBing(pic.returnPath() + ".png", pic.returnPath() + ".tiff", data.returnData())
#gdal.callGdal()
	