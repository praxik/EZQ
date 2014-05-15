#I might add another class to pass variables to the 
# Right now GeoLayers operations are run in the initializer for GeoLayers.
import sip 
import re
sip.setapi('QVariant', 1)

import sys
import qgis
import PyQt4

#These next few imports prevent errors in the .exe created by py2exe
import PyQt4.QtNetwork
import PyQt4.QtXml


import random

from qgis.core import *
from qgis.utils import *
from qgis.gui import *

from PyQt4.QtCore import *
from PyQt4.QtGui import *

import pdb		#This is used for testing. Use pdb.set_trace() to start stepping through code when executed.
import argparse #Deals with inputs from the command line.

from XMLMetadataParse import *

#returns a raster of orthoimagery from Bing.
class BingRaster:
	def __init__(self, jsonBoundBox):
		pic = bingImage(jsonBoundBox)
		pic.getURLs()

		data = xmlDoc(pic.returnPath() + ".xml")

		gdal = geoReferenceBing(pic.returnPath() + ".png", pic.returnPath() + ".tiff", data.returnData())
		gdal.callGdal()
		
		self.geoImagePath = pic.returnPath() + ".tiff"
		
	#boundBox = {'WestLongitude':str(rectangle.xMinimum()),'NorthLatitude':str(rectangle.yMaximum()),'EastLongitude':str(rectangle.xMaximum()),'SouthLatitude':str(rectangle.yMinimum())}
	#Gets the bounds in EPSG 4236
	def getBounds(self, bBox, epsg):
		vTemp = QgsVectorLayer("Point", "temporary_points", "memory")
		self.prov = vTemp.dataProvider()
		
		self.addPointFeat(bBox['WestLongitude'],bBox['NorthLatitude'])
		self.addPointFeat(bBox['EastLongitude'],bBox['SouthLatitude'])
		
		vTemp.updateExtents()
		
		ext = layer.extent()
		
	def addPointFeat(self, x, y):
		feat = QgsFeature()
		feat.setGeometry(QgsGeometry.fromPoint(QgsPoint(x,y)))
		
		self.prov.addFeature([feat])

class GeoLayers:	
	#Prepares an image object for rendering
	def setUpImage(self):
		dpi = self.composition.printResolution()
		dpmm = dpi/25.4
		width = int(dpmm * self.composition.paperWidth())
		height = int(dpmm * self.composition.paperHeight())
		self.sourceArea = QRectF(0, 0, self.width, self.height)
		self.targetArea = QRectF(0, 0, self.width, self.height)
		self.image = QImage(self.width, self.height, QImage.Format_ARGB32)
		self.image.setDotsPerMeterX(dpmm * 1000)
		self.image.setDotsPerMeterY(dpmm * 1000)
		self.image.fill(0)
		return self
		
		
	#renders an image
	def renderImage(self, target, source):
		imagePainter = QPainter(self.image)
		self.composition.render(imagePainter, target, source)
		imagePainter.end()
	
		return self
	
	#Creates the composer map
	def createComposerMap(self, renderer = "create"):
		ComposerMap = QgsComposerMap(self.composition, 0, 0, self.width, self.height)
		self.composition.addItem(ComposerMap)
		return self
		
	#Loads a vector layer
	def loadVectorLayer(self):
		#Attempts to load the shape file.
		try:
			#Get shapefile.
			self.layer= QgsVectorLayer(self.input, self.featureName, "ogr")
			#self.layer.setCoordinateSystem("crs=epsg:4326")
			#Add layer to instance
			QgsMapLayerRegistry.instance().addMapLayer(self.layer)
		
		#Stops the program and outputs an error message if the shapefile was not loaded.
		except:
			print ""
			print "Unexpected error loading vector layer: ", sys.exc_info()[ 0 ]
			print "File was: " + self.input
			raise
		
		
		#Adds the layer to the map renderer.
		self.layerset.append(self.layer.id())
		self.mapRenderer.setLayerSet(self.layerset)
		#Sets the extent.
		self.defineExtent()
		#self.findExtent()
		return self
	
	def loadRasterLayerWithTMS(self):
		#Sets the extent of the layer set.
		#This is necessary to get the right aerial imagery.
		#Note that this means another layer must have been previously loaded.
		#self.defineExtent()
		
		try:
			#adds the layer to the layer registry
			baseName = QFileInfo(self.TMSFile).baseName()
			rasterLayer = QgsRasterLayer (self.TMSFile, baseName)
			self.baseBing = rasterLayer
			print baseName
			print "raster test: " + str(rasterLayer.isValid())
			QgsMapLayerRegistry.instance().addMapLayer(rasterLayer)
			
		except:
			print "Raster layer not loaded."
			print "File was: " + self.TMSFile
		
		#adds the layer to the map renderer.
		self.layerset.append(rasterLayer.id())
		print self.layerset
		self.mapRenderer.setLayerSet(self.layerset)
		#self.defineExtent()
		#self.findExtent()
		
		return self
	
	#Loads a raster layer
	#This is unneeded now. Only loadRasterLayerWithTMS is needed right now.
	#def loadRasterLayer(self):
		#return self
	
	#Creates a composition
	def createComposition(self, style = "print"):
		self.composition = QgsComposition(self.mapRenderer)
		#I might add more styles later.
		if (style == "print"):
			self.composition.setPlotStyle(QgsComposition.Print)
		else:
			self.composition.setPlotStyle(QgsComposition.Print)
		
		return self
		
	#Adds a legend for the composition
	def addCompositionLegend(self):
		legend = QgsComposerLegend(self.composition)
		legend.model().setLayerSet(self.mapRenderer.layerSet())
		self.composition.addItem(legend)
		
		return self
		
		
	def getRelativeColor(self, Xs):
		Xs = Xs/255
		if (Xs <= 0.03928):
			X = Xs/12.92
		else:
			X = ((Xs+0.055)/1.055)**2.4

		return X
	
	#See http://www.w3.org/TR/WCAG20/#relativeluminancedef for the formula
	#Values range from 0 to 1.
	def getrLuminance(self, red, green, blue):
		R = self.getRelativeColor(red)
		G = self.getRelativeColor(green)
		B = self.getRelativeColor(blue)
		
		rLuminance = R*0.2126 + G*0.7152 + B*0.0722
		
		return rLuminance
	
	def getSymbolNew(self,i, count):
		#The buffer controls how much of the hue space is available to the colors.
		#Increasing it increases the minimum hue difference between colors
		#Decreasing it decreases the range of colors available.
		buffer = 5
		#The hue ranges from 0 to 359
		hueInterval = int(359/count)
		hueRandIncr = random.randrange(-1*int(hueInterval/buffer),int(hueInterval/buffer),1)
		
		hue = int(i*hueInterval) + hueRandIncr
		
		#This makes sure the hue is valid.
		hue = max(0,hue)
		hue = min(hue, 359)

		sat = random.randrange(100,240,1)
		value = random.randrange(200,240,1)
		
		return MapSymbolHSV(hue, sat, value).get()
	
	
	def getSymbol(self):
		rLuminance = 0.01
		
		while (rLuminance <= 0.3):
			red = float(random.randrange(1,256,1))
			green = float(random.randrange(1,256,1))
			blue = float(random.randrange(1,256,1))
			
			rLuminance = self.getrLuminance(red, green, blue)
			
		return MapSymbol(red, green, blue).get()
	
	#colors the features in the shapefile based on self.featureName and a categorized scheme
	
	#gets the number of unique features, based on the feature name input
	def getUniqueFeatureCount(self):
		featureList = []
		
		feat = self.layer.GetNextFeature()
		#while
		
	def colorCatFeatures(self):
		#pdb.set_trace()
		#print self.layer.isValid()
		#gets the list of features to iterate over
		iter = self.layer.getFeatures()
		
		#The list of rows in the column
		rowList = []
		#The list of symbols created from the list of rows.
		symbolList = []
		
		i = 0
		
		#Creates a list of features
		for feature in iter:
			row =feature.attributes()[self.layer.fieldNameIndex(self.featureName)]
			if rowList.count(row) == 0:
				rowList.append(row)
				
		
		#Iterates through the features and adds a symbol with a random color for each feature.
		#I should change the color generation.
		##colorRamp = QgsVectorRandomColorRampV2(featureNum,0,359,100,240,200,240)
		for row in rowList:
			symbol = self.getSymbolNew(i, len(rowList))
			symbolList.append(QgsRendererCategoryV2(row, symbol, str(row.toPyObject())))
			i += 1
		
		renderer = QgsCategorizedSymbolRendererV2( self.featureName, symbolList )
		self.layer.setRendererV2( renderer )
		
		return self
	
	
	
	#Returns a list of symbols colored with red first, yellow in the middle, and green last.
	def RedYelGreenColorBlender(self, num):
		#The list of symbols.
		symList= []
		#Used for Hex values for red and green.
		redVal = greenVal = []
		#Used in the while loop.
		i = 0
		
		#What red to yellow to green looks like:
		#First red starts out at 255 and green starts out at 0 and increases until it hits 255.
		#Yellow is at (255,255,0)
		#Then red goes from 255 to 0 and 
		while (i < self.gradCatNum):
			redVal = [255, int(510 - 510/self.gradCatNum*i)]
			greenVal = [255, int(0 + 510/self.gradCatNum*i)]
			symbol = MapSymbol(min(redVal),min(greenVal),0).get()
			symList.append(symbol)
			i += 1
			
		return symList
		
	#returns a list of ranges for the Graduated Symbol Renderer.
	#Assumes attribute values are numerical.	
	def GraduatedRangeList(self):
		#gets the list of features to iterate over
		iter = self.layer.getFeatures()
		#The list of rows in the column
		rowList = []
		#The list of symbols created from the list of rows.
		rangeList = []
		#Used for a list of colored symbols
		colorList = []
		#Used to track how many symbols are created.
		i = 0
		#Min and max values for the entire set of data.
		minVal = maxVal = 0
		#Min and max values for a specific range value.
		minRangeVal = maxRangeVal = 0
		#The size of the interval
		interval = 0
		#The label for a specific range.
		rangeLabel= ""
		
		colorList = self.RedYelGreenColorBlender(self.gradCatNum)
		
		#Gets a list of all unique values and sorts it.
		for feature in iter:
			row =feature.attributes()[self.layer.fieldNameIndex(self.featureName)]
			if rowList.count(row.toFloat()[0]) == 0:
				rowList.append(row.toFloat()[0])
		
		minVal = min(rowList)
		maxVal = max(rowList)
		
		interval = (maxVal - minVal)/self.gradCatNum
		
		while (i < self.gradCatNum):
			minRangeVal = minVal + interval*i
			maxRangeVal = minVal + interval*(i + 1)
			rangeLabel = str(minRangeVal) + " - " + str(maxRangeVal)
			rangeList.append(QgsRendererRangeV2(minRangeVal, maxRangeVal, colorList[i],rangeLabel))
			i += 1
			
		return rangeList
		
	#colors the features in the shapefile based on self.featureName and a graduated scheme
	def colorGradFeatures(self):
		#gets the list of range symbols.
		rangeList = self.GraduatedRangeList()
		
		renderer = QgsGraduatedSymbolRendererV2( self.featureName, rangeList)
		self.layer.setRendererV2( renderer )
		
		return self
		
	#http://stackoverflow.com/questions/39086/search-and-replace-a-line-in-a-file-in-python
	def replaceFile(self, filePath, text, subs, flags=0):
	    with open( filePath, "r+" ) as file:
			fileContents = file.read()
			textPattern = re.compile( re.escape( text ), flags )
			fileContents = textPattern.sub( subs, fileContents )
			file.seek( 0 )
			file.truncate()
			file.write( fileContents )
	
	#Adds labels to the features based on self.featureName
	def addLabel(self):
		layerOptions = QgsPalLayerSettings() 
		layerOptions.enabled = True 
		#Decides which attribute gets labelled.
		#layerOptions.fieldName = "round(" + self.featureName + ", 2)"
		layerOptions.fieldName = self.featureName
		layerOptions.isExpression = True
		
		if not self.textLabel:
			layerOptions.decimals= 2
			layerOptions.formatNumbers = True

		#Control how labels are placed.
		if (self.label == "AroundPoint"): 	layerOptions.placement = QgsPalLayerSettings.AroundPoint
		elif (self.label == "OverPoint"):	layerOptions.placement = QgsPalLayerSettings.OverPoint
		elif (self.label == "Line"): 		layerOptions.placement = QgsPalLayerSettings.Line
		elif (self.label == "Curved"):	 	layerOptions.placement = QgsPalLayerSettings.Curved 
		elif (self.label == "Horizontal"):  layerOptions.placement = QgsPalLayerSettings.Horizontal
		elif (self.label == "Free"):	 	layerOptions.placement = QgsPalLayerSettings.Free
		#AroundPoint is set as default
		else: layerOptions.placement = QgsPalLayerSettings.AroundPoint
		
		try:
			layerOptions.setDataDefinedProperty(QgsPalLayerSettings.Size,True,True, self.labelSize,'') 
		except:
			print "label failed"
		layerOptions.writeToLayer(self.layer)
		
		return self
	
	#Cleans up after the script is finished.
	def close(self):
		#Closes down QGIS
		self.app.exitQgis()
		
	#Prints the extent, useful for testing purposes.
	def getExtentBoundBox(self):
		rectangle = self.mapRenderer.fullExtent()
		boundBox = {'WestLongitude':str(rectangle.xMinimum()),'NorthLatitude':str(rectangle.yMaximum()),'EastLongitude':str(rectangle.xMaximum()),'SouthLatitude':str(rectangle.yMinimum())}
		
		#print "xMin: " + str(rectangle.xMinimum())
		#print "yMin: " + str(rectangle.yMinimum())
		#print "xMax: " + str(rectangle.xMaximum())
		#print "yMax: " + str(rectangle.yMaximum())
		
		#print "Full string: " + str(rectangle.xMinimum()) + ","  + str(rectangle.yMinimum()) + "," + str(rectangle.xMaximum()) + "," + str(rectangle.yMaximum())
		print boundBox
		return boundBox
		
	def defineExtent(self):
		rectangle = self.mapRenderer.fullExtent()
		rectangle.scale(self.scale)
		self.mapRenderer.setExtent(rectangle)
		
		return self
	
	#This creates a map with categorized labels.
	def catMap(self):
		self.loadVectorLayer()
		
		#featureNum = self.layer.featureCount()
		#print featureNum
		#colorRamp = QgsVectorRandomColorRampV2(featureNum,0,359,100,240,200,240)
		#self.vectorRenderer = QgsCategorizedSymbolRendererV2()
		#self.vectorRenderer.setSourceColorRamp(colorRamp)
		
		#self.layer.setRendererV2(self.vectorRenderer)
		
		self.colorCatFeatures()
		if (self.showLabel): self.addLabel()
		rectangle = self.mapRenderer.fullExtent()
		rectangle.scale(self.scale)
		self.mapRenderer.setExtent(rectangle)
		self.mapRenderer.setLabelingEngine(QgsPalLabeling())
		img = QImage(self.width, self.height, QImage.Format_ARGB32_Premultiplied)
		self.mapRenderer.setOutputSize(img.size(), img.logicalDpiX())
		self.createComposition()
		self.createComposerMap()
		#self.addCompositionLegend()
		self.setUpImage()
		self.renderImage(self.targetArea, self.sourceArea)
		try:
			self.image.save(self.output, "png")
		
		#Saves the image
		except:
			print "Unexpected error saving image.", sys.exc_info()[ 0 ]
			raise
		
		
		
		self.close()
		
	def testMap(self):
		
		self.loadVectorLayer()
			
		self.colorCatFeatures()
		self.addLabel()
		self.layer.selectAll()
		rectSource = self.layer.boundingBoxOfSelected() 
		self.layer.invertSelection()
		#self.layer.boundingBoxOfSelected()
		rectangle = self.mapRenderer.fullExtent()
		 
		rectangle.scale(self.scale)
		self.mapRenderer.setExtent(rectangle)
		self.mapRenderer.setLabelingEngine(QgsPalLabeling())
		scale = rectSource.width()/rectSource.height()
		self.createComposition()
		img = QImage(QSize(self.width, self.height), QImage.Format_ARGB32_Premultiplied)
		
		print "color"
		i = 0
		j = 0
		print "height", img.height()
		print "width", img.width()
		print "count", img.colorCount()
		print "total", img.height() * img.width()
		#scans vertical lines
		while (j <= img.width()):
			while (i < img.height()):
				if (img.color(i*img.width() + j - 1) != 0): 
					print "has color"
					
				print i*img.width() + j

				i += 1
			if(i == img.height() -1 ): print " does not have color"
			i = 0
			j +=1
			
		print "worked"
		
		self.mapRenderer.setOutputSize(img.size(), img.logicalDpiX())
		

		self.createComposerMap()
		#self.addCompositionLegend()
		self.setUpImage()
		self.sourceArea = QRectF(0, 0, rectSource.width(), rectSource.height())
		self.targetArea = QRectF(0, 0, rectSource.width()*self.composition.printResolution()/25.4,rectSource.height()*self.composition.printResolution()/25.4)
		print rectSource.width()
		print rectSource.height()
		self.renderImage(self.targetArea, self.sourceArea)
		try:
			self.image.save(self.output, "png")
		
		#Saves the image
		except:
			print "Unexpected error saving image.", sys.exc_info()[ 0 ]
			raise
		
		#Saves the legend.
		
		print "Label"
		#self.addLabel()
		rectangle = QgsRectangle(0,0,25,25)
		rectangle.scale(self.scale)
		self.mapRenderer.setExtent(rectangle)
		self.mapRenderer.setLabelingEngine(QgsPalLabeling())
		img = QImage(QSize(self.width, self.height), QImage.Format_ARGB32_Premultiplied)
		#self.mapRenderer.setOutputSize(img.size(), img.logicalDpiX())
		self.createComposition()
		#self.createComposerMap()
		self.addCompositionLegend()
		self.setUpImage()
		LegendSizeX = 25
		LegendSizeY = 75
		self.sourceArea = QRectF(0, 0, LegendSizeX, LegendSizeY)
		self.targetArea = QRectF(0, 0, LegendSizeX*self.composition.printResolution()/25.4, LegendSizeY*self.composition.printResolution()/25.4)
		self.renderImage(self.targetArea, self.sourceArea)
		try:
			self.image.save("E://QGisTestImages//testLabel.png",  "png")
		
		#Saves the image
		except:
			print "Unexpected error saving image.", sys.exc_info()[ 0 ]
			raise
		
		print "done"
		self.close()
	
	#This creates a graduatedMap
	def gradMap(self):
		
		self.loadVectorLayer()
		
		self.colorGradFeatures()
		if (self.showLabel): self.addLabel()
		rectangle = self.mapRenderer.fullExtent()
		rectangle.scale(self.scale)
		self.mapRenderer.setExtent(rectangle)
		self.mapRenderer.setLabelingEngine(QgsPalLabeling())
		img = QImage(QSize(self.width, self.height), QImage.Format_ARGB32_Premultiplied)
		self.mapRenderer.setOutputSize(img.size(), img.logicalDpiX())
		self.createComposition()
		self.createComposerMap()
		#self.addCompositionLegend()
		self.setUpImage()
		self.renderImage(self.targetArea, self.sourceArea)
		try:
			self.image.save(self.output, "png")
		
		#Saves the image
		except:
			print "Unexpected error saving image.", sys.exc_info()[ 0 ]
			raise
		
		self.close()
		
	#This creates a a test map from a qml file
	def QMLMap(self):
		#pdb.set_trace()
		#proj = QgsCoordinateReferenceSystem(4326, QgsCoordinateReferenceSystem.EpsgCrsId)
		#self.mapRenderer.setDestinationCrs(proj)
		
		replaceString = 'renderer-v2 attr="' + self.featureName + '"'
		
		#print "TMS?"
		#print self.TMSFile
		#if not self.TMSFile == False: 
		#	self.loadRasterLayerWithTMS()
		#	print "rasterLoaded"
			
		self.loadVectorLayer()
		#pdb.set_trace()
		#boundBox = self.getExtentBoundBox()
		#print self.layer.crs().authid()
		
		#self.layer.setCrs(proj) 
		#self.layer.updateExtents()
		#boundBox = self.getExtentBoundBox()
		#pdb.set_trace()
		#bBox = self.mapRenderer.layerExtentToOutputExtent(self.layer, self.layer.extent())
		#print bBox
		#print self.mapRenderer.destinationCrs().authid()
		#print self.layer.crs().authid()
		#var = raw_input("pause")

		#Attempts to edit the QMLFile
		try:
			self.replaceFile(self.QMLFile, 'renderer-v2 attr="xxxxx"', replaceString)
		except:
			print "Unexpected error loading QML file: ", sys.exc_info()[ 0 ]
			print "File was: " + self.QMLFile
			raise
		
		#Attempts to load the QML File
		try:
			self.layer.loadNamedStyle(self.QMLFile)
			self.layer.loadNamedStyle("C:/dev/leaf-apps/src/util/sgis/qgis/examples/simple/style.qml")
		except:
			print "Unexpected error loading QML file: ", sys.exc_info()[ 0 ]
			print "File was: " + self.input
			raise
		
		
		#pdb.set_trace()
		self.replaceFile(self.QMLFile, replaceString, 'renderer-v2 attr="xxxxx"')
		self.layer.setLayerTransparency(self.VLayerTransparency)
		
		#print "mapRegistry count: " + str(QgsMapLayerRegistry.instance().count())
		#print "tranparency value"
		#print self.layer.layerTransparency()
		if (self.showLabel): self.addLabel()
		
		#Prints the extent to the console.
		#This is used for testing.
		#self.findExtent()
		
		#pdb.set_trace()
		self.mapRenderer.setLabelingEngine(QgsPalLabeling())
		img = QImage(QSize(self.width, self.height), QImage.Format_ARGB32_Premultiplied)
		
		#img = QImage("C:/dev/leaf-apps/src/utils/gis/qgis/scripts/NorthLatitude42.9762446555WestLongitude-93.096323014SouthLatitude42.953916321EastLongitude-93.0836200721.tiff", "TIFF")
		#img = QImage("wmsTestDownload.tiff", "TIFF")
		#img = QImage("E://Downloads/getplacemap2.jpg")
		#color = QColor(100,0,255)
		#img.fill(color.rgb())
		
		self.mapRenderer.setOutputSize(img.size(), img.logicalDpiX())
		self.layer.setLayerTransparency(self.VLayerTransparency)
		self.createComposition()
		self.createComposerMap()
		self.setUpImage()
		self.renderImage(self.targetArea, self.sourceArea)
		
		#print "The image was rendered."
		
		try:
			self.image.save(self.output, "png")
		
		#Saves the image
		except:
			print "Unexpected error saving image.", sys.exc_info()[ 0 ]
			raise
		
		self.close()
	
	def __init__(self, CommandLineArgs):
		#initialize QGIS
		#pdb.set_trace()
		if (sys.platform == "win32"):
			QgsApplication.setPrefixPath( r"C:\OSGeo4W64\apps\qgis", True )
		else:
			QgsApplication.setPrefixPath( r"/usr/", True )
		QgsApplication.initQgis()
		self.app = QgsApplication([], True)	
		
		#This is the map renderer.
		self.mapRenderer = QgsMapRenderer()
		
		#This sets some variables to false that I don't think I will always need.
		#This holds the composition.
		self.composition = False
		#This should hold the features for the renderer
		self.layerset = []
		#This holds the image
		self.image = False
		
		#Get information from the commandLine.
		#Default value is None
		#This controls how labels are placed.
		self.label = CommandLineArgs.label
		#Input vector Layer
		self.input = CommandLineArgs.input
		#Output image
		self.output = CommandLineArgs.output
		#Attribute name that will be labelled
		self.featureName = CommandLineArgs.featureName
		#Controls what map type is created.
		self.mapType = CommandLineArgs.mapType
		#Controls the width of the image
		self.width = int(CommandLineArgs.width)
		#Controls the height of the image
		self.height = int(CommandLineArgs.height)
		#Controls the scale factor for the image
		self.scale = float(CommandLineArgs.scale)
		#Controls the size of the label on the image
		self.labelSize = str(CommandLineArgs.labelSize)
		#Controls whether the label is shown.
		self.showLabel = str(CommandLineArgs.showLabel)
		#Controls whether TMS is used and stores the file location if it is used..
		self.TMSFile = str(CommandLineArgs.TMSFile)
		#Controls the layer transparency.
		self.VLayerTransparency = int(CommandLineArgs.VLayerTransparency)
		#pdb.set_trace()
		#Controls the layer transparency.
		self.textLabel = str(CommandLineArgs.textLabel)
		
		if (self.textLabel == "True"): 
			self.textLabel = True
		elif (self.textLabel == "False"):
			self.textLabel = False
		
		if (self.showLabel == "True"): 
			self.showLabel = True
		elif (self.showLabel == "False"):
			self.showLabel = False
		#Control the number of graduated symbols
		if (CommandLineArgs.gradCatNum != None): self.gradCatNum = int(CommandLineArgs.gradCatNum)
		else: self.gradCatNum = None
		#The QML file that is loaded. Only necessary for a qml map.
		if (CommandLineArgs.QMLFile != None): self.QMLFile = CommandLineArgs.QMLFile
		else: self.QMLFile = None
		#Specifies whether test data is being loaded.
		if (CommandLineArgs.test == 'True'): self.test = True
		else: self.test = False
		if (self.TMSFile == "False"): 
			self.TMSFile = False
		
		#Sets ups data for testing script
		if (self.test):
			if (self.output == None): self.output = "E:/QGisTestImages/classTest.png"
			if (self.featureName == None): self.featureName= "musym"
			if (self.input == None): self.input = "E:/Dropbox (Praxik)/BMAS/January 2014 Case Study/field data/soils/field_1_soils.shp"
			if (self.mapType == None): self.mapType = "graduated"
			if (self.gradCatNum == None): self.gradCatNum = 3
			if (self.QMLFile == None): self.QMLFile = "E:/QGisTestImages/yieldSoybeanProfitTest.qml"

		#Creates the map
		if (self.mapType == "categorized"): 
			self.catMap()
			print "A categorized map was created."
		if (self.mapType == "graduated"): 
			self.gradMap()
			print "A graduated map was created."
		if (self.mapType == "QML"): 
			self.QMLMap()
			print "A map was created using a QML style sheet."
		#if (self.mapType == "test"): 
			#self.testMap()
			#print "A test map was created."
		

		
class OldMapSymbol:
#stores information for map symbols.
	#chooses the fill style, I expect I'll add more options later.
	def __init__(self, red, green, blue, style = "fill"):
		if (style == "fill"):
			self.symbol = QgsFillSymbolV2()
		else:
			self.symbol = QgsFillSymbolV2()
	
		self.symbol.setColor(QColor(red, green, blue))
		
	def get(self):
		return self.symbol

class MapSymbolHSV:
#stores information for map symbols.
	#style controls the fill style, right now it is unused. I expect I'll add options to it later.
	def __init__(self, hue, sat, value, style = "fill", styleBorder = 'solid', styleWidth = '0.26'):
		#Default values create a symbol with no border.
		map = dict()
		
		map['width_border'] = styleWidth
		map['style_border'] = styleBorder
		
		self.symbol = QgsFillSymbolV2.createSimple(map)
		self.symbol.setColor(QColor.fromHsv(hue,sat,value))
		
	def get(self):
		return self.symbol
		
class MapSymbol:
#stores information for map symbols.
	#style controls the fill style, right now it is unused. I expect I'll add options to it later.
	def __init__(self, red, green, blue, style = "fill", styleBorder = 'solid', styleWidth = '0.26'):
		#Default values create a symbol with no border.
		map = dict()
		
		map['width_border'] = styleWidth
		map['style_border'] = styleBorder
		colorVal = str(red) + ',' + str(green) + ',' + str(blue)
		
		self.symbol = QgsFillSymbolV2.createSimple(map)
		self.symbol.setColor(QColor(red, green, blue))
		
	def get(self):
		return self.symbol
		

#Having two options for each variable, var and --var, with the stated argument options allow for positional and explicit declarations.
#initialize the argument parser
argInput = argparse.ArgumentParser()
#Add argument options.
argInput.add_argument("input", help = "Input the location of the shapefile.", default=argparse.SUPPRESS, nargs='?')
argInput.add_argument("--input", help = "Input the location of the shapefile.", dest = "input", default =None)

argInput.add_argument("output", help = "Input where to save the image.", default=argparse.SUPPRESS, nargs='?')
argInput.add_argument("--output", help = "Input where to save the image.", dest = "output", default =None)

argInput.add_argument("label", help = "Control how labels are displayed.", default=argparse.SUPPRESS, nargs='?')
argInput.add_argument("--label", help = "Control how labels are displayed.", dest = "label", default =None)

argInput.add_argument("featureName", help = "Control what attribute is labelled.", default=argparse.SUPPRESS, nargs='?')
argInput.add_argument("--featureName", help = "Control what attribute is labelled.", dest = "featureName", default =None)

argInput.add_argument("mapType", help = "Control what map type is created.", default=argparse.SUPPRESS, nargs='?')
argInput.add_argument("--mapType", help = "Control what map type is created.", dest = "mapType", default =None)

argInput.add_argument("gradCatNum", help = "How many graduated symbols do you want for a graduated map?", default=argparse.SUPPRESS, nargs='?')
argInput.add_argument("--gradCatNum", help = "How many graduated symbols do you want for a graduated map?", dest = "gradCatNum", default =None)

argInput.add_argument("QMLFile", help = "Input location for the QML style file. This is neccessary for a QML type map.", default=argparse.SUPPRESS, nargs='?')
argInput.add_argument("--QMLFile", help = "Input location for the QML style file.", dest = "QMLFile", default =None)

argInput.add_argument("test", help = "Input 'True' to use test defaults.", default=argparse.SUPPRESS, nargs='?')
argInput.add_argument("--test", help = "Input 'True' to use test defaults.", dest = "test", default =None)

argInput.add_argument("width", help = "Width of the image in pixels.", default=argparse.SUPPRESS, nargs='?')
argInput.add_argument("--width", help = "Width of the image in pixels.", dest = "width", default =390)

argInput.add_argument("height", help = "Height of the image in pixels.", default=argparse.SUPPRESS, nargs='?')
argInput.add_argument("--height", help = "Height of the image in pixels.", dest = "height", default =390)

argInput.add_argument("labelSize", help = "Size of the labels.", default=argparse.SUPPRESS, nargs='?')
argInput.add_argument("--labelSize", help = "Size of the labels.", dest = "labelSize", default =30)

argInput.add_argument("showLabel", help = "A bool that controls whether maps are labelled.", default=argparse.SUPPRESS, nargs='?')
argInput.add_argument("--showLabel", help = "True and False are the valid inputs.", dest = "showLabel", default ="True")

argInput.add_argument("scale", help = "Factor used to scale the image around the center point.", default=argparse.SUPPRESS, nargs='?')
argInput.add_argument("--scale", help = "1.0 is the full image with the least amount of whitespace, < 1.0 cuts some of the image off, and > 1.0 increases whitespace.", dest = "scale", default = "1.06")

argInput.add_argument("TMSFile", help = "Input location for the XML file for using TMS. Leave blank if you are not using TMS.", default=argparse.SUPPRESS, nargs='?')
argInput.add_argument("--TMSFile", help = "Input location for the XML file for using TMS.", dest = "TMSFile", default ="False")

argInput.add_argument("VLayerTransparency", help = "Controls the opacity. Leave blank if you do not want to make the layer transparent.", default=argparse.SUPPRESS, nargs='?')
argInput.add_argument("--VLayerTransparency", help = "Controls the opacity. Leave blank if you do not want to make the layer transparent.", dest = "VLayerTransparency", default =0)

argInput.add_argument("textLabel", help = "Controls whether numbers are rounded.", default=argparse.SUPPRESS, nargs='?')
argInput.add_argument("--textLabel", help = "Controls whether numbers are rounded.", dest = "textLabel", default="False")


#Create the object with the option variables.
#pdb.set_trace()
args = argInput.parse_args()

vector = GeoLayers(args)

print "Image was successfully rendered."




