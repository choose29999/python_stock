from PIL import Image
import numpy
import sys
from skimage.measure import label
from skimage.measure import regionprops
from pytesseract import image_to_string
# background color is black 0
# word is white color 255
def dialation(imgArray,value):
	height, width = imgArray.shape
	#imgCpy=numpy.zeros((height, width), dtype=numpy.uint8)
	imgCpy=imgArray.copy()
	for y in range(1,height-1):
		for x in range(1,width-1):
			if imgArray[y][x] == value:
				imgCpy[y-1][x-1]=imgCpy[y-1][x  ]=imgCpy[y+1][x+1]=value
				imgCpy[y  ][x-1]=imgCpy[y  ][x  ]=imgCpy[y  ][x+1]=value
				imgCpy[y-1][x-1]=imgCpy[y+1][x  ]=imgCpy[y+1][x+1]=value
				
	return imgCpy
	
def replaceValue(imgArray,Ymin,Ymax,Xmin,Xmax,label,value):
	for y in range(Ymin,Ymax):
		for x in range(Xmin,Xmax):
			imgArray[y][x]==value
	return imgArray
	
def smoothImg(imgArray):
	height, width = imgArray.shape
	imgCpy=numpy.zeros((height, width), dtype=numpy.uint8)
	for y in range(1,height-1):
		for x in range(1,width-1):
			cnt=0
			if imgArray[y-1][x-1]==0:
				cnt+=1
			if imgArray[y-1][x  ]==0:
				cnt+=1
			if imgArray[y+1][x+1]==0:
				cnt+=1
			if imgArray[y  ][x-1]==0:
				cnt+=1
			if imgArray[y  ][x  ]==0:
				cnt+=1
			if imgArray[y  ][x+1]==0:
				cnt+=1
			if imgArray[y-1][x-1]==0:
				cnt+=1
			if imgArray[y+1][x  ]==0:
				cnt+=1
			if imgArray[y+1][x+1]==0:
				cnt+=1
			if cnt>=4:
				imgCpy[y][x]=0
			else:
				imgCpy[y][x]=255
	return imgCpy
	
def dowmsampling(imgArray,scale):
	height, width = imgArray.shape
	newHeight = int(height/scale)
	newWidth = int(width/scale)
	imgNew=numpy.zeros((newHeight, newWidth), dtype=numpy.uint8)
	for y in range(0,newHeight):
		for x in range(0,newWidth):
			imgNew[y][x]=imgArray[y*scale][x*scale]
	return imgNew
			
def replaceColor(imgArray,ori,rep):
	height, width = imgArray.shape
	for y in range(0,height):
		for x in range(0,width):
			if imgArray[y][x] == ori:
				imgArray[y][x] == rep
	return imgArray

def averageImg(imgArray):
	height, width = imgArray.shape
	imgNew=numpy.zeros((height, width), dtype=numpy.uint8)
	for y in range(1,height-2):
		for x in range(1,width-2):
			imgNew[y][x]=int((imgArray[y-1][x-1]+imgArray[y-1][x]+imgArray[y-1][x+1]+imgArray[y][x-1]+imgArray[y][x]+imgArray[y][x+1]+imgArray[y+1][x-1]+imgArray[y+1][x]+imgArray[y+1][x+1])/9)
	return imgNew
		
def getMagicNumberFromArray( imgArray,width,height ):
	imgArray=averageImg(imgArray)
	for y in range(0,height):
		for x in range(0,width):
			if imgArray[y][x]<=180 :
				imgArray[y][x]=0
			else:
				imgArray[y][x]=255

	#down sample
	#imgArray=dowmsampling(imgArray,2)
	#height, width = imgArray.shape


	#imgArray=smoothImg(imgArray)
	#imgArray=dialation(imgArray)
	imgArray=label(imgArray,connectivity=1)
	removeLabel=[0]
	for object in regionprops(imgArray):
		if object.area < 50 :
			removeLabel.append(object.label)
	for y in range(0,height):
		for x in range(0,width):
			if imgArray[y][x] in removeLabel:
				imgArray[y][x]=0
			else:
				imgArray[y][x]=255
				
	imgArray=dialation(imgArray,0)
	imgArray=dialation(imgArray,255)

	imgArray=label(imgArray,connectivity=1)
	removeLabel=[0]
	for object in regionprops(imgArray):
		if object.area < 100 :
			removeLabel.append(object.label)
	for y in range(0,height):
		for x in range(0,width):
			if imgArray[y][x] in removeLabel:
				imgArray[y][x]=255
			else:
				imgArray[y][x]=0
	'''	
	#remove vertical pixel
	for x in range(0,width):
		cnt = 0
		for y in range(0,height):
			if imgArray[y][x]==255:
				cnt+=1
		if cnt<4:
			for y in range(0,height):
				if imgArray[y][x]==255:
					imgArray[y][x]=0

	#imgArray=dialation(imgArray,255)
	'''
	imgCpy=numpy.zeros((height, width, 3), dtype=numpy.uint8)
	for y in range(0,height):
		for x in range(0,width):
			imgCpy[y][x]=[imgArray[y][x]]*3
				
	img = Image.fromarray(imgCpy, 'RGB')
	magic=image_to_string(img)
	magicNumber=''
	for char in magic:
		if 60<=ord(char)<=90 or 48<=ord(char)<=57:
			magicNumber+=str(char)
	return magicNumber


def getMagicNumberFromFile( filePath ):
	try:
		im = Image.open(filePath).convert('L')
	except:
		return -1
	(width, height) = im.size

	#dump to array
	imgArray = list(im.getdata())
	imgArray = numpy.array(imgArray)
	imgArray = imgArray.reshape((height, width))

	imgArray=averageImg(imgArray)
	for y in range(0,height):
		for x in range(0,width):
			if imgArray[y][x]<=180 :
				imgArray[y][x]=0
			else:
				imgArray[y][x]=255

	#down sample
	#imgArray=dowmsampling(imgArray,2)
	#height, width = imgArray.shape


	#imgArray=smoothImg(imgArray)
	#imgArray=dialation(imgArray)
	imgArray=label(imgArray,connectivity=1)
	removeLabel=[0]
	for object in regionprops(imgArray):
		if object.area < 50 :
			removeLabel.append(object.label)
	for y in range(0,height):
		for x in range(0,width):
			if imgArray[y][x] in removeLabel:
				imgArray[y][x]=0
			else:
				imgArray[y][x]=255
				
	imgArray=dialation(imgArray,0)
	imgArray=dialation(imgArray,255)

	imgArray=label(imgArray,connectivity=1)
	removeLabel=[0]
	for object in regionprops(imgArray):
		if object.area < 100 :
			removeLabel.append(object.label)
	for y in range(0,height):
		for x in range(0,width):
			if imgArray[y][x] in removeLabel:
				imgArray[y][x]=255
			else:
				imgArray[y][x]=0
	'''	
	#remove vertical pixel
	for x in range(0,width):
		cnt = 0
		for y in range(0,height):
			if imgArray[y][x]==255:
				cnt+=1
		if cnt<4:
			for y in range(0,height):
				if imgArray[y][x]==255:
					imgArray[y][x]=0

	#imgArray=dialation(imgArray,255)
	'''
	imgCpy=numpy.zeros((height, width, 3), dtype=numpy.uint8)
	for y in range(0,height):
		for x in range(0,width):
			imgCpy[y][x]=[imgArray[y][x]]*3
				
	img = Image.fromarray(imgCpy, 'RGB')
	magic=image_to_string(img)
	magicNumber=''
	for char in magic:
		if 60<=ord(char)<=90 or 48<=ord(char)<=57:
			magicNumber+=str(char)
	return magicNumber
	
def getMagicNumberFromImageObject( im ):

	(width, height) = im.size

	#dump to array
	imgArray = list(im.getdata())
	imgArray = numpy.array(imgArray)
	imgArray = imgArray.reshape((height, width))

	imgArray=averageImg(imgArray)
	for y in range(0,height):
		for x in range(0,width):
			if imgArray[y][x]<=180 :
				imgArray[y][x]=0
			else:
				imgArray[y][x]=255

	#down sample
	#imgArray=dowmsampling(imgArray,2)
	#height, width = imgArray.shape


	#imgArray=smoothImg(imgArray)
	#imgArray=dialation(imgArray)
	imgArray=label(imgArray,connectivity=1)
	removeLabel=[0]
	for object in regionprops(imgArray):
		if object.area < 50 :
			removeLabel.append(object.label)
	for y in range(0,height):
		for x in range(0,width):
			if imgArray[y][x] in removeLabel:
				imgArray[y][x]=0
			else:
				imgArray[y][x]=255
				
	imgArray=dialation(imgArray,0)
	imgArray=dialation(imgArray,255)

	imgArray=label(imgArray,connectivity=1)
	removeLabel=[0]
	for object in regionprops(imgArray):
		if object.area < 100 :
			removeLabel.append(object.label)
	for y in range(0,height):
		for x in range(0,width):
			if imgArray[y][x] in removeLabel:
				imgArray[y][x]=255
			else:
				imgArray[y][x]=0
	'''	
	#remove vertical pixel
	for x in range(0,width):
		cnt = 0
		for y in range(0,height):
			if imgArray[y][x]==255:
				cnt+=1
		if cnt<4:
			for y in range(0,height):
				if imgArray[y][x]==255:
					imgArray[y][x]=0

	#imgArray=dialation(imgArray,255)
	'''
	imgCpy=numpy.zeros((height, width, 3), dtype=numpy.uint8)
	for y in range(0,height):
		for x in range(0,width):
			imgCpy[y][x]=[imgArray[y][x]]*3
				
	img = Image.fromarray(imgCpy, 'RGB')
	magic=image_to_string(img)
	magicNumber=''
	for char in magic:
		if 60<=ord(char)<=90 or 48<=ord(char)<=57:
			magicNumber+=str(char)
	return magicNumber
	
#print magicNumber
#img.save('testnumber.jpeg')
#img.show()