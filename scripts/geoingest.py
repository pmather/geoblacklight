import csv
import datetime
import hashlib
import json
import os
import requests
import shutil
import sys

from os import listdir
from os.path import isfile, join


def createreport(filename, content):

	with open(filename, 'wb') as f:
		f.write(content)


def getfilelist(folderpath):
	
	onlyfiles = [f for f in listdir(folderpath) if isfile(join(folderpath, f))]

	return onlyfiles


def getfileprefix(filename):
	if filename.startswith("dmf_"):
		return "dmf_"
	elif filename.startswith("cgit_"):
		return "cgit_"
	else:
		return ""


def getcontent(filename):
	
	with open(filename, 'rb') as f:
		reader = csv.reader(f)
		content = list(reader)
    
	return content


def geojson(input):
	
	content = {}
	m = hashlib.sha256()
	m.update(input[1])

	content['uuid'] = m.hexdigest()
	content['dc_identifier_s'] = input[1]
	content['dc_rights_s'] = input[2]
	content['dct_provenance_s'] = input[3]
	content['dct_references_s'] = "{\"http://schema.org/downloadUrl\":\"" + input[1] + "\",\"http://www.opengis.net/def/serviceType/ogc/wcs\":\"" + input[4] + "\"}"     
	content['dc_creator_sm'] = input[5]
	content['dc_language_s'] = input[6]
	content['dc_publisher_s'] = input[7]
	content['dc_type_s'] = input[9]
	content['dct_spatial_sm'] = input[10]
	content['dct_temporal_sm'] = input[11]
	content['dct_issued_s'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
	content['dct_isPartOf_sm'] = input[13]
	content['georss_box_s'] = input[14]
	gdata = input[14].split(",")
	content['solr_geom'] = "ENVELOPE("+ gdata[1] + "," + gdata[3] + "," + gdata[2] + "," + gdata[0] + ")"
	content['dc_title_s'] = input[17]
	content['dc_description_s'] = input[18]
	content['dc_format_s'] = input[19]
	content['dc_subject_sm'] = input[20]
	content['layer_id_s'] = input[21]
	content['layer_modified_dt'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
	content['layer_slug_s'] = input[23]
	content['layer_geom_type_s'] = input[24]

	return json.dumps(content)

def is_valid_url(url):
    import re
    regex = re.compile(
        r'^https?://'  
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  
        r'localhost|'  
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' 
        r'(?::\d+)?'  
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return url is not None and regex.search(url)


def validaterecord(input):

	result = ""
	if len(input[1]) == 0:
		result += "dc_identifier field is empty. "
	if is_valid_url(input[1]) is None:
		result += "dc_identifier field is not a valid URL. "
	if len(input[2]) == 0:
		result += "dc_rights field is empty. "
	if len(input[3]) == 0:
		result += "dct_provenance field is empty. "
	if len(input[4]) == 0:
		result += "dct_references field is empty. "
	if len(input[5]) == 0:
		result += "dc_creator field is empty. "
	if len(input[6]) == 0:
		result += "dc_language field is empty. "
	if len(input[7]) == 0:
		result += "dc_publisher field is empty. "
	if len(input[9]) == 0:
		result += "dc_type field is empty. "
	if len(input[10]) == 0:
		result += "dct_spatial field is empty. "
	if len(input[13]) == 0:
		result += "isPartOf field is empty. "
	if len(input[14]) == 0:
		result += "georss_box field is empty. "
	elif len(input[14].split(",")) != 4:
		result += "georss_box field is incorrect. "
	elif len([s for s in input[14].split(",") if s.strip().replace('.','').replace('-','').isdigit()]) != 4:
		result += "georss_box field should be all numbers. "
	if len(input[17]) == 0:
		result += "dc_title field is empty. "
	if len(input[18]) == 0:
		result += "dc_description field is empty. "
	if len(input[19]) == 0:
		result += "dc_format field is empty. "
	if len(input[21]) == 0:
		result += "layer_id field is empty. "
	if len(input[23]) == 0:
		result += "layer_slug field is empty. "
	if len(input[24]) == 0:
		result += "layer_geom_type field is empty. "

	return result

archivepath = "Archive/"
errorpath = "Report/Errors/"
logpath = "Report/Logs/"
uploadpath = "Upload/"

headers = {
    'Content-Type': 'application/json',
}

list_of_files = getfilelist(uploadpath)

for f in list_of_files:

	prefix = getfileprefix(f)
	if len(prefix) > 0 and os.path.isfile(uploadpath+f):

		records = getcontent(uploadpath+f)
		total = len(records) - 1
		errorcontent = ""

		s=0 
		r=1
		for l in records[1:]:

			try:				
				c = validaterecord(l)
				if len(c) > 0:
					errorcontent += "row" + str(r) + ":" + c + "\n\n"
				else:
					s += 1
					data=geojson(l)
					requests.post('http://localhost:8983/solr/blacklight-core/update/json/docs?commit=true', headers=headers, data=data)

			except Exception, e:
				errorcontent += "row" + str(r) + ":" + str(e)

			r += 1

		# create log report file
		logfilename = logpath + prefix + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + ".log.txt"
		logcontent = f + ": Total ingest records: " + str(total) + ", ingested " + str(s) + " records."
		createreport(logfilename, logcontent)

		# create error report file
		if (total - s) > 0:
			errorfilename = errorpath + prefix + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + ".error.txt"
			createreport(errorfilename, f + "\n" + errorcontent)

		# move file to archive folders 
		shutil.move(uploadpath + f, archivepath + f)

