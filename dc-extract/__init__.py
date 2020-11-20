from azure.data.tables import TableClient
from azure.storage.fileshare import ShareFileClient
from opencensus.ext.azure.log_exporter import AzureLogHandler

import azure.functions as func
import csv
import datetime
import logging
import os
import sys
import tempfile

# global variables
DC_CONNECTION_STRING = "NOTSET"
FS_CONNECTION_STRING = "NOTSET"
AI_CONNECTION_STRING = "NOTSET"
DC_TABLE_NAME = "NOTSET"
LOGGER_LEVEL = "NOTSET"
CATALOG_FILENAME = "NOTSET"
TABLE_FILTER = "PartitionKey eq '*'"
SHARE_NAME = "NOTSET"

def upload_file(filename, logger):
	# JRG - NEED TO FIGURE OUT HOW TO HANDLE GROWTH OF FILES IN SHARE
	global FS_CONNECTION_STRING, SHARE_NAME

	# strip path from passed in filename...just filename for file_path below
	filepath, just_filename = os.path.split(filename)

	# add timestamp so we don't overwrite existing version.
	now = datetime.datetime.now()
	dt = now.strftime("%Y%m%d%H%M%S")
	filename_components = just_filename.split('.')
	just_filename = filename_components[0] + "-" + dt + "." + filename_components[1] 
	logger.info("Uploading file as (%s)", just_filename)

	logger.info("Connecting to file share...")
	# connect to fileshare
	try:
		file = ShareFileClient.from_connection_string(FS_CONNECTION_STRING, share_name=SHARE_NAME, file_path=just_filename)
	except (NameError) as err:
		logger.error("Failed to connect to file share (%s), exiting...", FS_CONNECTION_STRING)
		logger.error(str(err))
		return False

	logger.info("Uploading file (%s)...", filename)
	# Upload a file
	try:
		with open(filename, "rb") as source:
			file.upload_file(source)
	except:
		logger.error("Failed to upload file (%s), exiting...", filename)
		logger.error("ERROR: " + str(sys.exc_info()[0]))
		return False

	logger.info("File successfully uploaded (%s)", filename)
	return True

def extract_catalog_data(logger):
	global DC_TABLE_NAME, DC_CONNECTION_STRING, TABLE_FILTER

	# create local/temp filename (absolute path)
	logger.info("Creatinmg local file name...")
	local_temp_path = tempfile.gettempdir()
	local_file = os.path.abspath(local_temp_path + "/" + CATALOG_FILENAME)
	logger.info("Local file path (%s)", local_file)

	# remove file, if it exists
	if os.path.exists(local_file):
		logger.info("Removing existing catalog file (%s)...", local_file)
		try:
			os.remove(local_file)
		except:
			logger.error("Failed to remove existing catalog file (%s)...", local_file)
			logger.error("ERROR: " + str(sys.exc_info()[0]))
			return None

	# connect to table
	logger.info("Connecting to DataCatalog table (%s), connect string (%s)", DC_TABLE_NAME, DC_CONNECTION_STRING)
	try:
		table_client = TableClient.from_connection_string(conn_str=DC_CONNECTION_STRING, table_name=DC_TABLE_NAME)
	except NameError as err:
		logger.error("Failed to connect to Data Catalog table (%s), exiting...", DC_TABLE_NAME)
		logger.error("ERROR: " + str(err))
		return None
	except:
		logger.error("Failed to connect to Data Catalog table (%s), exiting...", DC_TABLE_NAME)
		logger.error("ERROR: " + str(sys.exc_info()[0]))
		return None

	# read entries
	# JRG - Add exceptions...
	# JRG - Will ev entually need to add in paging logic as catalog table grows
	entites = "NOTSET"
	logger.info("Reading table entries (%s)", DC_TABLE_NAME)
	try:
		entities = table_client.query_entities(filter=TABLE_FILTER)
	except:
		logger.error("Failed to read Data Catalog table (%s) extries, exiting...", DC_TABLE_NAME)
		logger.error("ERROR: " + str(sys.exc_info()[0]))
		return None

	# open local file
	logger.info("Opening local file (%s)...", local_file)
	try:
		csvfile = open(local_file, "w")
	except:
		logger.error("Failed to open new catalog file (%s)...", local_file)
		logger.error("ERROR: " + str(sys.exc_info()[0]))
		return None
   
	# initialize CSV file
	logger.info("Initializing local file (%s)", local_file)
	now = datetime.datetime.now()
	dt = now.strftime("%Y/%m/%d-%H:%M:%S")
	try:
		catalog_writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
	except:
		logger.error("Failed to initialize CSV file (%s)", local_file)
		logger.error("ERROR: " + str(sys.exc_info()[0]))
		return None
   
	try:
		catalog_writer.writerow(['DS Data Catalog', dt])
		catalog_writer.writerow(['--------------------------------------', '-----------------------'])
	except (TypeError | NameError)  as err:
		logger.error("Failed in write file header rows (%s)", local_file)
		logger.error("ERROR: %s", str(err))
		return None
   
	# write header row
	logger.info("Writing header row...")
	row = ['Data Source', 'Description', 'Data Generator', 'Source POC', 'Source Container', 'POC Contact Info', 'Soure URI', 'Data Owner', 'Retention (Hot)', 'Retention (Cool)', 'Retention (Delete)', 'Sensitivity', 'TimeStamp']

	try:
		catalog_writer.writerow(row)
		catalog_writer.writerow([''])
	except (TypeError | NameError)  as err:
		logger.error("Failed in write column header rows (%s)", local_file)
		logger.error("ERROR: %s", str(err))
		return None

	logger.info("RV:" + str(entities))
	# build/write to CSV file
	rows_written = 0
	logger.info("Writing rows...")
	logger.info("***(%s)***", entities)
	for entity in entities:
		logger.info("IN LOOP...")
		logger.info("ENTRY: (%s)", str(entity))
		if entity.RowKey != "instantiation_row":
			logger.info("Building row!")
			# build CSV row.
			try:
				row = []
				row.append(str(entity.data_source.value))
				row.append(str(entity.description.value))
				row.append(str(entity.data_generator.value))
				row.append(str(entity.data_source_poc.value))
				row.append(str(entity.data_source_container.value))
				row.append(str(entity.data_source_poc_contact_info.value))
				row.append(str(entity.data_source_uri.value))
				row.append(str(entity.ds_data_owner.value))
				row.append(str(entity.retention_period_hot.value))
				row.append(str(entity.retention_period_cool.value))
				row.append(str(entity.retention_period_delete.value))
				row.append(str(entity.sensitivity.value))
				row.append(str(entity._metadata['timestamp']))
			except:
				logger.error("Failed to build row for data source (%s), exiting...", entity.data_source.value)
				logger.error("ERROR: %s", str(err))
				return None
   
			logger.info("Building row!")
			try:
				catalog_writer.writerow(row)
				rows_written = rows_written + 1
			except (TypeError | NameError)  as err:
				logger.error("Failed to write row for data source (%s), exiting...", entity.data_source.value)
				logger.error("ERROR: %s", str(err))
				return None

	logger.info("Total rows written (%s)", str(rows_written))
	return local_file

def get_environment(logger):
	global LOGGER_LEVEL, DC_CONNECTION_STRING, FS_CONNECTION_STRING, AI_CONNECTION_STRING
	global SHARE_NAME, DC_TABLE_NAME, CATALOG_FILENAME

	# link logger to AI
	AI_CONNECTION_STRING = os.getenv("AI_CONNECTION_STRING", "NOTSET")
	logger.info("AI_CONNECTION_STRING (%s)", AI_CONNECTION_STRING)
	if AI_CONNECTION_STRING == "NOTSET":
		logger.error("No AI_CONNECTION_STRING provided, exiting...")
		return -1

	# get passed LOGGER_LEVEL value and update logger
	LOGGER_LEVEL = os.getenv("LOGGER_LEVEL", "NOTSET")
	logger.info("LOGGER_LEVEL (%s)", LOGGER_LEVEL)
	if LOGGER_LEVEL == "NOTSET":
		logger.warning("Logging level set to default (WARNING)")
		LOGGER_LEVEL = "WARNING"
		log_level = logging.WARNING
	else:
		valid_levels = {"DEBUG": logging.DEBUG,
				"INFO": logging.INFO,
				"WARNING": logging.WARNING,
				"ERROR": logging.ERROR,
				"CRITICAL": logging.CRITICAL }

		logger.info("Validating LOGGER_LEVEL value...")
		if LOGGER_LEVEL in valid_levels:
			log_level = valid_levels[LOGGER_LEVEL]
			logger.info("got LOGGER_LEVEL of (%s)", LOGGER_LEVEL)
		else:
			logger.error("Invalid LOGGER_LEVEL provided (%s), defaulting to WARNING", LOG_LEVEL)
			log_level = logging.WARNING
			LOGGER_LEVEL = WARNING

	logger.info("Setting LOG_LEVEL to (%s)", LOGGER_LEVEL)

	# get all required ENV values
	DC_CONNECTION_STRING = os.getenv("DC_CONNECTION_STRING", "NOTSET")
	logger.info("DC_CONNECTION_STRING (%s)", DC_CONNECTION_STRING)
	if DC_CONNECTION_STRING == "NOTSET":
		logger.error("No DC_CONNECTION_STRING provided, exiting...")
		return -1
 
	FS_CONNECTION_STRING = os.getenv("FS_CONNECTION_STRING", "NOTSET")
	logger.info("FS_CONNECTION_STRING (%s)", FS_CONNECTION_STRING)
	if FS_CONNECTION_STRING == "NOTSET":
		logger.error("No FS_CONNECTION_STRING provided, exiting...")
		return -l
   
	DC_TABLE_NAME = os.getenv("DC_TABLE_NAME", "NOTSET")
	logger.info("DC_TABLE_NAME (%s)", DC_TABLE_NAME)
	if DC_TABLE_NAME == "NOTSET":
		logger.error("No DC_TABLE_NAME provided, exiting...")
		return -1
   
	CATALOG_FILENAME = os.getenv("CATALOG_FILENAME", "NOTSET")
	logger.info("CATALOG_FILENAME (%s)", CATALOG_FILENAME)
	if CATALOG_FILENAME == "NOTSET":
		logger.error("No CATALOG_FILENAME provided, exiting...")
		return -1

	SHARE_NAME = os.getenv("SHARE_NAME", "NOTSET")
	logger.info("SHARE_NAME (%s)", SHARE_NAME)
	if SHARE_NAME == "NOTSET":
		logger.error("No SHARE_NAME provided, exiting...")
		return -1

	return 0

def main(mytimer: func.TimerRequest) -> None:
	global LOGGER_LEVEL, DC_CONNECTION_STRING, FS_CONNECTION_STRING, AI_CONNECTION_STRING
	global SHARE_NAME, DC_TABLE_NAME, CATALOG_FILENAME

	# initialize logger
	logger = logging.getLogger(__name__)
	logger.info("dc-extract: starting...")

	utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
	if mytimer.past_due:
		logger.info("dc-extract: executed past due!")
	logger.info("dc-extract: Python timer trigger function ran at %s", utc_timestamp)

	if get_environment(logger) != 0:
		logger.info("dc-extract: Failed to get all required environment valiables")
	else:
		logger.info("dc-extract: All required environment valiables collected")

		# Extact catalog data
		local_filename = extract_catalog_data(logger)
		if local_filename is not None:
			# upload file
			logger.info("(%s) local filename returned", local_filename)
			if upload_file(local_filename, logger):
				logger.info("Sucessfully uploaded Data Catalog file (%s)", local_filename)

				# success!!!
				logger.info("Success!")
			else:
				logger.error("Failed to upload data catalog file (%s), exiting...", local_filename)
		else:
			logger.info("dc-extract: Failed to extract data catalog information")
