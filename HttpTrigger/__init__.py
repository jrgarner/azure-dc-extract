from azure.data.tables import TableClient
from azure.storage.fileshare import ShareFileClient
from datetime import datetime
from opencensus.ext.azure.log_exporter import AzureLogHandler
import azure.functions as func
import csv
import logging
import os
import sys
import tempfile

#logger = logging.getLogger(__name__)
#logger.addHandler(AzureLogHandler(connection_string='InstrumentationKey=326628c9-ac40-43f5-84b0-5e8afc51d298'))

# global variables
DC_CONNECTION_STRING = "NOTSET"
FS_CONNECTION_STRING = "NOTSET"
AI_INSTRUMENTATION_KEY = "NOTSET"
DC_TABLE_NAME = "NOTSET"
LOGGER_LEVEL = "NOTSET"
CATALOG_FILENAME = "NOTSET"
TABLE_FILTER = "PartitionKey eq '*'"
SHARE_NAME = "NOTSET"

def upload_file(filename):
	# JRG - NEED TO FIGURE OUT HOW TO HANDLE GROWTH OF FILES IN SHARE
	global logger, FS_CONNECTION_STRING, SHARE_NAME

	# strip path from passed in filename...just filename for file_path below
	filepath, just_filename = os.path.split(filename)

	# add timestamp so we don't overwrite existing version.
	now = datetime.now()
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
		#sys.exit(1)

	logger.info("Uploading file (%s)...", filename)
	# Upload a file
	try:
		with open(filename, "rb") as source:
			file.upload_file(source)
	except:
		logger.error("Failed to upload file (%s), exiting...", filename)
		logger.error("ERROR: " + str(sys.exc_info()[0]))
		#sys.exit(1)

	logger.info("File upload complete (%s)", filename)
	return True

def extract_contents(local_filename):
	global logger, DC_TABLE_NAME, DC_CONNECTION_STRING, DC_TABLE_NAME, TABLE_FILTER
	# connect to table
	logger.info("Connecting to DataCatalog table (%s), connect string (%s)", DC_TABLE_NAME, DC_CONNECTION_STRING)
	try:
		table_client = TableClient.from_connection_string(conn_str=DC_CONNECTION_STRING, table_name=DC_TABLE_NAME)
	except:
		logger.error("Failed to connect to Data Catalog table (%s), exiting...", DC_TABLE_NAME)
		logger.error("ERROR: " + str(sys.exc_info()[0]))
		#sys.exit(1)

	# read entries
	# JRG - Add exceptions...
	logger.info("Reading table entries (%s)", DC_TABLE_NAME)
	try:
		entities = table_client.query_entities(filter=TABLE_FILTER)
	except:
		logger.error("Failed to read Data Catalog table (%s) extries, exiting...", DC_TABLE_NAME)
		logger.error("ERROR: " + str(sys.exc_info()[0]))
		#sys.exit(1)

	# open local file
	logger.info("Opening local file (%s)...", local_filename)
	try:
		csvfile = open(local_filename, "w")
	except:
		logger.error("Failed to open new catalog file (%s)...", local_filename)
		logger.error("ERROR: " + str(sys.exc_info()[0]))
		sys.exit(1)
   
	# initialize CSV file
	now = datetime.now()
	dt = now.strftime("%Y/%m/%d-%H:%M:%S")
	try:
		catalog_writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
	except:
		logger.error("Failed to initialize CSV file (%s)", local_filename)
		logger.error("ERROR: " + str(sys.exc_info()[0]))
		sys.exit(1)
   
	try:
		catalog_writer.writerow(['DS Data Catalog', dt])
		catalog_writer.writerow(['--------------------------------------', '-----------------------'])
	except (TypeError | NameError)  as err:
		logger.error("Failed in write file header rows (%s)", local_filename)
		logger.error("ERROR: %s", str(err))
		sys.exit(1)
   
	# write header row
	row = ['Data Source', 'Description', 'Data Generator', 'Source POC', 'Source Container', 'POC Contact Info', 'Soure URI', 'Data Owner', 'Retention (Hot)', 'Retention (Cool)', 'Retention (Delete)', 'Sensitivity', 'TimeStamp']
   
	try:
		catalog_writer.writerow(row)
		catalog_writer.writerow([''])
	except (TypeError | NameError)  as err:
		logger.error("Failed in write column header rows (%s)", local_filename)
		logger.error("ERROR: %s", str(err))
		sys.exit(1)

	# build/write to CSV file
	rows_written = 0
	for entry in entities:
		if entry.RowKey != "instantiation_row":
			# build CSV row.
			try:
				row = []
				row.append(str(entry.data_source.value))
				row.append(str(entry.description.value))
				row.append(str(entry.data_generator.value))
				row.append(str(entry.data_source_poc.value))
				row.append(str(entry.data_source_container.value))
				row.append(str(entry.data_source_poc_contact_info.value))
				row.append(str(entry.data_source_uri.value))
				row.append(str(entry.ds_data_owner.value))
				row.append(str(entry.retention_period_hot.value))
				row.append(str(entry.retention_period_cool.value))
				row.append(str(entry.retention_period_delete.value))
				row.append(str(entry.sensitivity.value))
				row.append(str(entry._metadata['timestamp']))
			except:
				logger.error("Failed to build row for data source (%s), exiting...", entry.data_source.value)
				logger.error("ERROR: %s", str(err))
				sys.exit(1)
   
			try:
				catalog_writer.writerow(row)
				rows_written = rows_written + 1
			except (TypeError | NameError)  as err:
				logger.error("Failed to write row for data source (%s), exiting...", entry.data_source.value)
				logger.error("ERROR: %s", str(err))
				sys.exit(1)

	logger.info("Total rows written (%s)", str(rows_written))
	return rows_written

def main(req: func.HttpRequest) -> func.HttpResponse:
	global LOGGER_LEVEL, DC_CONNECTION_STRING, FS_CONNECTION_STRING, AI_INSTRUMENTATION_KEY
	global SHARE_NAME, DC_TABLE_NAME, CATALOG_FILENAME

	# initialize logger
	logger = logging.getLogger(__name__)
	AI_INSTRUMENTATION_KEY = os.getenv("AI_INSTRUMENTATION_KEY", "NOTSET")
	logger.info("AI_INSTRUMENTATION_KEY (%s)", AI_INSTRUMENTATION_KEY)
	if AI_INSTRUMENTATION_KEY == "NOTSET":
		logger.error("No AI_INSTRUMENTATION_KEY provided, exiting...")
		sys.exit(1)
	logger.addHandler(AzureLogHandler(connection_string='InstrumentationKey='+ AI_INSTRUMENTATION_KEY))
	logger.info('Python HTTP trigger function processed a request.')

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

		if LOGGER_LEVEL in valid_levels:
			log_level = valid_levels[LOGGER_LEVEL]
			logger.info("got LOGGER_LEVEL of (%s)", LOGGER_LEVEL)
		else:
			logger.error("Invalid LOGGER_LEVEL provided (%s), defaulting to WARNING", LOG_LEVEL)
			log_level = logging.WARNING
			LOGGER_LEVEL = WARNING

		# set logging level
		logger.debug("Setting LOG_LEVEL to (%s)", LOGGER_LEVEL)
		# JRG logging.basicConfig(level=log_level)

	# get all required ENV values
	DC_CONNECTION_STRING = os.getenv("DC_CONNECTION_STRING", "NOTSET")
	logger.info("DC_CONNECTION_STRING (%s)", DC_CONNECTION_STRING)
	if DC_CONNECTION_STRING == "NOTSET":
		logger.error("No DC_CONNECTION_STRING provided, exiting...")
		#sys.exit(1)
   
	FS_CONNECTION_STRING = os.getenv("FS_CONNECTION_STRING", "NOTSET")
	logger.info("FS_CONNECTION_STRING (%s)", FS_CONNECTION_STRING)
	if FS_CONNECTION_STRING == "NOTSET":
		logger.error("No FS_CONNECTION_STRING provided, exiting...")
		#sys.exit(1)
   
	DC_TABLE_NAME = os.getenv("DC_TABLE_NAME", "NOTSET")
	logger.info("DC_TABLE_NAME (%s)", DC_TABLE_NAME)
	if DC_TABLE_NAME == "NOTSET":
		logger.error("No DC_TABLE_NAME provided, exiting...")
		#sys.exit(1)
   
	CATALOG_FILENAME = os.getenv("CATALOG_FILENAME", "NOTSET")
	logger.info("CATALOG_FILENAME (%s)", CATALOG_FILENAME)
	if CATALOG_FILENAME == "NOTSET":
		logger.error("No CATALOG_FILENAME provided, exiting...")
		#sys.exit(1)
   
	SHARE_NAME = os.getenv("SHARE_NAME", "NOTSET")
	logger.info("SHARE_NAME (%s)", SHARE_NAME)
	if SHARE_NAME == "NOTSET":
		logger.error("No SHARE_NAME provided, exiting...")
		#sys.exit(1)
   
	# create local/temp filename (absolute path)
	local_temp_path = tempfile.gettempdir()
	local_file = os.path.abspath(local_temp_path + "/" + CATALOG_FILENAME)
   
	# remove file, if it exists
	if os.path.exists(local_file):
		logger.info("Removing existing catalog file (%s)...", local_file)
		try:
			os.remove(local_file)
		except:
			logger.error("Failed to remove existing catalog file (%s)...", local_file)
			logger.error("ERROR: " + str(sys.exc_info()[0]))
			sys.exit(1)

	logger.info("Starting DataCatalog extraction process...")

	# extract data table contents
	if extract_contents(local_file) > 0:
		logger.info("Successfully extracted Data Catalog contents")
		if upload_file(local_file):
			logger.info("Sucessfully uploaded Data Catalog file (%s)", CATALOG_FILENAME)
		else:
			logger.error("Failed to upload data catalog file (%s), exiting...", CATALOG_FILENAME)
			return func.HttpResponse("Failed to upload catalog file, extiting...", status_code=200)
			#sys.exit(1)
	else:
		logger.info("Failed to extract Data Catalog contents, extiting...")
		return func.HttpResponse("Failed to extract Data Catalog contents, extiting...", status_code=200)
		#sys.exit(1)

	name = req.params.get('name')
	logger.info("Name value returned %s", str(name))
	if not name:
		logger.info("Getting name from body")
	try:
		req_body = req.get_json()
	except ValueError:
		pass
	else:
		name = req_body.get('name')
		logger.info("Got %s name from body", str(name))

	if name:
		return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
	else:
		return func.HttpResponse("This HTTP triggered function executed successfully.", status_code=200)
   
	# create local/temp filename (absolute path)
	local_temp_path = tempfile.gettempdir()
	local_file = os.path.abspath(local_temp_path + "/" + CATALOG_FILENAME)
   
	# remove file, if it exists
	if os.path.exists(local_file):
		logger.info("Removing existing catalog file (%s)...", local_file)
		try:
			os.remove(local_file)
		except:
			logger.error("Failed to remove existing catalog file (%s)...", local_file)
			logger.error("ERROR: " + str(sys.exc_info()[0]))
			sys.exit(1)

	logger.warning("Starting DataCatalog extraction process...")

	# extract data table contents
	if extract_contents(local_file) > 0:
		logger.info("Successfully extracted Data Catalog contents")
		if upload_file(local_file):
			logger.info("Sucessfully uploaded Data Catalog file (%s)", CATALOG_FILENAME)
		else:
			logger.error("Failed to upload data catalog file (%s), exiting...", CATALOG_FILENAME)
			return func.HttpResponse("Failed to upload catalog file, extiting...", status_code=200)
			#sys.exit(1)
	else:
		logger.info("Failed to extract Data Catalog contents, extiting...")
		return func.HttpResponse("Failed to extract Data Catalog contents, extiting...", status_code=200)
		#sys.exit(1)

	name = req.params.get('name')
	logger.info("Name value returned %s", str(name))
	if not name:
		logger.info("Getting name from body")
	try:
		req_body = req.get_json()
	except ValueError:
		pass
	else:
		name = req_body.get('name')
		logger.info("Got %s name from body", str(name))

	if name:
		return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
	else:
		return func.HttpResponse("This HTTP triggered function executed successfully.", status_code=200)
