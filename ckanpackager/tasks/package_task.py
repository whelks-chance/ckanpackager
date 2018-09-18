import os
import json
import smtplib
import hashlib
import logging
import traceback
from datetime import datetime
from email.mime.text import MIMEText
from ckanpackager.lib.utils import BadRequestError
from ckanpackager.lib.resource_file import ResourceFile
from ckanpackager.lib.statistics import statistics
from raven import Client

from ckanpackager.utils import local_settings
from ckanpackager.utils.QueueWriter import QueueWriter
from ckanpackager.utils.experiments import Things, ZIP_FILE_INCLUDE_FOLDER, create_download_zipfile


class PackageTask(object):
    """Base class for DatastorePackageTask and UrlPackageTask

    Note that all methods may be called from the web service or the task
    consumer.

    Derived classes must implement:

    - schema(): Return a dictionary of all possible request parameters to tuples defining (required,
                process function). Note that classes may define additional entries for their own use.
                'email' and 'resource_id' parameters are always required, so both are added to the schema
                as (True, None) if not defined;
    - host(): Return the hostname for the current request;
    - create_zip(ResourceFile): Create the ZIP file associated with the given resource file;

    In addition, derived class should implement:
    - speed(): Return 'slow' or 'fast' depending on the expected duration of the
               task. If not implemented, this always returns 'slow'.
    """
    def __init__(self, params, config):
        self.config = config
        self.download_payload = params.get('download_payload', None)
        if not self.download_payload:
            print('The file download payload was not received')
        self.sentry = Client(self.config.get('SENTRY_DSN'))
        self.time = str(datetime.now())
        self.request_params = {}
        self.log = logging.getLogger(__name__)
        schema = self.schema()
        if 'email' not in schema:
            schema['email'] = (True, None)
        if 'resource_id' not in schema:
            schema['resource_id'] = (True, None)
        for field, definition in schema.items():
            if definition[0] and field not in params:
                raise BadRequestError("Parameter {} is required".format(field))
            if field in params:
                if definition[1] is not None:
                    self.request_params[field] = definition[1](params.get(field, None))
                else:
                    self.request_params[field] = params.get(field, None)

    def schema(self):
        raise NotImplementedError

    def create_zip(self, resource):
        raise NotImplementedError

    def host(self):
        raise NotImplementedError
  
    def speed(self):
        """ Return the task estimated time as either 'fast' or 'slow'.

        If the file exists in the cache, then this returns 'fast'. It returns
        'slow' otherwise.
        """
        resource = ResourceFile(
            self.request_params,
            self.config['STORE_DIRECTORY'],
            self.config['TEMP_DIRECTORY'],
            self.config['CACHE_TIME']
        )
        if resource.zip_file_exists():
            return 'fast'
        else:
            return 'slow'

    def run(self, logger=None):
        """Run the task."""
        try:
            if logger is not None:
                self.log = logger
            else:
                self.log = logging.getLogger(__name__)
            self._run()
            statistics(self.config['STATS_DB']).log_request(
                self.request_params['resource_id'],
                self.request_params['email'],
                self.request_params.get('limit', None)
            )
        except Exception as e:
            statistics(self.config['STATS_DB']).log_error(
                self.request_params['resource_id'],
                self.request_params['email'],
                traceback.format_exc()
            )
            self.sentry.captureException()
            raise e

    def _run(self):
        """Run the task"""
        self.log.info("Task parameters: {}".format(str(self.request_params)))

        t = Things()
        # t.post_stuff()

        payload_string = self.download_payload
        payload = json.loads(payload_string)
        print 'DOWLOAD PAYLOAD:'
        print payload
        file_list = self.create_file_list_from_download_payload(payload)
        print 'FILE LIST:'
        qw = QueueWriter()
        for f in file_list:
            print f
            qw.add_file(f)

        filezilla_queue_xml_filename = 'FileZilla_Download_Queue.xml'
        qw.write_queue_xml(filename=os.path.join(ZIP_FILE_INCLUDE_FOLDER, filezilla_queue_xml_filename))

        password_filename = os.path.join(ZIP_FILE_INCLUDE_FOLDER, 'PASSWORD.txt')
        with open(password_filename, 'w') as f:
            f.write(local_settings.password)

        zip_file_filename = "CKANFileDownload.zip"
        create_download_zipfile(zip_file_filename, filezilla_queue_xml_filename)

        files = [zip_file_filename]
        try:
            # t.email_from_localhost(files=files)
            self.log.info('sent')
        except Exception as e1:
            self.log.info(e1)

    def create_file_list_from_download_payload(self, download_payload):
        # TODO: Return a list of files created from the package and resource information in the download payload
        return [
            '/tmp/test-package/file1.txt',
            '/tmp/test-package/file2.txt',
            '/tmp/test-package/file3.txt',
        ]

    def __str__(self):
        """Return a unique representation of this task"""
        md5 = hashlib.md5()
        md5.update(str(self.request_params))
        md5.update(self.time)
        return md5.hexdigest()
