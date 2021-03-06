import os
import json
import pprint
import smtplib
import hashlib
import logging
import traceback
from datetime import datetime
from email.mime.text import MIMEText

import requests
import sys

from ckanpackager.lib.ckan_dataset import CKANdataset
from ckanpackager.lib.utils import BadRequestError
from ckanpackager.lib.resource_file import ResourceFile
from ckanpackager.lib.statistics import statistics
from raven import Client

from ckanpackager.utils import local_settings
from ckanpackager.utils.QueueWriter import QueueWriter
from ckanpackager.utils.experiments import Things, ZIP_FILE_INCLUDE_FOLDER, create_download_zipfile
from ckanpackager.utils.local_settings import filestore_root


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
        print(pprint.pformat(params, indent=4))

        self.config = config
        self.download_payload = params.get('download_payload', None)
        self.file_paths = params.get('file_paths', None)

        if not self.download_payload:
            print('The file download payload was not received')
        else:
            self.download_payload = json.loads(self.download_payload)
            print ('Celery task got download payload :')
            print(self.download_payload)

        if not self.file_paths:
            print('The file paths were not received')
        else:
            print ('Celery task got file paths :')
            self.file_paths = json.loads(self.file_paths)
            print(self.file_paths)

        self.sentry = Client(self.config.get('SENTRY_DSN'))
        self.time = str(datetime.now())
        self.request_params = {}
        self.log = logging.getLogger(__name__)
        schema = self.schema()
        if 'email' not in schema:
            schema['email'] = (True, None)
        # if 'resource_id' not in schema:
        #     schema['resource_id'] = (True, None)
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
            # statistics(self.config['STATS_DB']).log_request(
            #     # self.request_params['resource_id'],
            #     self.request_params['email'],
            #     self.download_payload
            #     # self.request_params.get('limit', None)
            # )
        except Exception as e:
            # statistics(self.config['STATS_DB']).log_error(
            #     # self.request_params['resource_id'],
            #     '',
            #     self.request_params['email'],
            #     traceback.format_exc()
            # )
            self.sentry.captureException()

            self.log.error(e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.log.error('exc_type: {}, fname: {}, tb_lineno: {}'.format(exc_type, fname, exc_tb.tb_lineno))

            raise e

    # https://stackoverflow.com/a/32107484/2943238
    def paths(self, tree):
        if 'children' not in tree:
            # print '\n\nSingle file for {} size {}'.format(tree['name'], tree['size'])
            yield [
                [tree['full_path'],
                 tree['name'],
                 tree['size']], ]
        else:
            # print '\n\nChilden for dir {}'.format(tree['name'])
            for child in tree['children']:
                for descendant in self.paths(child):
                    yield [tree['name'], ] + descendant

    def _run(self):
        try:
            """Run the task"""
            self.log.info("Task parameters: {}".format(str(self.request_params)))
            self.log.info("Task download payload: {}".format(str(self.download_payload)))

            resource_ids = self.download_payload['download_list']
            package_ids = self.download_payload['download_package_list']

            extra_resources_data = []
            for package_id in package_ids:
                ckan_dataset = CKANdataset(package_id)
                try:
                    extra_resources_data.append(
                        ckan_dataset.fetch_resource_data()
                    )
                except:
                    # TODO care about failed resource fetches
                    pass

            self.log.info("extra_resources_data len {} : {}".format(
                len(extra_resources_data),
                str(extra_resources_data))
            )

            # # Get/create the file
            # resource = ResourceFile(
            #     self.request_params,
            #     self.config['STORE_DIRECTORY'],
            #     self.config['TEMP_DIRECTORY'],
            #     self.config['CACHE_TIME']
            # )
            # if not resource.zip_file_exists():
            #     self.create_zip(resource)
            # else:
            #     self.log.info("Found file in cache")
            # zip_file_name = resource.get_zip_file_name()
            # self.log.info("Got ZIP file {}. Emailing link.".format(zip_file_name))
            # # Email the link
            # place_holders = {
            #     'resource_id': self.request_params['resource_id'],
            #     'zip_file_name': os.path.basename(zip_file_name),
            #     'ckan_host': self.host()
            # }
            # from_addr = self.config['EMAIL_FROM'].format(**place_holders)
            # msg = MIMEText(self.config['EMAIL_BODY'].format(**place_holders))
            # msg['Subject'] = self.config['EMAIL_SUBJECT'].format(**place_holders)
            # msg['From'] = from_addr
            # msg['To'] = self.request_params['email']
            # server = smtplib.SMTP(self.config['SMTP_HOST'], self.config['SMTP_PORT'])
            # try:
            #     if 'SMTP_LOGIN' in self.config:
            #         server.login(self.config['SMTP_LOGIN'], self.config['SMTP_PASSWORD'])
            #     server.sendmail(from_addr, self.request_params['email'], msg.as_string())
            # finally:
            #     server.quit()

            t = Things()
            # t.post_stuff()


            # print 'DOWLOAD PAYLOAD:'
            # print self.payload
            # file_list = self.create_file_list_from_download_payload(self.payload)
            # print 'FILE LIST:'
            qw = QueueWriter()
            qw.generate_remote_credentials()

            for dataset in extra_resources_data:
                self.log.info("dataset len and type: {}".format(len(dataset), type(dataset)))

                for resource in dataset:
                    self.log.info("resource len and type: {}".format(len(resource), type(resource)))

                    if 'type' in resource:
                        if resource['type'] == 'BidsResource':

                            if 'url' in resource:
                                url = resource['url']
                                response = requests.get(url)
                                dir_struc = response.json()
                                resource_filelist = list(self.paths(dir_struc))
                                for dataset_file in resource_filelist:
                                    self.log.info("resource_filelist dataset_file: {}".format(
                                        dataset_file))

                                    file_path = '{}/'.format(resource['resource_filepath'])
                                    if len(dataset_file) > 2:
                                        file_path = '{}{}/'.format(file_path, '/'.join(dataset_file[1:-1]))
                                    file_path = '{}{}'.format(file_path, dataset_file[-1][1])

                                    qw.add_file(
                                        {
                                            'file_path': file_path,
                                            'name': dataset_file[-1][1],
                                            'size': dataset_file[-1][2]
                                        },
                                        remote_file=dataset_file[-1][0]
                                    )

                    else:
                        # TODO what do we do if there isn't a file path stored?
                        if 'filepath' in resource:
                            # self.log.info("resource filepath: {}".format(resource['filepath']))
                            qw.add_file(
                                {
                                    'file_path': resource['resource_filepath'],
                                    'name': resource['name'],
                                    'size': resource['size']
                                },
                                remote_file=resource['filepath']
                            )

            # TODO this probably isn't needed, it's from before we used the API in the packager celery task
            # for f in self.file_paths['paths']:
            #     self.log.info("File to save: {}".format(f))
            #
            #     if '.ds.json' in f['name']:
            #         with open(f['file_path'], 'r') as ds1:
            #             json_blob = json.load(ds1)
            #             file_list = t.blob_to_list(json_blob, '')
            #
            #             # print(file_list)
            #             self.log.info("File list to email: {}".format(file_list))
            #
            #             for ds_file in file_list:
            #                 self.log.info("adding: {}".format(ds_file))
            #                 ds_file['file_path'] = os.path.join(filestore_root, ds_file['file_path'])
            #                 qw.add_file(ds_file)
            #     else:
            #         qw.add_file(f)

            filezilla_queue_xml_filename = 'FileZilla_Download_Queue.xml'
            qw.write_queue_xml(filename=os.path.join(ZIP_FILE_INCLUDE_FOLDER, filezilla_queue_xml_filename))

            password_filename = os.path.join(ZIP_FILE_INCLUDE_FOLDER, 'PASSWORD.txt')
            with open(password_filename, 'w') as f:
                f.write(qw.password)

            zip_file_filename = "CKANFileDownload.zip"
            create_download_zipfile(zip_file_filename, filezilla_queue_xml_filename)

            files = [zip_file_filename]
            try:
                t.email_from_localhost(files=files)
                self.log.info('using logger to say : email sent')
            except Exception as e1:
                self.log.info(e1)
                raise e1

        except Exception as e3:
            self.log.error(e3)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.log.error('exc_type: {}, fname: {}, tb_lineno: {}'.format(exc_type, fname, exc_tb.tb_lineno))

            raise e3

    def create_file_list_from_download_payload(self, download_payload):
        # TODO: Return a list of files created from the package and resource information in the download payload

        resource_data = {}
        resource_urls = []

        for res_id in download_payload['download_list']:
            if res_id:

                response = requests.get(
                    'http://localhost:5000/api/3/action/resource_show',
                    params={'id': res_id}
                )

                res_json = response.json()
                print(res_json)

                # TODO use 'url' instead of name
                resource_data[res_id] = res_json['result']['name']
                resource_urls.append(res_json['result']['name'])

        return resource_urls

    def __str__(self):
        """Return a unique representation of this task"""
        md5 = hashlib.md5()
        md5.update(str(self.request_params))
        md5.update(self.time)
        return md5.hexdigest()
