import json
from urlparse import urlparse
from ckanpackager.lib.ckan_resource import CkanResource
from ckanpackager.lib.resource_file import ResourceFile
from ckanpackager.tasks.package_task import PackageTask

class CkanFailure(Exception):
    """Exception raised when CKAN returns unsuccessful request"""
    pass

class DatastorePackageTask(PackageTask):
    """Represents a datastore packager task."""
    def schema(self):
        """Define the schema for datastore package tasks

        Each field is a tuple defining (required, processing function, forward to ckan)
        """
        return {
            'api_url': (True, None, False),
            'resource_id': (True, None, True),
            'email': (True, None, False),
            'filters': (False, json.loads, True),
            'q': (False, None, True),
            'plain': (False, None, True),
            'language': (False, None, True),
            'limit': (False, None, True),
            'offset': (False, None, True),
            'fields': (False, None, True),
            'sort': (False, None, True),
            'format': (False, None, False),
        }

    def host(self):
        """Return the host name for the request"""
        return urlparse(self.request_params['api_url']).netloc

    def speed(self):
        """ Return the expected task duration.

         If the file exists in the cache, then this is assumed to be fast,
         Otherwise this is assumed to be fast only if there less than
         the configures SLOW_REQUEST number of rows.
         """
        if super(DatastorePackageTask, self).speed() == 'fast':
            return 'fast'
        if self.request_params.get('limit', False):
            offset = int(self.request_params.get('offset', 0))
            limit = int(self.request_params['limit']) - offset
            if limit > self.config['SLOW_REQUEST']:
                return 'slow'
            else:
                return 'fast'
        else:
            return 'slow'

    def create_zip(self, resource):
        """Create the ZIP file matching the current request

        @return: The ZIP file name
        """
        schema = self.schema()
        ckan_params = dict([(k, v) for (k, v) in self.request_params.items() if schema[k][2]])
        ckan_resource = CkanResource(self.request_params['api_url'], self.request_params.get('key', None), ckan_params)
        try:
            # Read the datastore fields, and generate the package structure.
            self.log.info("Fetching fields")
            response = ckan_resource.request(0, 0)

            # Write fields to out file as headers
            fields = self._write_headers(response, resource)

            # If this is a SOLR backend we want to use a cursor
            # This is much faster than the DB search - and prevents duplicates
            cursor = None
            try:
                if response['result']['_backend'] == 'datasolr':
                    cursor = '*'
                    self.log.info("Search type: Solr cursor")
            except KeyError:
                self.log.info("Search type: DB")

            page = 0
            count = 0
            max_count = int(self.request_params.get('limit', 0))
            while True:
                response = ckan_resource.request(page, self.config['PAGE_SIZE'], cursor)
                # If we've run out of records, break
                if not response['result']['records']:
                    break
                self._write_records(response['result']['records'], fields, resource)
                if cursor:
                    cursor = response['result']['next_cursor']
                # Start offset - not used for SOLR
                page += 1
                count += len(response['result']['records'])

                if max_count and count >= max_count:
                    break

                try:
                    response['result']['total']
                except KeyError:
                    print response
                if count >= response['result']['total']:
                    break

            # Finalize the resource
            self._finalize_resource(fields, resource)
            # Zip the file
            resource.create_zip(self.config['ZIP_COMMAND'])
        finally:
            resource.clean_work_files()

    @staticmethod
    def _write_headers(response, resource):
        # Build a list of fields
        fields = [f['id'] for f in response['result']['fields']]
        w = resource.get_csv_writer('resource.csv')
        w.writerow(fields)
        return fields

    @staticmethod
    def _write_records(records, fields, resource):
        """Stream the records from the input stream to the resource files
        @param records: json dict of records
        @param fields: List
        @param resource: Resource file
        @type resource: ResourceFile
        """
        w = resource.get_csv_writer('resource.csv')
        for record in records:
            row = []
            for field_id in fields:
                row.append(record.get(field_id, None))
            w.writerow(row)

    def _finalize_resource(self, fields, resource):
        """Finalize the resource before ZIPing it.

        This implementation does nothing - this is available as a hook for
        sub-classes who wish to add other files in the .zip file

        @param fields: List
        @param resource: The resource we are creating
        @type resource: ResourceFile
        """
        pass

