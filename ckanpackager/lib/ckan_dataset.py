import logging

import requests


# The terms Dataset and package are used interchangeably
class CKANdataset:
    def __init__(self, package_id):
        self.package_id = package_id
        self.log = logging.getLogger(__name__)

    def fetch_resource_ids(self):
        if self.package_id is None:
            raise Exception

        # TODO catch exceptions better
        try:
            response = requests.get('http://localhost:5000/api/3/action/package_show?id={}'.format(self.package_id))

            resource_ids = []
            for resource in response.json()['result']['resources']:
                resource_ids.append(resource['id'])
            return resource_ids
        except Exception as e:
            raise e

    def fetch_resource_data(self):
        if self.package_id is None:
            raise Exception

        url = 'http://localhost:5000/api/3/action/package_show?id={}'.format(self.package_id)
        # TODO catch exceptions better
        try:
            self.log.info('Package data download URL : {}'.format(url))

            response = requests.get(url)

            self.log.info("Package data download response: {}".format(str(response.text)))

            return response.json()['result']['resources']
        except Exception as e:
            raise e