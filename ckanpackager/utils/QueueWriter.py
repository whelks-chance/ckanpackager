import os
from lxml import etree as ET

import local_settings


class ServerType:
    DEFAULT = 0
    UNIX = 1
    VMS = 2
    DOS = 3  # Backslashes as preferred separator
    MVS = 4
    VXWORKS = 5
    ZVM = 6
    HPNONSTOP = 7
    DOS_VIRTUAL = 8
    CYGWIN = 9
    DOS_FWD_SLASHES = 10  # Forwardslashes as preferred separator
    SERVERTYPE_MAX = 11


class QueueWriter:
    def __init__(self):
        # Files descriptions have shape like...
        # {
        #     'LocalFile': '/home/ianh/filezilla_dls/java_error_in_PYCHARM.hprof',
        #     'RemoteFile': 'java_error_in_PYCHARM.hprof',
        #     'RemotePath': '1 0 4 home 4 ianh',
        #     'Download': 1,
        #     'Size': 946972281,
        #     'DataType': 1
        # }

        self.queue_data = {
            'Host': 'localhost',
            'Port': 22,
            'Protocol': 1,
            'Type': 0,
            'User': 'ianh',
            'Logontype': 2,
            'TimezoneOffset': 0,
            'PasvMode': 'MODE_DEFAULT',
            'MaximumMultipleConnections': 0,
            'EncodingType': 'Auto',
            'BypassProxy': 0,
            'Name': 'New site',
            'files': []
        }

    # Weird magic string, encoded something like:
    # <RemotePath>1 0 4 home 4 ianh 13 filezilla_dls</RemotePath>
    # For weird ENUM references, look here
    # https://forum.filezilla-project.org/viewtopic.php?t=8416
    # https://svn.filezilla-project.org/filezilla/FileZilla3/trunk/src/include/server.h?view=markup
    def get_magic_enums(self, filepath, server_type=ServerType.UNIX):
        path_arr = os.path.normpath(filepath).split(os.path.sep)[:-1]
        magic_string = '{} '.format(server_type)
        for p in path_arr:
            if len(p):
                magic_string += '{} {} '.format(len(p), p)
            else:
                magic_string += '0 '
        return magic_string.strip()

    # Adding a file to the email
    def add_file(self, local_file, remote_file=None,
                 remote_path_magic_enums=None, download_num=1, data_type_enum=1):

        if remote_file is None:
            remote_file = os.path.basename(local_file)
        if remote_path_magic_enums is None:
            remote_path_magic_enums = self.get_magic_enums(local_file)

        self.queue_data['files'].append(
            {
                'LocalFile': local_settings.win_dir + os.path.basename(local_file),
                'RemoteFile': remote_file,
                'RemotePath': remote_path_magic_enums,
                'Download': download_num,
                'Size': os.path.getsize(local_file),
                'DataType': data_type_enum
            }
        )

    # This file is imported/exported from FileZilla
    def write_queue_xml(self, filename="filename.xml"):
        root = ET.Element("FileZilla3", version="3.15.0.2", platform="*nix")
        queue = ET.SubElement(root, "Queue")
        server = ET.SubElement(queue, "Server")

        for key in self.queue_data.keys():
            print(key, self.queue_data[key])
            if key == 'files':
                for queue_file in self.queue_data['files']:
                    file_element = ET.SubElement(server, 'File')
                    for file_key in queue_file.keys():
                        ET.SubElement(file_element, file_key).text = str(queue_file[file_key])
            else:
                ET.SubElement(server, key).text = str(self.queue_data[key])

        tree = ET.ElementTree(root)
        print(tree)
        tree.write(filename, pretty_print=True, encoding='utf-8', xml_declaration=True)


if __name__ == '__main__':
    qw = QueueWriter()
    for f in local_settings.files:
        qw.add_file(f)
        print(os.path.getsize(f))
    qw.write_queue_xml(filename='FileZilla.xml')
