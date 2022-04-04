import mimetypes

from azure.storage.blob import BlobServiceClient
from azure.storage.blob._list_blobs_helper import BlobPrefix
from azure.storage.blob._models import BlobProperties
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceNotFoundError

from . import config
from .e import HTTPError


blob_service_client = BlobServiceClient.from_connection_string(config.blob_connection_string)
unvdisco = blob_service_client.get_container_client(config.blob_container)

data_lake_service_client = DataLakeServiceClient(account_url="https://{}.dfs.core.windows.net".format(config.storage_account_name), credential=config.storage_account_key)
unvdisco_lake = data_lake_service_client.get_file_system_client(file_system=config.blob_container)


_unvdisco = blob_service_client.get_container_client(config.blob_container)
class _u:
    def __getattribute__(self, x):
        print(f'unvdisco.{x}')
        return _unvdisco.__getattribute__(x)
unvdisco = _u()


class 源:
    def __init__(self, path, *, mode='r', meta=None, auth=None):
        self.path = path
        self._auth = auth
        self._mode = mode
        self._meta = meta
        if False and 什么(auth):
            raise HTTPError(401, '验证错误。')
        if mode == 'r':
            None
        elif mode == 'w':
            unvdisco.upload_blob(path, b'', overwrite=True)
        elif mode == 'mkdir':
            if not path.endswith('/'):
                path += '/'
            unvdisco.upload_blob(path+'小精灵', b'')
            unvdisco.delete_blob(path+'小精灵')
        else:
            raise Exception('啊？')

    def isfile(self):
        return bool(self.妹.content_settings.content_md5)

    @property
    def 妹(self):
        if self._meta:
            return self._meta
        try:
            p = unvdisco.get_blob_client(self.path).get_blob_properties()
        except ResourceNotFoundError:
            raise HTTPError(404, f'{self.path}丢了！')
        self._meta = p
        return self._meta

    def write(self, data):
        assert self._mode == 'w'
        unvdisco.upload_blob(self.path, data, overwrite=True)

    def get_content(self, bpoint, epoint):
        st_size = self.妹.size
        assert 0 <= bpoint
        assert bpoint-1 <= epoint <= st_size
        if bpoint-1 == epoint:
            return b''
        水 = unvdisco.download_blob(self.path, offset=bpoint, length=epoint - bpoint + 1)
        return 水.readall()

    def get_size(self):
        return self.妹.size

    def get_props(self):
        p = {
            'iscollection': int(not self.isfile()),
            'creationdate': '2000-01-01T00:00:00-00:00',
            'getetag': self.妹.etag,
        }
        if self.isfile():
            p['getcontentlength'] = self.get_size()
            p['getcontenttype'] = mimetypes.guess_type(self.path)[0]
        else:
            p['getcontenttype'] = 'httpd/unix-directory'
        return p

    def listdir(self):
        p = self.path
        if not p.endswith('/'):
            p += '/'
        for f in unvdisco.walk_blobs(name_starts_with=p):
            if type(f) is BlobPrefix:
                yield 源(f.name, auth=self._auth)
            elif type(f) is BlobProperties:
                yield 源(f.name, meta=f, auth=self._auth)
            else:
                raise Exception('？？？')

    def delete(self):
        unvdisco.delete_blob(self.path)

    def move(self, target):
        unvdisco_lake.get_file_client(self.path).rename_file(f'{config.blob_container}/{target}')

    def __repr__(self):
        return f'<源 {self.path} >'


def test():
    缩进 = 0

    def f(x):
        nonlocal 缩进
        print(f'{" "*缩进}{x} - {x.妹.size} - {x.isfile()}')
        if not x.isfile():
            缩进 += 2
            for i in x.listdir():
                f(i)
            缩进 -= 2
    f(源('/'))
