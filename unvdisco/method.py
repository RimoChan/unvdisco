import os
import uuid
import urllib
import logging

from .源 import 源
from .e import HTTPError


def options():
    header = {
        'Server': 'unv-disco',
        'Allow': 'GET, HEAD, PUT, DELETE, OPTIONS, PROPFIND, PROPPATCH, MKCOL, LOCK, UNLOCK, MOVE, COPY',
        'Content-length': '0',
        'DAV': '1, 2',
        'MS-Author-Via': 'DAV',
    }
    return 200, b'', header


def mkcol(path, auth):
    path = urllib.parse.unquote(path)
    if path == '':
        raise HTTPError(403)
    源(path, mode='mkdir', auth=auth)
    return 201, b'', {}


def move(path, auth, headers, path_prefix):
    destination = headers['destination']
    assert destination.startswith(path_prefix)
    destination = destination[len(path_prefix):]
    # print('慢慢', path, destination)
    文件 = 源(path, auth=auth)
    文件.move(destination)
    return 200, b'', {}


def copy():
    raise HTTPError(501)


def _propfind(path, auth, depth, wished_props) -> bytes:
    文件 = 源(path, auth=auth)
    s = '<?xml version="1.0" encoding="utf-8" ?><D:multistatus xmlns:D="DAV:" xmlns:Z="urn:schemas-microsoft-com:">'
    def write_props_member(m):
        nonlocal s
        name = m.path
        if not name.startswith('/'):
            name = '/'+name
        小s = f'<D:response><D:href>{urllib.parse.quote(name)}</D:href><D:propstat><D:prop>'
        props = m.get_props()
        for wp in wished_props:
            if wp not in props:
                小s += f'<D:{wp}/>'
            else:
                小s += f'<D:{wp}>{props[wp]}</D:{wp}>'
        小s += '</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>'
        return 小s
    s += write_props_member(文件)
    if depth > 1:
        raise Exception('太大了，不行。')
    if depth == 1:
        for 子文件 in 文件.listdir():
            s += write_props_member(子文件)
    s += '</D:multistatus>'
    return s.encode('utf-8')


def propfind(headers, path, auth):
    if path=='desktop.ini':
        raise HTTPError(404)
    path = urllib.parse.unquote(path)
    depth = int(headers.get('Depth', 0))
    wished_props = ('getcontenttype', 'getcontentlength', 'creationdate', 'iscollection', 'getetag')
    s = _propfind(path, auth, depth, wished_props)
    return 207, s, {'Content-Type': 'text/xml', 'Content-Length': len(s)}


# 实际上什么也没做
def lock():
    clientid = '2333'
    lockid = str(uuid.uuid1())
    retstr = f'<?xml version="1.0" encoding="utf-8" ?><D:prop xmlns:D="DAV:"><D:lockdiscovery><D:activelock><D:locktype><D:write/></D:locktype><D:lockscope><D:exclusive/></D:lockscope><D:depth>Infinity</D:depth><D:owner><D:href>{clientid}</D:href></D:owner>\n<D:timeout>Infinite</D:timeout><D:locktoken><D:href>opaquelocktoken:{lockid}</D:href></D:locktoken></D:activelock></D:lockdiscovery></D:prop>'.encode('utf8')
    return 201, retstr, {'Content-type': 'text/xml', 'Lock-Token': '<opaquelocktoken:'+lockid+'>', 'Content-Length': len(retstr)}


def unlock():
    return 204, b'', {}


def head(headers, path, auth, url):
    return get(headers, path, auth=auth, url=url, onlyhead=True)


def get(headers, path, auth, url, onlyhead=False):
    文件 = 源(path, auth=auth)
    if 文件.isfile():
        fullen = 文件.get_size()
        props = 文件.get_props()
        header = {
            'Content-type': props['getcontenttype'],
        }
        data = b''
        if 'Range' in headers:
            stmp = headers['Range'][6:]
            stmp = stmp.split('-')
            try:
                bpoint = int(stmp[0])
            except Exception:
                bpoint = 0
            try:
                epoint = int(stmp[1])
            except Exception:
                epoint = fullen - 1
            if epoint <= bpoint:
                bpoint = 0
                epoint = fullen - 1
            fullen = epoint - bpoint + 1
            header['Content-Range'] = 'Bytes %s-%s/%s' % (bpoint, epoint, fullen)
            if not onlyhead:
                data = 文件.get_content(bpoint, epoint)
            return 206, data, header
        else:
            if not onlyhead:
                data = 文件.get_content(0, fullen-1)
            return 200, data, header
    else:
        if not url.endswith('/'):
            return 307, '', {'Location': url+'/'}
        s = f'<title>幼女盘</title><h1>当前母鹿: {path}</h1>'
        for x in 文件.listdir():
            if 文件.path == '/':
                name = x.path
            else:
                assert x.path.startswith(文件.path), '不可能吧'
                name = x.path[len(文件.path):]
            s += f'<li><a href="{name}">{name}</a></li>'
        return 200, s.encode('utf8'), {'Content-Type': 'text/html;charset=UTF-8'}


def put(headers, path, body, auth):
    size = int(headers.get('Content-length', 0))
    f = 源(path, mode='w', auth=auth)
    if size == 0:
        return 201, b'', {}
    else:
        f.write(body)
        return 204, b'', {}


def delete(path, auth):
    文件 = 源(path, auth=auth)
    文件.delete()
    return 200, b'', {}


# 实际上什么也没做
def proppatch(url):
    data = f'''
    <?xml version="1.0"?>
    <a:multistatus xmlns:Z="urn:schemas-microsoft-com:" xmlns:a="DAV:">
        <a:response>
            <a:href>{url}</a:href>
            <a:propstat>
                <a:prop/>
                <a:status>HTTP/1.1 200 OK</a:status>
            </a:propstat>
        </a:response>
    </a:multistatus>
    '''.encode('utf8')
    return 200, data, {'Content-type': 'text/xml'}
