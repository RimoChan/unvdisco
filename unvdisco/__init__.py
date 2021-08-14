import json
import base64
import random
import inspect
import logging

import azure.functions as func

from . import method
from .e import HTTPError


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # logging.warning(repr([req.url, req.method, dict(req.headers), req.get_body(), req.params]))
        f = method.__getattribute__(req.method.lower())
        d = {
            'path': req.route_params.get('path', '/'),
            'headers': req.headers,
            'req': req,
            'url': req.url,
            'body': req.get_body(),
            'auth': req.headers.get('authorization'),
        }
        a = inspect.getargspec(f).args
        d = {k:v for k,v in d.items() if k in a}
        status_code, t, headers = f(**d)
        # logging.warning(repr([req.url, req.method, dict(req.headers)]) + f' => {status_code}\n')
        return func.HttpResponse(
            t,
            headers={k:str(v) for k,v in headers.items()},
            status_code=status_code,
        )
    except HTTPError as e:
        return response(f'运行错误({e.code}): {repr(e)}', status_code=e.code)
    except Exception as e:
        logging.exception(e)
        logging.warning(repr([req.url, req.method, req.headers, req.params]) + '坏了')
        return response(f'运行错误: {repr(e)}', status_code=500)


def response(x, status_code=200, headers={}) -> func.HttpResponse:
    新x = json.dumps(x, ensure_ascii=False)
    return func.HttpResponse(新x, status_code=status_code, headers=headers)
