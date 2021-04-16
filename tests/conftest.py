import pytest
from uvicorn.config import Config
from uvicorn.main import Server as UvicornServer
from .asgi_app import app
import threading
import time
import contextlib


class Server(UvicornServer):
    def install_signal_handlers(self):
        pass


@pytest.fixture(scope="session", autouse=True)
def uvicorn_server():
    uvicorn_config = {'host': '127.0.0.1', 'port': 8000, 'uds': None, 'fd': None, 'loop': 'asyncio', 'http': 'auto', 'ws': 'auto', 'lifespan': 'auto', 'env_file': None, 'log_config': {'version': 1, 'disable_existing_loggers': False, 'formatters': {'default': {'()': 'uvicorn.logging.DefaultFormatter', 'fmt': '%(levelprefix)s %(message)s', 'use_colors': None}, 'access': {'()': 'uvicorn.logging.AccessFormatter', 'fmt': '%(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s'}}, 'handlers': {'default': {'formatter': 'default', 'class': 'logging.StreamHandler', 'stream': 'ext://sys.stderr'}, 'access': {'formatter': 'access', 'class': 'logging.StreamHandler', 'stream': 'ext://sys.stdout'}}, 'loggers': {'uvicorn': {'handlers': ['default'], 'level': 'INFO'}, 'uvicorn.error': {'level': 'INFO'}, 'uvicorn.access': {'handlers': ['access'], 'level': 'INFO', 'propagate': False}}}, 'log_level': None, 'access_log': True, 'interface': 'auto', 'debug': False, 'reload': False, 'reload_dirs': None, 'workers': None, 'proxy_headers': True, 'forwarded_allow_ips': None, 'root_path': '', 'limit_concurrency': None, 'backlog': 2048, 'limit_max_requests': None, 'timeout_keep_alive': 5, 'ssl_keyfile': None, 'ssl_certfile': None, 'ssl_version': 2, 'ssl_cert_reqs': 0, 'ssl_ca_certs': None, 'ssl_ciphers': 'TLSv1', 'headers': [], 'use_colors': None}

    config = Config(app, **uvicorn_config)
    server = Server(config=config)

    thread = threading.Thread(target=server.run)
    thread.start()
    try:
        while not server.started:
            time.sleep(1e-3)
        yield
    finally:
        server.should_exit = True
        thread.join()
