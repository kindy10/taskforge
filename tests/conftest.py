import asyncio
import sys

# On Windows, psycopg async requires SelectorEventLoop instead of the default ProactorEventLoop
if sys.platform == "win32":
    def pytest_asyncio_loop_factories(config, item):
        return {"selector": asyncio.SelectorEventLoop}