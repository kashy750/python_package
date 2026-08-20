# General Utilities

A General Utilities Library that provides a Python interface to many technologies

Available **Objects**:
1. `RMQ` - RabbitMQ for queueing purpose
2. `REDIS` - Redis as a secondary storage
3. `FileStorage` - MinIO for storing and retrieving files
4. `FileStorageAzure` - Azure Blob Storage for retrieving files
5. `APIrequest` - API requests module

Available **Methods**:
1. `logger` - Logging module

## Installation

```bash
pip install general_utils
```

Or install the latest unreleased code straight from the repository:

```bash
pip install git+https://github.com/kashy750/python_package.git
```

Requires Python 3.7. Licensed under GPLv3.

See [SETUP.md](SETUP.md) for how to build and publish a new release.

## Sentry logging

Sentry is **opt-in**. Importing the library does not send anything anywhere.

To forward logs to Sentry, pass `sentry_flag=True` to `logger()` and supply a DSN,
either as an argument or through the `SENTRY_URL` environment variable:

```python
# DSN from the environment (recommended - keeps it out of source control)
log = utils.logger(sentry_flag=True)

# or passed explicitly
log = utils.logger(sentry_flag=True, sentry_url="https://<key>@<host>/<project>")
```

Records at the level given to `logger()` and above are sent as Sentry events. If
`sentry_flag=True` but no DSN is found, Sentry is skipped and local logging carries
on as normal.

Keep the DSN in a `.env` file (git-ignored) or your deployment's secret store -
never commit it.

## Usage

```python
from general_utils import utils

log = utils.logger()
log.info("ready")
```

RabbitMQ - publish and consume:

```python
rmq = utils.RMQ(url="amqp://user:pwd@host:5672/")
rmq.publish({"id": 1}, publish_queue="my_queue")

def handle(message):
    log.info(message)

rmq.listen(consume_queue="my_queue", callback_func=handle)
```

Redis - store and fetch:

```python
r = utils.REDIS(url="redis://host:6379/0")
r.set_data("my_key", {"a": 1}, expiry=3600)
data = r.get_data("my_key")
```

MinIO - read a file and upload one:

```python
store = utils.FileStorage("host:9000", "access_key", "secret_key")
df = store.get_data("my_bucket", "data.csv", data_type="csv")
store.putFile("my_bucket", "data.csv", "/local/path/data.csv")
```

Azure Blob Storage - read a file:

```python
blob = utils.FileStorageAzure(connectionStr="<azure_connection_string>")
df = blob.get_data("my_container", "data.csv", data_type="csv")
```

API requests - single and parallel:

```python
api = utils.APIrequest()
resp = api.get("https://example.com/endpoint")
resps = api.get_multi(["https://example.com/a", "https://example.com/b"], pool_size=2)
```

Kindly refer the docstrings for further details regarding the objects and methods :

```python
from general_utils import utils

help(utils)
##-- OR --##
print(utils.__doc__)
```
