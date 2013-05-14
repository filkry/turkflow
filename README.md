turkflow
========

Library for quickly creating Mechanical Turk workflows. In some ways, turkflow ended up being an accidental re-implementation of [TurKit](http://dl.acm.org/citation.cfm?id=1866029.1866040&coll=DL&dl=GUIDE&CFID=308021477&CFTOKEN=94691217).

``turkflow`` allows you to specify arbitrary HTML HITs in [jinja2](http://jinja.pocoo.org/docs/) template files and python objects. It also provides functionality for waiting on the results of those HITs, checking HITs previously created by ``turkflow`` for completion, and parsing the results into a python structure.

## Installation
The recommended way to install ``turkflow`` is with ``pip``:

    pip install turkflow

You also need to create a [boto configuration file](https://code.google.com/p/boto/wiki/BotoConfig) with your Amazon AWS information.

## Usage

Here is a minimal example of a ``turkflow`` program:

```python
from turkflow.turkflow import *
from jinja2 import *

# create jinja2 environment using "templates" subdirectory
env = Environment(loader=PackageLoader('common', 'templates'))

class TestHIT(TurkHITType):
    def __init__(self):
        TurkHITType.__init__(self,
            "This is a test HIT",
            string.split('keywords'),
            description = 'test description',
            duration = 10, # minutes
            max_assignments = 50,
            annotation = 'test', # by default, this will make turkflow look for a "test.html" jinja2 template
            reward = 0.05,
            env = env)

tc = TurkConnection("ID", "~/place_to_put_db")
hit_key = tc.createHIT(TestHit())
results, completion_times = tc.waitForHIT(key, timeout=30) # stop polling after 30 seconds
```
