from turkflow.turkflow import *
from jinja2 import *

# create jinja2 environment using "templates" subdirectory
env = Environment(loader=PackageLoader('turkflow', 'templates'))

class TestHIT(TurkHITType):
    def __init__(self):
        TurkHITType.__init__(self,
            "This is a test HIT",
            string.split('keywords'),
            description = 'test description',
            duration = 600, # seconds
            max_assignments = 50,
            annotation = 'test', # by default, this will make turkflow look for a "test.html" jinja2 template
            reward = 0.05,
            env = env)

tc = TurkConnection("turkflow_test_id", "~/scratch")
hit_key = tc.createHIT(TestHIT())
results, completion_times = tc.waitForHIT(hit_key, timeout=30) # stop polling after 30 seconds
