"""
This file is part of turkflow.

turkflow is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

turkflow is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with turkflow.  If not, see <http://www.gnu.org/licenses/>.
"""

from boto.mturk.connection import *
from boto.mturk.question import *
from boto.mturk.qualification import *
from boto.s3 import *
from boto.s3.key import Key
from jinja2 import Environment, PackageLoader, Template
import cPickle, time, os, sys, argparse, string, urllib, re, gdbm, operator
from collections import defaultdict
from os.path import *
from xml.sax.saxutils import escape

testURL = "https://workersandbox.mturk.com/mturk/externalSubmit"
liveURL = "https://mturk.com/mturk/externalSubmit"

class Job:
    def __init__(self, key):
        self.reset_counter = None;
        self.key = key
        self.hitid = None
        self.nassignments = 0
        self.url = None
        self.uploads = []

class JobDB:
    def __init__(self, db_filename, reset = False):
        self.__open__ = False
        self.__ref__ = None
        self.__dbfn__ = expanduser(db_filename)
        if reset and os.path.exists(self.__dbfn__):
            os.remove(self.__dbfn__)

    def open(self, ro = False):
        while self.__ref__ == None:
            if ro and exists(self.__dbfn__):
                self.__ref__ = gdbm.open(self.__dbfn__, 'r')
            else: 
                self.__ref__ = gdbm.open(self.__dbfn__, 'cs')

    def close(self):
        if self.__ref__:
            self.__ref__.close()
            self.__ref__ = None 

    def hasKey(self, key):
        if not key:
            return False
        return self.__ref__.has_key(key)

    def addJob(self, job):
        self.__ref__[job.key] = cPickle.dumps(job, 2)

    def removeJob(self, job):
        del self.__ref__[job.key]

    def getJob(self, key):
        return cPickle.loads(self.__ref__[key])

    def allKeys(self):
        k = self.__ref__.firstkey()
        while k != None:
            yield k
            k = self.__ref__.nextkey(k)

class TurkHITType:
    def __init__(self, title, keywords, duration,
                 annotation, reward, env,
                 max_assignments = 1, 
                 approval_delay = 1800, template_name = None, description = None,
                 lifetime = datetime.timedelta(7)):
        self.title = title[:120]
        self.description = description if description else title
        self.keywords = keywords
        self.duration = duration
        self.max_assignments = max_assignments
        self.annotation = annotation
        self.reward = reward
        self.approval_delay = approval_delay
        self.lifetime = lifetime
        self.template_name = template_name if template_name else annotation + ".html"
        self.html = None
        self.env = env

    def compileHTML(self, testmode = True):
        template = self.env.get_template(self.template_name)
        d = self.__dict__.copy()
        d['action_url'] = testURL if testmode else liveURL
        self.html = template.render(d)
        return self.html # TODO: gross statefulness

    def uniqueKey(self):
        return hash(self.__dict__)

class TurkConnection:
    def __init__(self, bucketname, db_filename, testmode = True, reset=None, extra_files = None, us_only=False):
        self.testmode = testmode
        self.job_db = JobDB(db_filename)
        self.connection = MTurkConnection() if not testmode else MTurkConnection(host='mechanicalturk.sandbox.amazonaws.com', is_secure=True, debug=0)
        s3conn =  boto.connect_s3()
        self.us_only = us_only

        try:
            self.bucket = s3conn.get_bucket(bucketname)
        except boto.exception.S3ResponseError:
            self.bucket = s3conn.create_bucket(bucketname)
            print("The S3 bucket '%s' has been created," %(bucketname))
            
        if extra_files:
            for f in extra_files:
                path, name = os.path.split(f)
                self._uploadfile(name, open(f, 'r').read())

        if not reset is None:
            self.job_db.open()
            all_keys = [k for k in self.job_db.allKeys()]
            for key in all_keys:
                job = self.job_db.getJob(key)
                if job.reset_counter >= 0 and job.reset_counter >= reset:
                    self.connection.expire_hit(job.hitid)
                    print("Disabled hit %s" % (job.hitid))
                    self.job_db.removeJob(job)

            self.job_db.close()

    def _uploadfile(self, fn, data=None):
        """Uploads a file to Amazon S3.

        Arguments:
        fn -- file name to upload
        data -- contents of file to upload, if not stored in file at fn

        Returns the URL of the now-uploaded file.

        """
        key = Key(self.bucket, fn)
        if data:
            key.path = fn
            key.set_contents_from_string(data, policy='public-read')
        else:
            key.set_contents_from_filename(fn, policy='public-read')
        url = key.generate_url(86400, force_http=True, query_auth=False).replace(':433', '')
        return url

    def _uploadHTMLQuestion(self, question_html, upload_name, frame_height = 1000):
      """Creates an HTML question on Amazon Mechanical Turk, inserting required scripts and markup.

      Arguments:
      question_html -- an HTML string representing a page to be presented as the question
      upload_name -- the name of the final HTML page once uploaded

      Returns a boto ExternalQuestion object

      """
      mainurl = self._uploadfile(upload_name, question_html)
      return ExternalQuestion(escape(mainurl), frame_height)

    def createHIT(self, hit_type, key = None, reset_counter = None):
        html = hit_type.compileHTML(self.testmode)
        hit_values_hash = hash(html) # to ensure different html file per question
        question = self._uploadHTMLQuestion(html, "%s_%d.html" % (hit_type.annotation, hit_values_hash))
        return self._postHIT(hit_type,
                             question,
                             key=(str(hit_values_hash) if not key else key),
                             reset_counter=reset_counter)

    def _postHIT(self, tht, external_question, key = None, reset_counter = None):
        """Post a programmtic HIT on Turk using a string replacement schema.

        Arguments:
        HIT_dict -- dictionary of properties expected by MTurk.
        external_question -- ExternalQuestion object representing the question
        key -- optionally specify your own key

        Returns a key generated to refer to the posted HIT. This is a local key.

        """
        while True:
            self.job_db.open()

            # create HIT if it doesn't already exist
            if not (key and self.job_db.hasKey(key)):

                # create qualficiations
                # TODO: factor this out
                lr = LocaleRequirement('EqualTo', 'US')
                quals = Qualifications()
                quals.add(lr)
                if not self.us_only:
                    quals = None

                # Some aspects changed for sandbox mode to make testing easier
                rs = self.connection.create_hit(
                    question = external_question,
                    title = tht.title,
                    description = tht.description,
                    duration = tht.duration,
                    max_assignments = tht.max_assignments,
                    annotation = tht.annotation,
                    reward = tht.reward,
                    qualifications = quals,
                    lifetime = tht.lifetime,
                    approval_delay = tht.approval_delay)

                for hit in rs:
                    if not hasattr(hit, 'HITId'):
                        print("%s: Could not create HIT." % key)
                        sys.exit(5)
                    print("%s: Created HIT %s" %(key, hit.HITId))

                    job = None
                    if not key:
                        key = hit.HITId
                    job = Job(key)
                    job.reset_counter = reset_counter

                    job.hitid = hit.HITId
                    job.nassignments = tht.max_assignments
                    self.job_db.addJob(job)

            else: # Check if job is done
                print("Job with that key already exists, not creating!")

            self.job_db.close()
            return key


    @staticmethod
    def _unpackAssignments(assignments):
        """ output format:

        {question1_id: [[worker1_replies], [worker2_replies]...],
         question2_id: ...,
         ...}
        """
        
        times = []
        d = defaultdict(list)

        for ass in assignments:
            times.append({'acceptTime': ass.AcceptTime, 'submitTime': ass.SubmitTime})
            for a in ass.answers: # not sure why boto calls this answers, it seems like this or next line should not be loop 
                for q in a:
                    worker_reply = []
                    for field in q.fields:
                        worker_reply.append(unicode(field))
                    d[q.qid].append(worker_reply)
        
        return d, times

    def waitForHIT(self, key, check_interval = 60, timeout = 0):
        """Poll Turk until a HIT is completed, exiting after a timeout.

        Arguments:
        key -- a local key representing a HIT created by this program.
        check_interval -- time between checks in seconds
        timeout -- number of secons after which to give up

        Returns a dictionary of the form {qid: [list of answers]}. The list of answers reliably maps indices to workers.

        """

        time0 = time.time()
        completedHits = []
        while True:
            self.job_db.open(True)

            if not self.job_db.hasKey(key):
                print("Job wasn't created! why are you waiting for it?")
                self.job_db.close()
                return None
            
            job = self.job_db.getJob(key)
            self.job_db.close()

            asss = self.connection.get_assignments(hit_id=job.hitid)
            print('%s: %d/%d assignments completed.' % (job.key, len(asss), job.nassignments))

            if(len(asss) == job.nassignments):
                return self._unpackAssignments(asss)

            timenow = time.time()
            if timeout > 0 and timenow - time0 > timeout:
                return None

            time.sleep(check_interval)

    def createAndWaitForHIT(self, hit_type, key = None):
        """Helper function to create a HIT and block on its completion in one step.
        """
        key = self.createHIT(hit_type, key)
        return self.waitForHIT(key)


