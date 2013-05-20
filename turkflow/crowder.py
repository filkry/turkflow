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

"""
Implementation of CrowdER entity resolution algorithm by Wang, Kraska, Franklin, Feng
"""

from turkflow import *
import networkx as nx
from Levenshtein import *
# https://github.com/miohtama/python-Levenshtein

def guess_autoescape(template_name):
    if template_name is None or '.' not in template_name:
        return False
    ext = template_name.rsplit('.', 1)[1]
    return ext in ('html', 'htm', 'xml')

env = Environment(autoescape=guess_autoescape,
                  loader=PackageLoader('turkflow', 'templates'),
                  extensions=['jinja2.ext.autoescape'])

class EntityResolutionHit(TurkHITType):
    def __init__(self, pair_set):
        self.pairs = pair_set
        TurkHITType.__init__(self,
                title='Are these things the same?',
                keywords=string.split("entity resolution english"),
                duration = 600,
                max_assignments = 1,
                annotation = 'crowdER_template',
                reward = 0.15,
                env = env)

def levenshtein_edit_distance(pair):
    return distance(pair[0], pair[1])

def jaccard_similarity(pair):
    a = set(pair[0].lower().split())
    b = set(pair[1].lower().split())
    return len(a.intersection(b)) / len(a.union(b))

def gen_any_function(fs):
    return (lambda pair: any([f(pair) for f in fs]))

def gen_jaccard_resolve_function(threshold = 0.30):
    return (lambda pair: (jaccard_similarity(pair) > threshold))

def gen_levenshtein_resolve_function(threshold = 0.50):
    return (lambda pair: 2*levenshtein_edit_distance(pair)/(len(pair[0]) + len(pair[1])) < threshold)

# ref http://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks-in-python
def chunks(l, n):
        """ Yield successive n-sized chunks from l.
        """
        for i in xrange(0, len(l), n):
            yield l[i:i+n]

def resolve(turk_connection, entities, machine_test, comparisons_per_hit = 5):
    # build pairs
    pairs = [(a, b) for (i, a) in enumerate(entities)
                    for b in entities[i+1:] if a != b]
    pairs = list(eval(x) for x in set([str(x) for x in pairs])) # remove exact duplicates

    # compute machine test on each pair
    potential_same_pairs = [p for p in pairs if machine_test(p)]

    # invoke crowd on all potential matches
    hit_keys = []
    for pair_set in chunks(potential_same_pairs, comparisons_per_hit):
        hit = EntityResolutionHit(pair_set)
        hit_keys.append((turk_connection.createHIT(hit), pair_set))

    # TODO: Factor out?
    duplicates = []
    for (key, pair_set) in hit_keys:
        results, times = turk_connection.waitForHIT(key)
        if not results:
            return None

        for pair in pair_set:
            question_key = "%s_%s" % (pair[0], pair[1])
            if results[question_key][0][0] == 'same':
                duplicates.append(pair) 
    g = nx.Graph()
    g.add_nodes_from(entities)
    g.add_edges_from(duplicates)
    d = nx.connected_component_subgraphs(g)

    # return resolved set of unique entities
    return [sg.nodes()[0] for sg in d]
    
if __name__ =='__main__':

    print "Running a quick test"

    db_location = "%s/%s.%s.jobs" % ("~/scratch/crowex", "crower_test", "sandbox")
    tc = TurkConnection("crower_test", db_location, env, True, True)
    
    test_set = ['ketchup', 'catsup', 'nick and nat', 'nick and nate', 'mustard', 'mustard', 'relish', 'chicken']

    print resolve(tc,
                  test_set,
                  gen_any_function([gen_jaccard_resolve_function(0.3),
                                    gen_levenshtein_resolve_function(0.5)]))

