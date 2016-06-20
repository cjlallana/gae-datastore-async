'''
Created on Jun 16, 2016

@author: Carlos Lallana
'''
import os
import jinja2, webapp2

from google.appengine.ext import ndb

import random, string
import logging
from datetime import datetime

class MyEntity(ndb.Model):

    _use_memcache = True

    string_property = ndb.StringProperty()


class Controller(webapp2.RequestHandler):

    def get(self):
        if self.request.get('action') == 'populate':
            self.populate(20000)
        
        if self.request.get('action') == 'fetch':
            n_entities = int(self.request.get('n_entities'))
            repeats = int(self.request.get('repeats'))
            self.fetch_parallel(n_entities, repeats)
            self.fetch_serial(n_entities, repeats)
            
            self.response.out.write("\n\nFetching complete!") 
        
        else:            
            template = JINJA_ENVIRONMENT.get_template('templates/index.html')
            self.request.response.write(template.render())

    def populate(self, n_entities):
        entity_list = []
        for _ in range(n_entities):
            random_string = ''.join(random.choice(string.lowercase) for _ in range(10))
            
            new_entity = MyEntity(string_property=random_string)
            entity_list.append(new_entity)

        ndb.put_multi(entity_list)
        
        self.response.out.write("Datastore populated with " + n_entities + " entities!<br>")
    
    
    def fetch_serial(self, fetch_limit, repeats):
        start = datetime.utcnow()

        query = MyEntity.query()
        query = query.order(MyEntity.string_property)
        
        logging.info("Serial fetching...")
        for _ in range(repeats):
            results = query.fetch(fetch_limit)
            
            delta_secs = (datetime.utcnow() - start).total_seconds()
            self.response.out.write("Got %d results serially, delta_sec: %f<br>\n" %(len(results), delta_secs))
        

    def fetch_parallel(self, fetch_limit, repeats):
        start = datetime.utcnow()
        
        query = MyEntity.query()
        query = query.order(MyEntity.string_property)
        
        logging.info("Async fetching...")
        futures = []
        for _ in range(repeats):
            f = query.fetch_async(fetch_limit)
            futures.append(f)
            
        while futures:
            f = ndb.Future.wait_any(futures)
            futures.remove(f)
            results = f.get_result()

            delta_secs = (datetime.utcnow() - start).total_seconds()
            self.response.out.write("Got %d results async, delta_sec: %f<br>\n" %(len(results), delta_secs))
        
        
JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)

app = webapp2.WSGIApplication(
    [
        ('/', Controller)
    ], debug=True)
