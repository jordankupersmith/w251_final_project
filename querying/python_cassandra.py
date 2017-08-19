from cassandra.cluster import Cluster
import time
import pandas as pd
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
#from cassandra.query import ValueSequence
#from cassandra.query import dict_factory
#https://github.com/datastax/python-driver
cluster = Cluster()

session = cluster.connect('wikikeyspace')
#session.row_factory = dict_factory
class t20160101(Model):
     __keyspace__  = 'wikikeyspace' 
     __table_name__ = 't20160101'
     page_name= columns.Text(primary_key=True)
     view_count = columns.Integer()
     language = columns.Text(primary_key=True)

   
     
start_time = time.time()     
#q = t20160101.objects.filter(language='es').allow_filtering()     
#print q
#s = q.filter(page_name__contains='Obama').allow_filtering()
#query_test=q.get()
#print query_test
# #select "page_name", "view_count" from t20160101 limit 20;


#my_user_ids = ('obama', 'bush', 'trump')
#query = "SELECT * FROM users WHERE user_id IN %s"
#query="SELECT * from t20160101 WHERE page_name IN %s"
#select "page_name", "view_count", "language" from t20160101
#rows=session.execute(query, parameters=[ValueSequence(my_user_ids)])


#rows = t20160101.objects.all()
#rows = t20160101.objects.filter(language='en')
#print rows
rows = session.execute("select page_name, view_count, language from t20160101")
#rows=session.execute(query, parameters=['en'])
#cassandra.cluster.ResultSet
#for line in rows:
    #print rows
    #print line
#print type(rows)

#def pandas_factory(colnames, rows):
 #   return pd.DataFrame(rows, columns=colnames)

#session.row_factory = pandas_factory
#session.default_fetch_size = None

#query = "select page_name, view_count from t20160101"
#rslt = session.execute(query, timeout=None)
#df = rslt._current_rows
#df = pd.DataFrame(rows[0])
#d=rows[0]
#print d

#print "the above is d"

d2={}
#print type(rows)
#d2_list=[]
for line in rows:
    
    if 'obama' in line.page_name and line.language=='en':
        #print line.page_name, line.view_count
       #list_add=[line.page_name, line.view_count]
        d2.update({line.page_name:line.view_count})
        #d2[k] = v
       #d2_list.append(list_add)
    else:
       pass
        
print "Length : %d" % len (d2)        
print "--- %s seconds ---" % (time.time() - start_time)
#for key, value in d2.iteritems():   
    #print key, value     