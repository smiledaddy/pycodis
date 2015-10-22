import time
import json
from kazoo.client import KazooClient


client = KazooClient("127.0.0.1:2181",
                     connection_retry={'max_delay': 10, 'max_tries': -1})

WATCH_PATH = '/zk/codis/db_test/proxy'

@client.ChildrenWatch(WATCH_PATH, allow_session_lost=True, send_event=True)
def changed(children, event):
    if event:
        print "receive children path event: type=%s" % event.type
        print "children:%s" % children
        for child in children:
            print '%s:%s' % (type(child), child)
            child_path = '/'.join((WATCH_PATH, child))
            data, stat = client.get(child_path)
            print 'data:%s, stat:%s' % (data, stat)
            obj = json.loads(data)
            print 'obj:%s' % obj["state"]
            if obj["state"] == 'online':
                print '%s' % obj["addr"]

print "%s" % client.client_state
client.start()
print "%s" % client.client_state

time.sleep(100)
