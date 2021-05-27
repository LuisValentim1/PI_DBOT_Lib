import time
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

class Cache:

    def __init__(self, cacheSize):
        self.cachedElements = dict()
        self.timeout = 100000
        self.size = cacheSize
    
    def add(self, user, session):
        if user not in cachedElements:
            if len(cachedElements) < self.size:
                self.cachedElements[user] = [session, time.time()]
            else:
                self.cachedElements.pop(self.cachedElements.keys()[0])
                self.cachedElements[user] = [session, time.time()]
        else: 
            self.cachedElements[user][1] = time.time()

    def get(self, user):
        if self.exists(user):
            return [user, self.cachedElements[user][0]]
        else:
            return None

    def exists(self, user):
        if user in self.cachedElements:
            if time.time() - self.cachedElements[user][1] < self.timeout:
                return True
            else:
                self.cachedElements.pop(user)
        return False
             

