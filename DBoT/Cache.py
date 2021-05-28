import time
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

class Cache:

    def __init__(self, cacheSize):
        self.cachedElements = dict()
        self.timeout = 3000
        self.size = cacheSize
    
    def add(self, user, session):
        if user not in self.cachedElements:
            if len(self.cachedElements) < self.size:
                self.cachedElements[user] = [session, time.time()]
            else:
                self.cachedElements.pop(list(self.cachedElements.keys())[0])
                self.cachedElements[user] = [session, time.time()]
        else: 
            self.cachedElements[user][1] = time.time()

    def get(self, user):
        if self.valid(user):
            return [user, self.cachedElements[user][0]]
        else:
            return None

    def valid(self, user):
        if user in self.cachedElements:
            if time.time() - self.cachedElements[user][1] < self.timeout:
                return True
            else:
                self.cachedElements.pop(user)
        return False
             

