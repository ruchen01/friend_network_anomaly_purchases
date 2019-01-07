import codecs
import json
import os
import numpy as np
import time
import heapq


class Person(object):

    def __init__(self, person_id, friends=None, purchases=None, network=None):
        self.person_id = person_id
        if friends:
            self.friends = friends
        else:
            self.friends = {}
        if purchases:
            self.purchases = purchases
        else:
            self.purchases = []      
        if network:
            self.network = network
        else:
            self.network = set()
            
    def GetFriends(self):
        return set(self.friends.keys())   
        
    def GetID(self):
        return self.person_id

    def AddFriend(self, person):
        self.friends[person.GetID()] = person   
        
    def UnFriend(self, person):
        self.friends.pop(person.GetID(), None)
    
    def Purchase(self, purchase_count, amount):
        self.purchases.append((purchase_count, amount))
        return self.purchases
    
    def Network(self, people, D):
        self.network = set()
        work_list = [(self.person_id, 0)]
        idx = 0
        while idx < len(work_list):
            current = work_list[idx]
            idx += 1
            if not current[0] in self.network and current[1] < D:
                for friend in people[current[0]].GetFriends():
                    work_list.append((friend, current[1] + 1))
            self.network.add(current[0])
        self.network.remove(self.person_id)
        #return self.network
    
    def Network_purchases(self, people, T):
        
        purchase=[]
        for friend_id in self.network:
            if len(people[friend_id].purchases):
                purchase.extend(people[friend_id].purchases)
                
        s = [x[0] for x in purchase]
        heapq.heapify(s)
        tmp = []
        for _ in range(T): tmp.append(heapq.heappop(s))
        tmp2 = dict(purchase)
        recent_purchases = [tmp2[key] for key in tmp]                
                
        if len(recent_purchases) >=2: 
            self.network_mean = np.mean(recent_purchases)
            self.network_std = np.std(recent_purchases)
            return self.network_mean, self.network_std 
     

     


def test_anomaly(amount, network_mean, network_std):
    if amount >= network_mean + 3 * network_std:
        return True
    return False

def jsonWrite_anomaly(file_path, line, mean, std):
    line.update({'mean': round(mean,2), 'sd': round(std,2)})
    with open(file_path, 'a') as f:
        json.dump(line, f)
        f.write('\n')

#----------------------------------------
def run_batch(path):
    people = {}
    purchase_count = 0
    path = os.getcwd() + '/'
        
    with codecs.open(path + 'batch_log.json','rU','utf-8') as f:
    
            
        for num, line in enumerate(f):
            each = json.loads(line)
            if num == 0:
                if 'D' not in each and 'T' not in each:
                    print('I can not find D and T in the batch log!!!')
                    print('returning default values D=2 and T=50')
                    D=2
                    T=50
                else:
                    D = int(each['D'])
                    T = int(each['T'])
            else:            
                if each['event_type']=='purchase':
                    purchase_count += 1
                    if each['id'] not in people:
                        people[each['id']] = Person(each['id'])
                    people[each['id']].Purchase(purchase_count, float(each['amount']))
        
                if each['event_type']=='befriend':
                    if each['id2'] not in people:
                        people[each['id2']] = Person(each['id2'])
                    if each['id1'] not in people:
                        people[each['id1']] = Person(each['id1'])
        
                    people[each['id1']].AddFriend(people[each['id2']])
                    people[each['id2']].AddFriend(people[each['id1']])
                    
                if each['event_type']=='unfriend':
                    if each['id2'] not in people:
                        people[each['id2']] = Person(each['id2'])
                    if each['id1'] not in people:
                        people[each['id1']] = Person(each['id1'])
        
                    people[each['id1']].UnFriend(people[each['id2']])
                    people[each['id1']].UnFriend(people[each['id2']])
    return D, T, purchase_count

    
def run_stream(path, D, T, purchase_count, friend_list, purchase_tot):
    people = {}
    path_anomaly = path + 'flagged_purchases.json'
    open(path_anomaly, 'w').close()
        
    with codecs.open(path + 'stream_log.json','rU','utf-8') as f:
        for line in f:
            if line == '\n':
                break
            else: 
                each = json.loads(line)     
            
                if each['event_type']=='purchase':
                    if each['id'] not in people:
                        people[each['id']] = Person(each['id'])
                    people[each['id']].Network(people, D)
                    m, sd = people[each['id']].Network_purchases(people, T)
                    if test_anomaly(float(each['amount']), m, sd) == True:
                        jsonWrite_anomaly(path_anomaly, each, m, sd)  
                
                    purchase_count += 1
                    people[each['id']].Purchase(purchase_count, float(each['amount']))                       
                        
                if each['event_type']=='befriend':
                    if each['id2'] not in people:
                        people[each['id2']] = Person(each['id2'])
                    if each['id1'] not in people:
                        people[each['id1']] = Person(each['id1'])
        
                    people[each['id1']].AddFriend(people[each['id2']])
                    people[each['id2']].AddFriend(people[each['id1']])
                    
                if each['event_type']=='unfriend':
                    if each['id2'] not in people:
                        people[each['id2']] = Person(each['id2'])
                    if each['id1'] not in people:
                        people[each['id1']] = Person(each['id1'])
        
                    people[each['id1']].UnFriend(people[each['id2']])
                    people[each['id1']].UnFriend(people[each['id2']])        
       

    




def main():
    path = os.getcwd() + '/'
    D, T, purchase_count= run_batch(path)
    run_stream(path, D, T, purchase_count)
    
if __name__ == "__main__":
    t = time.time()
    main()            
    elapsed = time.time() - t
    print('time', elapsed)            
