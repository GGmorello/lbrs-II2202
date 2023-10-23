import os
import sys
from utils.func import *
from tqdm import tqdm
from torch.optim import Adam
from torch.utils.data import DataLoader
import time
import networkx as nx
from model import *
import random
import pandas

if __name__ == '__main__': 
    arg = {}

    arg['dataFolder'] = "processedFiles"

    dataSource = "global_scale"


    def view_file(file):
        with open(file, 'rb') as handle:
            allData = pickle.load(handle)
        with open(file.split('.')[0] + '.vis', 'w') as handle:
            handle.write(str(allData))

    # TODO what is geohash? internal ID? hash of lon+lat?
    file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_allData.pickle'
    view_file(file)
    file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_beamSearchHashDict.pickle'
    view_file(file)
    file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_data.pickle'
    view_file(file)
    file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_geohash2Index_4.pickle'
    view_file(file)
    file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_geohash2poi_2.pickle'
    view_file(file)
    file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_globalIndex2latlon.pickle'
    view_file(file)
    file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_poi2geohash_6.pickle'
    view_file(file)
    file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_poi2index.pkl'
    view_file(file)
    file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_poi2timeslotList.pickle'
    view_file(file)
    file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_poiCount.pickle'
    view_file(file)
    file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_test.pkl'
    view_file(file)
    file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_testData.pickle'
    view_file(file)
    file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_timeSlot2Index.pickle'
    view_file(file)
    file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_timeslot2POIlist.pickle'
    view_file(file)
    file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_train.pkl'
    view_file(file)
    file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_user2index.pickle'
    view_file(file)
    file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_userCount.pickle'
    view_file(file)
    file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_usersData.pickle'
    view_file(file)

    with open(file, 'rb') as handle:
        pickleData = pickle.load(handle)
        print(pickleData)
        # print(len(pickleData[1]))
        # print(len(pickleData))