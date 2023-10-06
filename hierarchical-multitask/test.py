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

    # TODO what is geohash? internal ID? hash of lon+lat?
    file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_allData.pickle'
    # file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_beamSearchHashDict.pickle'
    # file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_data.pickle'
    # file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_geohash2Index_2.pickle'
    # file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_geohash2poi_2.pickle'
    file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_globalIndex2latlon.pickle'
    # file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_poi2geohash_6.pickle'
    # file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_poi2index.pkl'
    # file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_poi2timeslotList.pickle'
    file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_poiCount.pickle'
    # file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_test.pkl'
    # file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_testData.pickle'
    # file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_timeSlot2Index.pickle'
    # file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_timeslot2POIlist.pickle'
    # file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_train.pkl'
    # file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_user2index.pickle'
    # file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_userCount.pickle'
    # file = 'data/' + arg['dataFolder'] + '/' + dataSource + '_usersData.pickle'

    with open(file, 'rb') as handle:
        pickleData = pickle.load(handle)
        print(pickleData)
        # print(len(pickleData[1]))
        # print(len(pickleData))