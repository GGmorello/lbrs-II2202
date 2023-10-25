import pickle

data_source = "nyc"

prefix = "_test"
spatial_graph_orig_fname = data_source + "_spatial.edgelist"

# POIs that have the same 4 letter hash prefix
# are in the same cell, and should have edges between them
def create_spatial_dump():
    spatial_graph_curr_fname = data_source + "_spatial" + prefix + ".edgelist"
    fname_hashes = data_source + "_geohash2poi_4.pickle"
    f_hashes = open(fname_hashes, 'rb')
    hashes_dict = pickle.load(f_hashes)
    pairs_dict = dict()
    for item in hashes_dict.items():
        k = item[0]
        vals = item[1]
        pairs_dict[k] = vals
    
    edges = list()
    for key in pairs_dict.keys():
        pairs = pairs_dict[key]
        l = len(pairs)
        for i in range(l):
            item1 = pairs[i]
            for j in range(i, l):
                item2 = pairs[j]
                edges.append((item1,item2))
    edges.append((0,0))
    f_spatial = open(spatial_graph_curr_fname, 'w')
    for edge in edges:
        s = str(edge[0]) + " " + str(edge[1]) + " {}\n"
        f_spatial.write(s)
    print("dumped spatial graph in file", spatial_graph_curr_fname)

def verify():
    f_spatial = open(spatial_graph_curr_fname, 'r')
    list_curr = list()
    for row in f_spatial.readlines():
        s = row.split(" ")
        list_curr.append((s[0], s[1]))
    
    f_orig = open(spatial_graph_orig_fname, 'r')
    list_orig = list()
    for row in f_orig.readlines():
        s = row.split(" ")
        list_orig.append((s[0], s[1]))
    
    # TODO why is 0 included in original?
    if len(list_curr) != len(list_orig):
        print("length of orig/curr differs", len(list_orig), len(list_curr))
    
    for i in range(len(list_curr)):
        curr = list_orig[i]
        orig = list_orig[i]
        if curr[0] != orig[0]:
            print("from edge differes between orig/curr", orig, curr)
        if curr[1] != orig[1]:
            print("to edge differs between orig/curr", orig, curr)

    print("verified contents of file", spatial_graph_curr_fname)

if __name__ == "__main__":
    if data_source == "nyc":
        prefix = ""
    create_spatial_dump()
    if data_source != "nyc":
        verify()