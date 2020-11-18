import numpy


def reservoir_sampling_edge(edge, time):
    updated = False
    x = numpy.random.uniform(0, 1)
    if x < 1 - numpy.power((1 - (1 / time)), edge_res_size):
        index = numpy.random.random_integers(0, edge_res_size - 1)
        edge_res[index] = edge
        updated = True

    return updated

def reservoir_sampling_wedge(wedges, tot_wedges):
    q = len(wedges) / tot_wedges
    for i in range(wedge_res_size):
        x = numpy.random.uniform(0, 1)
        if x < q:
            new_wedge_index = numpy.random.random_integers(0, len(wedges) - 1)
            wedge_res[i] = wedges.pop(new_wedge_index)
            isClosed[i] = False
            if len(wedges) == 0:
                return

def get_stream():
    # TODO: Make it random
    print("Started loading stream..\n")
    file = open("./data/web-BerkStan_shuffled.txt", "r")
    print("Finished loading stream..\n")

    return file


def edge_close_wedge(edge, wedge, time):
    # To be implemented
    pass


def get_new_wedges(new_edge):
    # Hur många wedges finns det bland vår sample av edges?
    # Asumtions: if the new edge is already present in edge_reservoir no new wedges will be created.

    new_wedges = []
    for edge in edge_res:
        if edge == 0:  # If edge is empty.
            pass

        elif edge == new_edge or edge == tuple([new_edge[1], new_edge[0]]):  # If same edge, no new wedge
            pass

        else:
            if edge[1] == new_edge[0]:
                new_wedges.append(tuple([edge[0], new_edge[0], new_edge[1]]))

            if edge[0] == new_edge[1]:
                new_wedges.append(tuple([new_edge[0], edge[0], edge[1]]))

    return new_wedges

def find_triangles(edge):
    # Finds all wedges that are closed by edge. Closed wedge is a Triangle.
    for i in range(wedge_res_size):
        # Assuming directed graph
        if edge[0] == wedge_res[i][2] and edge[1] == wedge_res[i][0]:
            isClosed[i] = True

def update(edge, time, tot_wedges):
    find_triangles(edge)
    for i in range(wedge_res_size):
        if edge_close_wedge(edge, wedge_res[i], time):
            isClosed[i] = True
    sample_updated = reservoir_sampling_edge(edge, time)
    if sample_updated:
        new_wedges = get_new_wedges(edge)
        if len(new_wedges) > 0:
            tot_wedges += len(new_wedges)
            reservoir_sampling_wedge(new_wedges, tot_wedges)

# Get stream
stream = get_stream()

# Initialize variables
edge_res_size = 5000
wedge_res_size = 5000
tot_wedges = 0
time = 0

# Streaming-Triangle algorithm
edge_res = numpy.zeros(edge_res_size, dtype=object)
wedge_res = numpy.zeros(wedge_res_size, dtype=object)
isClosed = numpy.zeros(wedge_res_size)

for edge in stream.readlines():
    time = time + 1
    edge = tuple(edge.strip("\n").split("\t"))  # Make it a tuple (fromNode, toNode)
    update(edge, time, tot_wedges)

closed_edges = numpy.sum(isClosed)/numpy.shape(isClosed)


