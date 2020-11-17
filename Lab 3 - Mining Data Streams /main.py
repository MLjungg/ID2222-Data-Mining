import numpy

def reservoir_sampling(edge, time):
    updated = False
    for i in range(edge_res_size):
        x = numpy.random.uniform(0,1)
        if x < (1/time):
            edge_res[i] = edge
            updated = True

    return updated

def get_stream():
    # TODO: Make it random
    print("Started loading stream..\n")
    file = open("./data/web-BerkStan.txt", "r")
    print("Finished loading stream..\n")

    return file

def edge_close_wedge(edge, wedge, time):
    # To be implemented

def get_new_wedges(new_edge):
    # Hur många wedges finns det bland vår sample av edges?
    # Asumtions: if the new edge is already present in edge_reservoir no new wedges will be created.

    new_wedges = []
    for edge in edge_res:
        if edge == new_edge or edge == tuple([new_edge[1], new_edge[0]]):
            pass

        else:
            if edge[0] == new_edge[1]:
                new_wedges.append(tuple([edge[0], new_edge[0], new_edge[1]]))

            if edge[1] == new_edge[0]:
                new_wedges.append(tuple([new_edge[0], edge[0], edge[1]]))

    return new_wedges

def update(edge, time):
    for i in range(wedge_res_size):
        if edge_close_wedge(edge, wedge_res[i], time):
            isClosed[i] = True
    sample_updated = reservoir_sampling(edge, time)
    if sample_updated:
        new_wedges = get_new_wedges(edge)
        tot_wedges = len(new_wedges) #TODO: This should be addition +=.

if __name__ == "__main__":

    # Get stream
    stream = get_stream()

    # Initialize variables
    edge_res_size = 200
    wedge_res_size = 200
    tot_wedges = 0
    time = 0

    # Streaming-Triangle algorithm
    edge_res = numpy.zeros(edge_res_size)
    wedge_res = numpy.zeros(wedge_res_size)
    isClosed = numpy.zeros(wedge_res_size)

    for edge in stream.readlines():
        time = time + 1
        update(edge, time)
