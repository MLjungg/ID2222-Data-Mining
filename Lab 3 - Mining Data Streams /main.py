import numpy


def get_stream():
    print("Started loading stream..\n")
    file = open("./data/web-Stanford_shuffled.txt", "r")
    print("Finished loading stream..\n")

    return file

def reservoir_sampling_edge(edge, time):
    updated = False
    x = numpy.random.uniform(0, 1)
    if x < 1 - numpy.power((1 - (1 / time)), edge_res_size):
        index = numpy.random.randint(0, edge_res_size)
        edge_res[index] = edge
        updated = True

    return updated

def reservoir_sampling_wedge(wedges, tot_wedges):
    q = len(wedges) / tot_wedges
    for i in range(wedge_res_size):
        x = numpy.random.uniform(0, 1)
        if x < q:
            new_wedge_index = numpy.random.randint(0, len(wedges))
            wedge_res[i] = wedges.pop(new_wedge_index)
            isClosed[i] = False
            if len(wedges) == 0:
                return


def get_new_wedges(new_edge):
    # Hur många wedges finns det bland vår sample av edges?
    # Asumtions: if the new edge is already present in edge_reservoir no new wedges will be created.

    new_wedges = []
    indices = numpy.where(edge_res[:, 1] == new_edge[0])
    for idx in indices[0]:
        new_wedges.append([edge_res[idx][0], new_edge[0], new_edge[1]])

    indices = numpy.where(edge_res[:, 0] == new_edge[1])
    for idx in indices[0]:
        new_wedges.append([new_edge[0], edge_res[idx][0], edge_res[idx][1]])

    return new_wedges

def set_closed():
    candidate_closed_indices = numpy.where(wedge_res[:, 0] == edge[1])
    candidates_closed_indices2 = numpy.where(wedge_res[:, 2] == edge[0])
    closed_indices = numpy.intersect1d(candidate_closed_indices, candidates_closed_indices2)
    for closed_idx in closed_indices:
        isClosed[closed_idx] = True

def update(edge, time, tot_wedges):
    set_closed()
    sample_updated = reservoir_sampling_edge(edge, time)
    if sample_updated:
        new_wedges = get_new_wedges(edge)
        if len(new_wedges) > 0:
            tot_wedges += len(new_wedges)
            reservoir_sampling_wedge(new_wedges, tot_wedges)

    return tot_wedges


# Get stream
stream = get_stream()

# Initialize variables
edge_res_size = 20000
wedge_res_size = 20000
tot_wedges = 0
time = 0

# Streaming-Triangle algorithm
edge_res = numpy.zeros((edge_res_size, 2))
wedge_res = numpy.zeros((wedge_res_size, 3))
isClosed = numpy.zeros(wedge_res_size)

for edge in stream.readlines():
    time = time + 1
    edge = numpy.array(edge.strip("\n").split("\t"), dtype=int)
    tot_wedges = update(edge, time, tot_wedges)

    if (time % 100000) == 0:
        print(time)
        print("The number of wedges is: " + str(tot_wedges))

        p = numpy.sum(isClosed) / numpy.shape(isClosed)
        k = 3 * p
        t = ((p * numpy.power(time, 2)) / (edge_res_size * (edge_res_size - 1))) * tot_wedges
        print("Number of triangle after " + str(time) + " processed edges is " + str(t[0]))

p = numpy.sum(isClosed) / numpy.shape(isClosed)
k = 3 * p
t = ((p * numpy.power(time, 2)) / (edge_res_size * (edge_res_size - 1))) * tot_wedges
print("The total number of triangle are approximated to " + str(t[0]))
