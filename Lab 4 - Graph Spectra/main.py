import numpy as np
from scipy import sparse
from scipy.sparse.linalg import eigs

def load_data():
    file = open("./data/example1.txt")
    from_edges = []
    to_edges = []
    for line in file.readlines():
        from_edge, to_edge = line.split(",")
        from_edges.append(int(from_edge))
        to_edges.append(int(to_edge.strip("\n")))

    return to_edges, from_edges

def create_adjacent_matrix(from_edges, to_edges):
    max_ids = np.max(np.maximum(from_edges, to_edges))
    value = [1] * len(from_edges)
    adjacent_matrix = sparse.csr_matrix((value, (from_edges, to_edges)), shape=(max_ids + 1, max_ids + 1)).toarray()    # +1 is probably because of index starts with 0.
    return adjacent_matrix

def get_eigenvalues(adjacent_matrix):
    v, D = eigs(adjacent_matrix, k=len(adjacent_matrix[0]))
    second_smallest_eigenvalue_index = np.where(np.sort(v)[1] == v)[0][0] # TODO: Why is eigenvalues imaginary?
    field_vector = D[:, second_smallest_eigenvalue_index]

    return field_vector

from_edges, to_edges = load_data()
adjacent_matrix = create_adjacent_matrix(from_edges, to_edges)
eigenvalues = get_eigenvalues(adjacent_matrix, )