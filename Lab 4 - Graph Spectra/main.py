import numpy as np
from scipy import sparse
from scipy import linalg
from sklearn.preprocessing import normalize
from sklearn.cluster import KMeans
from matplotlib import pyplot as plt
import networkx


def load_data(fileName):
    file = open("./data/" + fileName + ".txt")
    from_edges = []
    to_edges = []
    for line in file.readlines():
        edge = line.split(",")
        from_edge = edge[0]
        to_edge = edge[1]
        from_edges.append(int(from_edge) - 1)
        to_edges.append(int(to_edge.strip("\n"))-1)

    return from_edges, to_edges

def create_adjacent_matrix(from_edges, to_edges):
    max_ids = np.max(np.maximum(from_edges, to_edges))
    value = [1] * len(from_edges)
    adjacent_matrix = sparse.csr_matrix((value, (from_edges, to_edges)), shape=(max_ids + 1, max_ids + 1)).toarray()
    adjacent_matrix = np.where(adjacent_matrix==2, 1, adjacent_matrix)

    return adjacent_matrix

def get_fiedler_vector(L):
    v, D = np.linalg.eig(L)

    second_smallet_eigenvalue_index = np.argsort(v)[1]
    fiedler_vector = D[:, second_smallet_eigenvalue_index]

    return np.sort(fiedler_vector)

def get_Y(L, k):
    v, D = np.linalg.eig(L)

    k_largest_eigenvalue_index = np.argsort(v)[range(-1, -k-1, -1)]
    X = D[:, k_largest_eigenvalue_index]

    Y = normalize(X, axis=1)

    return Y

def get_symmetric_normalized_laplacian(adjacent_matrix):
    D = np.diag(np.sum(adjacent_matrix, axis=1))
    L = np.dot(np.dot((np.linalg.inv(np.sqrt(D))), adjacent_matrix), (np.linalg.inv(np.sqrt(D))))

    return L

def cluster_network(from_edges, to_edges, Y):
    kmeans = KMeans(n_clusters=k).fit(Y)

    vertexes = set()
    edges = set()
    for (from_edge, to_edge) in zip(from_edges, to_edges):
        edges.add(tuple((from_edge, to_edge)))
        vertexes.update([from_edge, to_edge])

    G = networkx.Graph()
    G.add_nodes_from(vertexes)
    G.add_edges_from(edges)

    networkx.draw(G, node_size=15, node_color=kmeans.labels_)
    plt.title("Graph clusters")
    plt.show()

    return G

def get_laplacian_matrix(adjacent_matrix):
    D = np.diag(np.sum(adjacent_matrix, axis=1))
    laplacian_matrix = D - adjacent_matrix

    return laplacian_matrix

# Init
k = 4

# Load data and create adjacent matrix
from_edges, to_edges = load_data("example1")
adjacent_matrix = create_adjacent_matrix(np.array(from_edges), np.array(to_edges))

# Create two types of L matrices
symmetric_normalized_laplacian = get_symmetric_normalized_laplacian(adjacent_matrix)
laplacian_matrix = get_laplacian_matrix(adjacent_matrix)

# Get Y vector
Y = get_Y(symmetric_normalized_laplacian, k)

# Get field vector
fiedler_vector = get_fiedler_vector(laplacian_matrix)

# Plot and cluster graph
G = cluster_network(from_edges, to_edges, Y)

# Plot fiedler vector and sparsity pattern
plt.plot(fiedler_vector)
plt.title("Fiedler Vector")
plt.show()
plt.matshow(adjacent_matrix)
plt.title("Sparsity Pattern")
plt.show()

