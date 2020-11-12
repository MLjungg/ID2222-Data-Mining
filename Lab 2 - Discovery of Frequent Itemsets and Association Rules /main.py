import itertools


def a_priori_algorithm(baskets):
    # Init variables
    count = {}
    similarity_threshold = 500

    # first pass (n=1)
    for basket in baskets:
        for item in basket:
            item = tuple([item])
            if not item in count:
                count[item] = 1
            else:
                count[item] += 1

    # second pass (n --> infinity)
    stored_itemsets = n_pass(baskets, similarity_threshold, count)

    return stored_itemsets


def n_pass(baskets, similarity_threshold, count):
    n = 1
    new_itemsets = True    # Stores information about if we shall look for a higher order of itemset.
    stored_itemsets = {}
    while new_itemsets:
        n = n + 1
        stored_itemsets[n] = set()
        new_itemsets = False
        for index, basket in enumerate(baskets):
            frequent_itemset = []
            for item in basket:
                if n == 2:
                    if count[tuple([item])] > similarity_threshold:
                        frequent_itemset.append(item)
                else:
                    if count[item] > similarity_threshold:
                        frequent_itemset.append(item)
            itemsets = generate_itemsets(frequent_itemset, count, n, similarity_threshold)
            if len(itemsets) > 0:
                stored_itemsets[n].update(itemsets)
                new_itemsets = True
            baskets[index] = itemsets
            for itemset in itemsets:
                if not itemset in count:
                    count[itemset] = 1
                else:
                    count[itemset] += 1

    return stored_itemsets


def generate_itemsets(frequent_itemset, count, n, similarity_threshold):
    # This function generates valid n+1 itemsets given a n-itemset.
    # E.g input [1,2] [2,3] [3,4] --> Which triples can we make?

    valid_itemsets = []
    if n == 2:  # Trivial case, all pairs are valid.
        return list(itertools.combinations(frequent_itemset, 2))
    else:
        itemsets = itertools.combinations(frequent_itemset, n)
        for itemset in itemsets:
            valid_itemset = check_frequent(itemset, similarity_threshold, count)
            if valid_itemset:
                valid_itemset, set_singletons = higher_order_validation(itemset, n) # set_singletons return the n+1-itemset.

                # If still valid, store the new itemset
                if valid_itemset:
                    valid_itemsets.append(tuple(sorted(set_singletons)))

        return valid_itemsets


def check_frequent(itemset, similarity_threshold, count):
    #  This fuctions checks if respective itemset that constitutes the new itemset is frequent --> eg: if we have [1,2,3] check if [1,2] [2,3] [1,3] is frequent.
    valid_itemset = True
    for items in itemset:
        if count[items] < similarity_threshold:
            valid_itemset = False

    return valid_itemset


def higher_order_validation(itemset, n):
    # This function check if the new itemset can be constructed from the current itemset --> eg. to create the triple [1,2,3] the following pairs most exist: [1,2], [2,3], [1,3]
    valid_itemset = True
    singletons = []
    for items in itemset:
        for singleton in items:
            singletons.append(singleton)
    set_singletons = set(singletons)
    for singleton in set_singletons:
        if singletons.count(singleton) != n - 1:  # E.g for the triple [1,2,3] to be valid each singleton needs to exist 3-1 times.
            valid_itemset = False
            break

    if valid_itemset:
        return valid_itemset, set_singletons
    else:
        return valid_itemset, []


def load_data():
    data = []
    file = open("./data/T10I4D100K.dat", "r")
    for basket in file.readlines():
        data.append(basket[0:-2].split(" "))

    return data


def main():
    baskets = load_data()
    frequent_itemsets = a_priori_algorithm(baskets)  # This return all
    print("Done")


main()
