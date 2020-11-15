import itertools


def a_priori_algorithm(baskets, similarity_threshold):
    # Initate a dict to keep track of occurrence count of respective itemset.
    count = {}

    # first pass (n=1)
    for basket in baskets:
        for item in basket:
            item = tuple([item])
            if not item in count:
                count[item] = 1
            else:
                count[item] += 1

    # second pass (n --> infinity)
    frequent_itemsets, count = n_pass(baskets, similarity_threshold, count)

    return frequent_itemsets, count


def n_pass(baskets, similarity_threshold, count):
    n = 1                   # Length of itemset
    new_itemsets = True     # Stores information about if we shall look for a higher order of itemset.
    frequent_itemsets = {}    # Key = length of itemset, value = all frequent valid itemsets of length key.
    while new_itemsets:
        n = n + 1
        frequent_itemsets[n-1] = set()  # Previous n-itemset
        new_itemsets = False
        for index, basket in enumerate(baskets):
            frequent_itemset_in_basket = []
            for item in basket:
                if n == 2:
                    if count[tuple([item])] > similarity_threshold:
                        frequent_itemset_in_basket.append(item)
                else:
                    if count[item] > similarity_threshold:
                        frequent_itemset_in_basket.append(item)

            higher_order_itemsets = generate_itemsets(frequent_itemset_in_basket, count, n, similarity_threshold)
            if len(higher_order_itemsets) > 0:
                frequent_itemsets[n-1].update(frequent_itemset_in_basket)
                new_itemsets = True
            baskets[index] = higher_order_itemsets
            for itemset in higher_order_itemsets:
                if not itemset in count:
                    count[itemset] = 1
                else:
                    count[itemset] += 1

    return frequent_itemsets, count


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
                    valid_itemsets.append(tuple(sorted(set_singletons, key=int)))

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

def find_rules(frequent_itemsets, support, c):
    association_rules = []

    # iterates itemsets of all sizes
    for itemsets_size in frequent_itemsets:
        if itemsets_size != 1: # If not considering single items.
            for itemset in frequent_itemsets[itemsets_size]:
                subsets = generate_subsets(itemset)
                for subset in subsets:
                    confidence = support[itemset] / support[subset]
                    if confidence > c:
                        association_rules.append([subset, tuple(set(itemset).difference(set(subset))), confidence])
    return sorted(association_rules, key=lambda x: x[2], reverse=True)


def generate_subsets(itemset):
    subsets = set()
    for r in range(1, len(itemset)):
        subsets.update(list(itertools.combinations(itemset, r)))
    return subsets


def main():
    baskets = load_data()
    similarity_threshold = 700
    c = 0.9
    frequent_itemsets, support = a_priori_algorithm(baskets, similarity_threshold)
    association_rules = find_rules(frequent_itemsets, support, c)
    print("Done!")


main()
