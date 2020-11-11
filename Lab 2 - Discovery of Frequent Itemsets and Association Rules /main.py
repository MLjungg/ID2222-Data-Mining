import itertools

def a_priori_pass(baskets, similarity_threshold):
    count = {}
    frequent_items = []

    # first pass
    for basket in baskets:
        for item in basket.split(" "):
            if not item in count:
                count[item] = 1
            else:
                count[item] += 1

    # second pass
    for basket in baskets:
        singletons = []
        for item in basket.split(" "):
            if count[item] > similarity_threshold:
                singletons.append(item)

        pairs = list(itertools.combinations(singletons, 2))
        for pair in pairs:
            if not pair in count:
                count[pair] = 1
            else:
                count[pair] +=1

                if count[pair] > similarity_threshold and pair not in frequent_items:
                    frequent_items.append(pair)

    return frequent_items


def load_data():
    data = []
    file = open("./data/T10I4D100K.dat", "r")
    for basket in file.readlines():
        data.append(basket[0:-2])

    return data


def main():
    similarity_threshold = 1000

    baskets = load_data()
    frequent_items = a_priori_pass(baskets, similarity_threshold)
    for item in frequent_items:
        print(item)
    print("Done")


main()
