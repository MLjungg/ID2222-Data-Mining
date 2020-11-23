import random
print("Started loading data..\n")
data = []
file = open("./data/web-Stanford.txt", "r")
for basket in file.readlines():
    data.append(basket)

shuffled_data = random.sample(data[4:], len(data)-4)
print("Finished loading data..\n")
new_file = open("./data/web-Stanford_shuffled.txt", "w")
for line in shuffled_data:
    new_file.write(line)
