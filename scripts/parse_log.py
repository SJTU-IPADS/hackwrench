import sys
import os
import functools
def compareConfig(c1, c2):
    client1 = int(c1.split("-")[1].split("client")[0])
    client2 = int(c2.split("-")[1].split("client")[0])
    if client1 <= client2:
        return -1
    else :
        return 1

if len(sys.argv) > 1:
    path = sys.argv[1]
else:
    print("lacks log path!")

configs = []
for config in os.listdir(path):
    configs.append(config)
configs = sorted(configs, key=functools.cmp_to_key(compareConfig))

hashmap = {}
namemap = {}
files = []
for config in configs:
    files.append(os.path.join(path, config, "db.log"))

max_i = 0
i = 0
for filename in files:
    file = open(filename)
    lines = file.readlines()
    for line in lines:
        if "timer.h:405" in line:
            elements = line.split(" ")
            if elements[1] in hashmap:
                max_i = max(max_i, int(elements[1]))
                hashmap[elements[1]][i] = [int(elements[3]), int(elements[4]), int(elements[5])]
            else:
                dictionary = dict()
                dictionary[i] = [int(elements[3]), int(elements[4]), int(elements[5])]
                hashmap[elements[1]] = dictionary
                namemap[elements[1]] = elements[2]
    i += 1

for i in hashmap.keys():
    key = namemap[i]
    dictionary = hashmap[i]
    s = ""
    for j in range(len(files)):
        if j in dictionary.keys():
            s += " " + ' '.join(map(str, dictionary[j]))
        else:
            s += " 0 0 0"
    # for num in hashmap[i]:
    #     s += " " + str(num)
    print(key, s)
