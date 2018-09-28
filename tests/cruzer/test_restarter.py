from concurrent.futures import ThreadPoolExecutor



def misc():
    path = '/Volumes/crypt/_Coding/PYTHON/penetration/wordpress/Duplicator/MEMORY_DEBUG_1.txt'

    lines = [int(line.strip().split('|')[2].strip()) for line in open(str(path)) if 'seen_set' in line]
    delta_ls = []
    print(lines)
    for index,num in enumerate(lines):
        if index == 0:
            continue

        percent = round((float(num)/float(lines[index-1]))*100) - 100
        delta_ls.append(percent)

    print(delta_ls)


if __name__ == '__main__':
    misc()
