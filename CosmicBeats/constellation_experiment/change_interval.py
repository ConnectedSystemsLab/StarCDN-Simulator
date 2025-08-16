import sys
data = []
prev_time = None
with open(sys.argv[1], 'r') as fr:
    with open(sys.argv[2], 'w') as fw:
        for line in fr:
            time, id, size = line.strip().split(":")
            time = int(time)
            if prev_time is None:
                prev_time = time
            if time > prev_time:
                assert len(data) >= 4
                sub_len = int(len(data) / 4)
                for i in range(4):
                    for j in range(sub_len):
                        item = data[0]
                        fw.write(f'{item[1] + int(15 * i)}:{item[0]}:{item[2]}\n')
                        data.pop(0)
                while len(data) > 0:
                    fw.write(f'{item[1] + 45}:{item[0]}:{item[2]}\n')
                    data.pop(0)
                prev_time = time
            data.append([id, time, size])
        
        if len(data) > 0:
            sub_len = int(len(data) / 4)
            for i in range(4):
                for j in range(sub_len):
                    item = data[0]
                    fw.write(f'{item[1] + int(15 * i)}:{item[0]}:{item[2]}\n')
                    data.pop(0)
            while len(data) > 0:
                fw.write(f'{item[1] + 45}:{item[0]}:{item[2]}\n')
                data.pop(0)





