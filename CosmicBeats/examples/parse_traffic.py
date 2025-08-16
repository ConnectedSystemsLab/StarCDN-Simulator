import ast, sys
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print(f'usage: python3 parse_traffic.py INPUT_DIR OUTPUT_DIR')
        sys.exit(0)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    with open(input_file, 'r') as f:
        with open(output_file, 'w') as fw:
            for line in f:
                if '[Requests]' in line:
                    requests = ast.literal_eval(line[line.rfind(':') + 1 : line.rfind(']') + 1]) 
                    for r in requests:
                        fw.write(f'{r[0]}:{r[1]}:{r[2]}\n')