import socket

def read_from_socket(conn) -> tuple[str, bytearray]:
    """ 
    Receive customed protocol from sockets
    """
    # Read 4 bytes verb and payload size
    data = conn.recv(8)
    if len(data) == 0:
        return None, None
    verb = data[:4].decode()
    payload_size = int.from_bytes(data[4:], byteorder='big') 
    data_read_in = 0
    data = bytes()
    while len(data) < payload_size:
        data += conn.recv(102400)
    return verb, data

def write_to_socket(conn, verb: str, data: str):
    """
    Send a packet with specified verb and payload to socket
    """
    header = verb.encode() + len(data).to_bytes(4, byteorder='big') 
    conn.sendall(header + data.encode())

def send_request(host: str, port: int, verb: str, data: str):
    """
    Establish a connection and send packet. 
    Connection will not wait for response.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.connect((host, port)) 
        write_to_socket(s, verb, data)
        s.shutdown(socket.SHUT_RDWR)

def send_request_wait_response(host: str, port: int, verb: str, data: str) -> tuple[str, bytearray]:
    """
    Establish a connection and wait for response.
    @return
        verb: str
        data: bytearray
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.connect((host, port)) 
        write_to_socket(s, verb, data)
        ret = read_from_socket(s)
        s.shutdown(socket.SHUT_RDWR)
        return ret
