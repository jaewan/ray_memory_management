import socket
import time

def start_server():
    host = '10.138.0.2'
    port = 6378
    buffer_size = 4096  # Define the buffer size

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        print('Server started at', host, 'on port', port)
        s.listen()
        conn, addr = s.accept()
        with conn:
            print('Connected by', addr)
            
            # Receive data in chunks
            start_time = time.time()
            received_bytes = 0
            with open('received_data.npy', 'wb') as f:
                while True:
                    data = conn.recv(buffer_size)
                    if not data:
                        break
                    f.write(data)
                    received_bytes += len(data)

            end_time = time.time()

    print('File received successfully')

    time_taken = end_time - start_time  # Time taken in seconds
    bandwidth = received_bytes / time_taken / (1024**2)  # Calculate bandwidth in MB/s

    print('Time taken: {} seconds'.format(time_taken))
    print('Bandwidth: {} MB/s'.format(bandwidth))

if __name__ == '__main__':
    start_server()