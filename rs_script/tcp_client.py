import socket
import numpy as np
import time

def connect_to_server():
    host = '10.138.0.2' 
    port = 6378
    buffer_size = 4096

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))

        # Generate 20GB of float32 data
        num_elements = (20 * 1024**3) // 4

        start_time = time.time()
        for i in range(num_elements // buffer_size):
            data = np.random.rand(buffer_size).astype(np.float32).tobytes()
            s.sendall(data)
        end_time = time.time()

    print('Data sent successfully')

    time_taken = end_time - start_time  # Time taken in seconds
    sent_bytes = num_elements * 4  # 4 bytes per float32
    bandwidth = sent_bytes / time_taken / (1024**2)  # Calculate bandwidth in MB/s

    print('Time taken: {} seconds'.format(time_taken))
    print('Bandwidth: {} MB/s'.format(bandwidth))

if __name__ == '__main__':
    connect_to_server()
