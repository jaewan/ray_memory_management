import numpy as np
import subprocess
import json
import os

CHUNK_SIZE_GB = 1  # Size of each chunk in gigabytes
DATA_SIZE_GB = 60  # Total size of data in gigabytes

def create_data():
    # Number of float32 numbers in each chunk
    chunk_len = (CHUNK_SIZE_GB * 1024**3) // 4
    # Total number of chunks to write
    num_chunks = DATA_SIZE_GB // CHUNK_SIZE_GB

    filename = "testfile"

    # Remove the file if it exists
    if os.path.exists(filename):
        os.remove(filename)

    # Create and write the chunks
    for _ in range(num_chunks):
        data = np.random.rand(chunk_len).astype(np.float32)
        data.tofile(filename)

def run_fio():
    cmd = ["fio", "--name=/ray_spill/testfile", "--ioengine=sync", "--rw=write", "--direct=0",
           "--bs=4k", "--size=60g", "--numjobs=4", "--output-format=json"]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print("An error occurred while running fio:")
        print(result.stderr)
        return

    fio_output = json.loads(result.stdout)

    #read_bw = fio_output['jobs'][0]['read']['bw'] / 1024
    write_bw = fio_output['jobs'][0]['write']['bw'] / 1024

    #print("Read bandwidth: {} MB/s".format(read_bw))
    print("Write bandwidth: {} MB/s".format(write_bw))

if __name__ == "__main__":
    #create_data()
    run_fio()
