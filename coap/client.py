import asyncio
import aiocoap
import aiocoap.resource as resource
import os
import time
import csv

async def fetch_file(uri, save_path=None):
    """
    Fetches the file from the given CoAP URI and prints the size and time taken.
    """
    
    context = await aiocoap.Context.create_client_context()
    
    request = aiocoap.Message(code=aiocoap.Code.GET, uri=uri)
    
    start_time = time.time()
    
    try:
        response = await context.request(request).response
        end_time = time.time()
        transfer_time = end_time - start_time
        print(f"The response code is: {response.code}")
        if response.code.is_successful():
            file_size = len(response.payload)
            througput = file_size / transfer_time
            
            # Save the file 
            if save_path:
                with open(save_path, 'wb') as f:
                    f.write(response.payload)
                print(f"Saved to {save_path}")
            return transfer_time, througput, file_size
        else:
            print("The code was not success")
            return None, None, None 
        
    except Exception as e:
        print("Error while receiving the data", e)
        return None, None, None 

async def run_experiment(uri, num_transfers, file_name, file_path_to_save=None):
    print(f"\nStarting experiment: {file_name} x {num_transfers} transfers")
    
    times = []
    throughputs = []
    file_sizes = []
    
    for i in range(num_transfers):
        transfer_time, file_size, throughput = await fetch_file(uri, file_path_to_save)
        
        if transfer_time is not None:
            times.append(transfer_time)
            throughputs.append(throughput)
            file_sizes.append(file_size)
    
    avg_time = sum(times) / len(times)
    avg_throughput = sum(throughputs) / len(throughputs)
    
    print(f"Completed: Avg time={avg_time:.4f}s, Avg throughput={avg_throughput:.2f} bytes/s")
    
    return times, throughputs, file_sizes
    
        
async def main():
    server_ip = "172.20.10.3:5683"
    transfer_time, file_size, throughput = await fetch_file(f'coap://{server_ip}/10MB', "transfered_files/10MBCopy")
    transfer_time, file_size, throughput = await fetch_file(f'coap://{server_ip}/10MB')
    print("This is transfered time:", transfer_time)
    
    csv_filename = "result2.csv"
    
    with open(csv_filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['File', 'Transfer#', 'Time(s)', 'FileSize(bytes)', 'Throughput(bytes/s)'])
        
        experiments = [
            (f'coap://{server_ip}/100B', 10000, "100B"),
            (f'coap://{server_ip}/10KB', 1000, "10KB"),
            (f'coap://{server_ip}/1MB', 100, "1MB"),
            (f'coap://{server_ip}/10MB', 10, "10MB")
        ]
        
        for uri, num_transfers, file_name in experiments:
            times, throughputs, file_sizes = await run_experiment(uri, num_transfers, file_name)
            
            for i in range(len(times)):
                writer.writerow([
                    file_name,
                    i + 1,
                    times[i],
                    file_sizes[i],
                    throughputs[i]
                ])

if __name__ == "__main__":
    asyncio.run(main())