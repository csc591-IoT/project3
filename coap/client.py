import asyncio
import aiocoap
import aiocoap.resource as resource
import os
import time
import csv

async def fetch_file(uri, save_path=None):
    """
    Fetches the file from the given CoAP URI and prints the size and time taken.
    Returns: (transfer_time, file_size, throughput_Bps, token_len, overhead_ratio_est)
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
            throughput = file_size / transfer_time if transfer_time > 0 else 0.0

            # Save the file 
            if save_path:
                os.makedirs(os.path.dirname(save_path), exist_ok=True)
                with open(save_path, 'wb') as f:
                    f.write(response.payload)
                print(f"Saved to {save_path}")

            # token length + a tiny single-response overhead estimate (no blocksize assumption)
            token_len = len(response.token or b"")
            # fixed header(4) + token + payload marker(1) + small Content-Format hint(~2)
            overhead_bytes_est = 4 + token_len + 1 + 2
            overhead_ratio_est = (file_size + overhead_bytes_est) / file_size if file_size > 0 else float("nan")

            # NOTE: return order now (time, file_size, throughput, token_len, overhead)
            return transfer_time, file_size, throughput, token_len, overhead_ratio_est
        else:
            print("The code was not success")
            return None, None, None, None, None

    except Exception as e:
        print("Error while receiving the data", e)
        return None, None, None, None, None

async def run_experiment(uri, num_transfers, file_name, file_path_to_save=None):
    print(f"\nStarting experiment: {file_name} x {num_transfers} transfers")

    times = []
    throughputs = []
    file_sizes = []

    for i in range(num_transfers):
        transfer_time, file_size, throughput, token_len, overhead_ratio = await fetch_file(uri, file_path_to_save)

        if transfer_time is not None:
            times.append(transfer_time)
            throughputs.append(throughput)
            file_sizes.append(file_size)

            # write one row immediately 
            rows_buffer.append([
                file_name,
                i + 1,
                transfer_time,
                file_size,
                throughput,
                token_len,
                overhead_ratio
            ])

    avg_time = sum(times) / len(times) if times else float("nan")
    avg_throughput = sum(throughputs) / len(throughputs) if throughputs else float("nan")

    print(f"Completed: Avg time={avg_time:.4f}s, Avg throughput={avg_throughput:.2f} bytes/s")

    return times, throughputs, file_sizes

# buffer to collect rows between experiments (so we only open CSV once)
rows_buffer = []

async def main():
    server_ip = "172.20.10.3:5683"

    # quick warmup (optional)
    await fetch_file(f'coap://{server_ip}/100B')

    csv_filename = "result.csv"
    with open(csv_filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # NEW: two extra columns at the end
        writer.writerow(['File', 'Transfer#', 'Time(s)', 'FileSize(bytes)', 'Throughput(bytes/s)',
                         'TokenLen(bytes)', 'Overhead_Est_Ratio'])

        experiments = [
            (f'coap://{server_ip}/100B', 10000, "100B"),
            (f'coap://{server_ip}/10KB', 1000, "10KB"),
            (f'coap://{server_ip}/1MB', 100, "1MB"),
            (f'coap://{server_ip}/10MB', 10, "10MB")
        ]

        for uri, num_transfers, file_name in experiments:
            await run_experiment(uri, num_transfers, file_name)
            # flush rows for this experiment
            writer.writerows(rows_buffer)
            rows_buffer.clear()

if __name__ == "__main__":
    asyncio.run(main())
