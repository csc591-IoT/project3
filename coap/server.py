import asyncio
import aiocoap
import aiocoap.resource as resource
import os
from datetime import datetime

class FileResource(resource.Resource):
    """Represents a COAP resource.

    Args:
        file_path (str): The path to the file to be served.
    """
    def __init__(self, file_path):
        super().__init__()
        self.file_path = file_path
        self.request_count = 0
        self.context_cache = None 
        
    async def render_get(self, request):
        self.request_count += 1
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"\n[{timestamp}] Incoming GET request")
        print(f"Request Count: {self.request_count}")
        
        try:    
            with open(self.file_path, 'rb') as f: # Read the file if found
                self.context_cache = f.read()
            file_size = len(self.context_cache)
            
            print(f"Sending {file_size}, {file_size/1024:.2f}KB")
            
            return aiocoap.Message(
                code=aiocoap.Code.CONTENT,
                payload=self.context_cache
            )
        except FileNotFoundError:
            return aiocoap.Message(code=aiocoap.Code.NOT_FOUND)
        except Exception as e:
            return aiocoap.Message(code=aiocoap.Code.INTERNAL_SERVER_ERROR)
        
async def main():
    # Resource tree creation 
    root = resource.Site()
    
    # Now lets add file to the resource tree with proper endpoint path 
    hundred_byte_file_path = "../files/100B"
    ten_kb_file_path = "../files/10KB"
    ten_mb_file_path = "../files/10MB"
    one_mb_file_path = "../files/1MB"

    root.add_resource(['100B'], FileResource(hundred_byte_file_path)) # coap://host/100B
    root.add_resource(['10KB'], FileResource(ten_kb_file_path)) # coap://host/10KB
    root.add_resource(['10MB'], FileResource(ten_mb_file_path)) # coap://host/10MB
    root.add_resource(['1MB'], FileResource(one_mb_file_path)) # coap://host/1MB

    await aiocoap.Context.create_server_context(root)
    await asyncio.get_running_loop().create_future() 
    
if __name__ == "__main__":
    print("Starting CoAP server...")
    asyncio.run(main()) 
    print("CoAP server shutdown.")