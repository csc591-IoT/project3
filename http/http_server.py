from http.server import SimpleHTTPRequestHandler, HTTPServer
import os

DATA_DIR = "./DataFiles"
PORT = 8000

os.chdir(DATA_DIR)


class CustomHandler(SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        return


def main():
    server = HTTPServer(("", PORT), CustomHandler)
    print(f"Serving files from {os.path.abspath(DATA_DIR)} on port {PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")


if __name__ == "__main__":
    main()
