import http.server
import socketserver

PORT = 8000

class SFPoliceCallsVisualizationServer(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        super().do_GET()


Handler = SFPoliceCallsVisualizationServer

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print("serving at port", PORT)
    httpd.serve_forever()