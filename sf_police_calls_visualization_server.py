import http.server
import socketserver
from urllib import parse
from http import HTTPStatus
from hashlib import sha256
from gevent import os

import sys
sys.path.append('c:/prog/python/scimple/scm')
import scimple as scm
from scimple import pyspark_utils
from pyspark.sql.functions import col
class Plotter:
    """
    cache same requests
    """
    sc, spark = pyspark_utils.contexts()
    df = spark.read.option('header', 'true').option('separator', ',').csv("police-department-incidents_.csv")
    df.createGlobalTempView("calls")
    svg_pool = set()
    def __init__(self):
        raise RuntimeError("Singleton can't be instanciated")

    @staticmethod
    def plot_over_is_time_stamp(string_plot_over):
        return string_plot_over in {"Year", "YearAndMonth", "DayOfMonth", "DayOfWeek", "Hour"}
    @staticmethod
    def getSvgFromQueryDict(query_dict):
        hashed = sha256(str(query_dict).encode("utf-8")).hexdigest()
        print(hashed)
        if hashed in Plotter.svg_pool:
            return hashed
        elif query_dict["query_type"] == "plot":
            title=f"Plot over {query_dict['plot_over']}"
            WHERE = ""
            conditions_list = []
            Category = ""
            if "type" in query_dict:
                Category = f"Type==\"{query_dict['type']}\""
                conditions_list.append(Category)
            elif "category" in query_dict:
                Category = f"Category==\"{query_dict['category']}\""
                conditions_list.append(Category)

            Resolution = ""
            if "resolution" in query_dict:
                Resolution = f"Resolution==\"{query_dict['resolution']}\""
                conditions_list.append(Resolution)
            if len(conditions_list):
                title += " with " + " and ".join(conditions_list)
                WHERE = "WHERE " + " AND ".join(conditions_list)
            if not Plotter.plot_over_is_time_stamp(query_dict["plot_over"]):
                ORDERBY = "ORDER BY count(*) DESC"
            else:
                ORDERBY = f"ORDER BY {query_dict['plot_over']}"
            sql_query = f"""SELECT {query_dict["plot_over"]}, count(*) FROM global_temp.calls
                    {WHERE} 
                    GROUP BY {query_dict["plot_over"]} {ORDERBY}"""
            print(sql_query)
            query_res = Plotter.spark.sql(sql_query).collect()
            if not Plotter.plot_over_is_time_stamp(query_dict["plot_over"]):
                plot = scm.Plot(title=title).add(query_res, 0, 1, marker="bar")
            else:
                plot = scm.Plot(title=title).add(query_res, 0, 1, marker=".", markersize=3, colored_area=0.3)

            plot.save_as_svg(file_name=hashed, dir_name='plots', aspect=2.2)

            Plotter.svg_pool.add(hashed)
            return hashed
        elif query_dict["query_type"] == "map":
            print("OK")
            return "test"


PORT = 8000

class SFPoliceCallsVisualizationServer(http.server.SimpleHTTPRequestHandler):
    def send_head(self):
        path = self.translate_path(self.path)
        f = None
        parts = parse.urlsplit(self.path)
        query_dict = parse.parse_qs(parts.query)
        query_dict = {key: query_dict[key][0] for key in query_dict}
        if os.path.isdir(path):
            if not parts.path.endswith('/'):
                # redirect browser - doing basically what apache does
                self.send_response(HTTPStatus.MOVED_PERMANENTLY)
                new_parts = (parts[0], parts[1], parts[2] + '/',
                             parts[3], parts[4])
                new_url = parse.urlunsplit(new_parts)
                self.send_header("Location", new_url)
                self.end_headers()
                return None
            for index in "index.html", "index.htm":
                index = os.path.join(path, index)
                if os.path.exists(index):
                    path = index
                    break
            else:
                return self.list_directory(path)
        ctype = self.guess_type(path)
        try:
            f = open(path, 'rb')
        except OSError:
            self.send_error(HTTPStatus.NOT_FOUND, "File not found")
            return None
        try:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-type", ctype)
            fs = os.fstat(f.fileno())
            self.send_header("Content-Length", str(fs[6]))
            self.send_header("Last-Modified", self.date_time_string(fs.st_mtime))
            if "query_type" in query_dict:
                print(query_dict, parts)
                img_name = Plotter.getSvgFromQueryDict(query_dict)
                self.send_header('img', f'<img src="plots/{img_name}.svg" width="1000px"></img>')
            self.end_headers()
            return f
        except:
            f.close()
            raise
    def do_GET(self):
        """Serve a GET request."""
        f = self.send_head()
        if f:
            try:
                self.copyfile(f, self.wfile)
            except:
                f.close()



Handler = SFPoliceCallsVisualizationServer

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print("serving at port", PORT)
    httpd.serve_forever()