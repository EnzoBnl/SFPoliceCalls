import http.server
import socketserver
from urllib import parse
from http import HTTPStatus
from hashlib import sha256
from gevent import os
import matplotlib.image as mim
import sys
sys.path.append('c:/prog/python/scimple')
import scm.scimple as scm
from scm.scimple import pyspark_utils

from os import mkdir
from os.path import isdir

class Plotter:
    """
    cache same requests
    """
    sc, spark = pyspark_utils.contexts()
    df = spark.read.option('header', 'true').option('separator', ',').csv("./data/police-department-incidents_.csv")
    df.createGlobalTempView("calls")
    svg_pool = set()
    if not isdir('./plots'): mkdir('./plots')
    def __init__(self):
        raise RuntimeError("Singleton can't be instanciated")

    @staticmethod
    def plot_over_is_time_stamp(string_plot_over):
        return string_plot_over in {"Year", "YearAndMonth", "DayOfMonth", "Hour", "Month"}
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
            if "type" in query_dict:
                Category = f"Type==\"{query_dict['type']}\""
                conditions_list.append(Category)
            elif "category" in query_dict:
                Category = f"Category==\"{query_dict['category']}\""
                conditions_list.append(Category)

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
                plot = scm.Plot().add(query_res, 0, 1, marker="bar")
            else:
                plot = scm.Plot().add(query_res, 0, 1, marker=".", markersize=3, colored_area=0.3)
            plot.axe.set_title(title, fontsize=7)
            plot.save_as_svg(file_name=hashed, dir_name='plots', aspect=2.7)

            Plotter.svg_pool.add(hashed)
            return hashed
        elif query_dict["query_type"] == "map":
            title = f"{query_dict['plot_over']} calls"
            AND = ""
            conditions_list = []
            if "type" in query_dict:
                Category = f"Type==\"{query_dict['type']}\""
                conditions_list.append(Category)
            elif "category" in query_dict:
                Category = f"Category==\"{query_dict['category']}\""
                conditions_list.append(Category)

            if "resolution" in query_dict:
                Resolution = f"Resolution==\"{query_dict['resolution']}\""
                conditions_list.append(Resolution)
            if len(conditions_list):
                title += " with " + " and ".join(conditions_list)
                AND = "AND " + " AND ".join(conditions_list)
            date_start, date_end = query_dict['plot_over'].split(",")
            sql_query = f"""SELECT FLOAT(X), FLOAT(Y) FROM global_temp.calls
                            WHERE "{date_start}" <= SUBSTRING(Date, 0, 10) AND SUBSTRING(Date, 0, 10) <= "{date_end}"
                                {AND}"""
            print(sql_query)
            query_res = Plotter.spark.sql(sql_query).collect()
            plot = scm.Plot()
            plot.axe.imshow(mim.imread("./imgs/sf.png"))
            plot.axe.set_xticks([])
            plot.axe.set_yticks([])
            plot.add(list(map(lambda e: ((e[0] + 122.44) * 2700 + 277, -1 * ((e[1] - 37.76) * 3320 - 243)), query_res)), 0,
                  1, marker=".", markersize=2, colored_by="#dd8888")
            plot.axe.set_title(title, fontsize=7)
            plot.save_as_svg(file_name=hashed, dir_name='plots')
            Plotter.svg_pool.add(hashed)
            return hashed


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
                self.send_header('img', f'<img src="plots/{img_name}.svg" width="{1000 if query_dict["query_type"] == "plot" else 515}px"></img>')
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


# TODO: From Date to Date map !, cumulative plot if no category selected , gmap plot boronoi commisariat
# virer OUTLAYER !!! -175 000