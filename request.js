var addQueriesDict = function(url, listQueries){
    return url + '?' + listQueries.join('&');
}

// PLOT BUTTON
var button = document.getElementById("get_plot");
button.addEventListener("click", function (ev) {
    var xhttp = new XMLHttpRequest();
    xhttp.onreadystatechange = function() {
        if (this.readyState == 4 && this.status == 200) {
            document.getElementById("plot").innerHTML = this.getResponseHeader('img') + document.getElementById("plot").innerHTML;
        }
    }
    url = addQueriesDict('/', ["query_type=plot",
                               "plot_over=" + document.getElementById("fields_list_plot").value,
                               "category=" + document.getElementById("categories_list_plot").value,
                               "type=" + document.getElementById("types_list_plot").value,
                               "resolution=" + document.getElementById("resolutions_list_plot").value])
    xhttp.open("GET", url, true);
    xhttp.send();
});

// MAP BUTTON
var button_ = document.getElementById("get_map");
button_.addEventListener("click", function (ev) {
    var xhttp = new XMLHttpRequest();
    xhttp.onreadystatechange = function() {
        if (this.readyState == 4 && this.status == 200) {
            document.getElementById("map").innerHTML = this.getResponseHeader('img') + document.getElementById("map").innerHTML;
        }
    }
    url = addQueriesDict('/', ["query_type=map",
                               "plot_over=" + document.getElementById("date").value,
                               "category=" + document.getElementById("categories_list_map").value,
                               "type=" + document.getElementById("types_list_map").value,
                               "resolution=" + document.getElementById("resolutions_list_map").value])
    xhttp.open("GET", url, true);
    xhttp.send();
});