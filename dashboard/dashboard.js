
function Dashboard(){}

Dashboard.prototype.get_report = function (url) {
    $.getJSON(url, this.transform);     
};

Dashboard.prototype.transform = function (raw_data) {
  
};




