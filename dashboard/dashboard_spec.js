describe("Dashboard", function() {
  var dashboard;
  var data_url = "report_fixtures.json";

  beforeEach(function() {
    dashboard = new Dashboard();
  });

  it("should call the transformation method when json is received", function () {
    spyOn($, "getJSON").andCallFake(function(options) {
      options.success();
    });
    dashboard.get_report(data_url);
    expect(dashboard.transform).toHaveBeenCalled();
  });

  it("should transform labeled report data into x/y datapoints", function() {
    raw_data = dashboard.get_report(data_url);
    transformed_data = dashboard.transform(raw_data)
  });
});
