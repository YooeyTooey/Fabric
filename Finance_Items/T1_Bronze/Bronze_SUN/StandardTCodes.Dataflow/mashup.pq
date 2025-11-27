[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "SUNEXTRACT_ANALYSIS_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared SUNEXTRACT_ANALYSIS = let
  Source = Sql.Databases("10.204.32.133"),
  #"Navigation 1" = Source{[Name = "Sun6Extract"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "dbo", Item = "SUNEXTRACT_ANALYSIS"]}[Data]
in
  #"Navigation 2";
shared SUNEXTRACT_ANALYSIS_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "cb65a0a4-f1b1-4adf-aaed-7cbff0148ef8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c39d0feb-b253-47c3-afd9-29b725d43c5d"]}[Data],
  TableNavigation = Navigation_2{[Id = "Bronze_SUN_Analysis", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
