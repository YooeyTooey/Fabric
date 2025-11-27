[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "B1PBAS_FY25_Upload_SSIS_RAW_20250930_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared B1PBAS_FY25_Upload_SSIS_RAW_20250930 = let
  Source = Sql.Databases("10.204.32.134"),
  #"Navigation 1" = Source{[Name = "DW_PBAS"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "dbo", Item = "B1PBAS_FY25_Upload_SSIS_RAW_20251016"]}[Data]
in
  #"Navigation 2";
shared B1PBAS_FY25_Upload_SSIS_RAW_20250930_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "cb65a0a4-f1b1-4adf-aaed-7cbff0148ef8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1bc7e1c8-5772-47d5-9649-4d937d1d7450"]}[Data],
  TableNavigation = Navigation_2{[Id = "Bronze_PBAS_Test", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
