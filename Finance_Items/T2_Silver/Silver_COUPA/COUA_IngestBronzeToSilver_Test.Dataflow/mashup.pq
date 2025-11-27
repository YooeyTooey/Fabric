[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_invoice_header_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared bronze_invoice_header = let
  Source = Lakehouse.Contents([]),
  #"Navigation 1" = Source{[workspaceId = "cb65a0a4-f1b1-4adf-aaed-7cbff0148ef8"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "2dcbfe8e-d9e8-4137-95fe-44a4db0795aa"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "bronze_invoice_header", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
shared bronze_invoice_header_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "cb65a0a4-f1b1-4adf-aaed-7cbff0148ef8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "cbdc82e1-4401-48a5-91dd-1264d23fb23b"]}[Data],
  TableNavigation = Navigation_2{[Id = "silver_invoice_header", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
