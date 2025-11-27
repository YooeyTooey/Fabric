[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "CY Transaction Details_CE_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"CY Transaction Details_CE" = let
  Source = PowerPlatform.Dataflows([]),
  #"Navigation 1" = Source{[Id = "Workspaces"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[workspaceId = "2dd63507-a290-46f9-95fa-2414773a6538"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[dataflowId = "7e076c5b-82f9-473a-8eeb-09baf87b739a"]}[Data],
  Navigation = #"Navigation 3"{[entity = "CY_US Transaction Details", version = ""]}[Data]
in
  Navigation;
shared #"CY Transaction Details_CE_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "cb65a0a4-f1b1-4adf-aaed-7cbff0148ef8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c39d0feb-b253-47c3-afd9-29b725d43c5d"]}[Data],
  TableNavigation = Navigation_2{[Id = "Bronze_SUN_Test", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
