[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_invoice_header_incremental_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared bronze_invoice_header_incremental = let
    // STEP 1: Get Access Token
    token_url = "https://wvi-stage.coupahost.com/oauth2/token",
    token_body = Text.ToBinary(
        "grant_type=client_credentials" &
        "&client_id=cbde988f092e68aafda32e0aa32b807c" &
        "&client_secret=da85e23be5396f5dc4a1a1ec8dabd4807f07ea7f1064f7039119959434eee12c" &
        "&scope=core.invoice.read"
    ),
    token_response = Json.Document(Web.Contents(
        token_url,
        [
            Content = token_body,
            Headers = [
                #"Content-Type" = "application/x-www-form-urlencoded",
                #"Accept" = "application/json"
            ]
        ]
    )),
    access_token = token_response[access_token],

    // STEP 2: Call Coupa /suppliers
    api_url = "https://wvi-stage.coupahost.com/api/invoices",
    supplier_data_raw = Web.Contents(
        api_url,
        [
            Headers = [
                Authorization = "Bearer " & access_token,
                #"Accept" = "application/json",
                #"Content-Type" = "application/json"
            ]
        ]
    ),
    json_response = Json.Document(supplier_data_raw),

    // STEP 3: Convert List of Records into Table
    coupa_table = Table.FromRecords(json_response),
  #"Expanded invoice-lines" = Table.ExpandListColumn(coupa_table, "invoice-lines"),
  #"Expanded payments" = Table.ExpandListColumn(#"Expanded invoice-lines", "payments"),
  #"Expanded created-by" = Table.ExpandRecordColumn(#"Expanded payments", "created-by", {"login", "fullname"}, {"login", "fullname"}),
  #"Expanded updated-by" = Table.ExpandRecordColumn(#"Expanded created-by", "updated-by", {"email", "fullname"}, {"email", "fullname.1"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded updated-by", {{"email", "updated-by-login"}, {"fullname.1", "updated-by-name"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"tax-lines", "withholding-tax-lines", "tcs-tax-lines", "current-integration-history-records", "tags", "taggings", "failed-tolerances", "dispute-reasons", "payment-agreement-notes", "pay-invoice"}),
  #"Expanded custom-fields" = Table.ExpandRecordColumn(#"Removed columns", "custom-fields", {"lco-t5-code", "lco-t8-code"}, {"lco-t5-code", "lco-t8-code"}),
  #"Expanded currency" = Table.ExpandRecordColumn(#"Expanded custom-fields", "currency", {"code"}, {"code"}),
  #"Expanded attachments" = Table.ExpandListColumn(#"Expanded currency", "attachments"),
  #"Expanded approvals" = Table.ExpandListColumn(#"Expanded attachments", "approvals"),
  #"Expanded invoice-charges" = Table.ExpandListColumn(#"Expanded approvals", "invoice-charges"),
  #"Expanded invoice-payment-receipts" = Table.ExpandListColumn(#"Expanded invoice-charges", "invoice-payment-receipts"),
  #"Removed columns 1" = Table.RemoveColumns(#"Expanded invoice-payment-receipts", {"printer-location", "invoice-charges", "invoice-lines", "attachments"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns 1", {{"id", type text}, {"created-at", type text}, {"updated-at", type text}, {"comments", type text}, {"compliant", type text}, {"handling-amount", type text}, {"internal-note", type text}, {"invoice-date", type text}, {"delivery-number", type text}, {"delivery-date", type text}, {"invoice-number", type text}, {"line-level-taxation", type text}, {"misc-amount", type text}, {"shipping-amount", type text}, {"status", type text}, {"total-with-taxes", type text}, {"gross-total", type text}, {"supplier-total", type text}, {"supplier-note", type text}, {"discount-due-date", type text}, {"net-due-date", type text}, {"discount-amount", type text}, {"tax-code", type text}, {"tax-rate", type text}, {"tax-amount", type text}, {"tolerance-failures", type text}, {"paid", type text}, {"payment-date", type text}, {"payment-notes", type text}, {"exported", type text}, {"canceled", type text}, {"tax-amount-engine", type text}, {"tax-code-engine", type text}, {"tax-rate-engine", type text}, {"last-exported-at", type text}, {"image-scan", type text}, {"image-scan-url", type text}, {"supplier-created", type text}, {"early-payment-provisions", type text}, {"margin-scheme", type text}, {"cash-accounting-scheme-reference", type text}, {"exchange-rate", type text}, {"late-payment-penalties", type text}, {"credit-reason", type text}, {"origin-currency-net", type text}, {"taxes-in-origin-country-currency", type text}, {"origin-currency-gross", type text}, {"pre-payment-date", type text}, {"self-billing-reference", type text}, {"reverse-charge-reference", type text}, {"discount-percent", type text}, {"credit-note-differences-with-original-invoice", type text}, {"original-value-of-supply", type text}, {"correct-value-of-supply", type text}, {"customs-declaration-number", type text}, {"customs-office", type text}, {"customs-declaration-date", type text}, {"payment-order-reference", type text}, {"payment-order-number", type text}, {"advance-payment-received-amount", type text}, {"lock-version-key", type text}, {"series", type text}, {"folio-number", type text}, {"use-of-invoice", type text}, {"form-of-payment", type text}, {"type-of-receipt", type text}, {"payment-method", type text}, {"issuance-place", type text}, {"confirmation", type text}, {"original-invoice-date", type text}, {"withholding-tax-override", type text}, {"clearance-document", type text}, {"coupa-accelerate-status", type text}, {"type-of-relationship", type text}, {"invoice-reference-number", type text}, {"dispute-method", type text}, {"date-received", type text}, {"sender-email", type text}, {"inbox-name", type text}, {"amount-due", type text}, {"tax-due-to-supplier", type text}, {"customer-accounting-tax", type text}, {"payment-channel", type text}, {"show-tax-information", type text}, {"document-type", type text}, {"original-invoice-number", type text}, {"date-of-discovery-of-facts-decisive-for-correction", type text}, {"place-of-supply", type text}, {"split-payment-mechanism", type text}, {"gross-total-less-discount", type text}, {"net-total-less-discount", type text}, {"total-taxes-less-discount", type text}, {"customer-accounting-tax-less-discount", type text}, {"amount-due-less-discount", type text}, {"endorsement-on-invoices", type text}, {"new-means-of-transport", type text}, {"place-of-issuance", type text}, {"amount-of-advance-payment", type text}, {"transaction-uuid", type text}, {"transaction-notification-date", type text}, {"content-validation", type text}, {"supplier-invoice-issuer-name", type text}, {"supplier-invoice-reviewer-name", type text}, {"supplier-payment-collector-name", type text}, {"signed-qr-code", type text}, {"archive-entity-id", type text}, {"invoice-issuance-time", type text}, {"cash-register-operator", type text}, {"means-of-payment", type text}, {"unique-identification-code-of-cash-receipt", type text}, {"security-code-of-issuer", type text}, {"state-tax-number", type text}, {"state-tax-number-for-substitute-taxpayer", type text}, {"municipal-tax-number", type text}, {"serial-code-of-fiscal-invoice", type text}, {"verification-code", type text}, {"type-of-document", type text}, {"protocol-number", type text}, {"nature-of-operation", type text}, {"type-of-operation", type text}, {"freight-type", type text}, {"vehicle-license-plate", type text}, {"national-enrollment-of-conveyor", type text}, {"volume-amount", type text}, {"volume-gross-weight", type text}, {"volume-liquid-weight", type text}, {"volume-brand", type text}, {"volume-type", type text}, {"volume-numbering", type text}, {"image-scan-content-type", type text}, {"image-scan-file-name", type text}, {"image-scan-file-size", type text}, {"is-credit-note", type text}, {"channel", type text}, {"currency-id", type text}, {"line-count", type text}, {"invoice-description", type text}, {"hub-entity", type text}, {"national-office-po", type text}, {"gl-date", type text}, {"gpo-processing-fee", type text}, {"bpo", type text}, {"date-rs", type text}, {"lco-t5-code", type text}, {"lco-t8-code", type text}, {"payments", type text}, {"code", type text}, {"invoice-payment-receipts", type text}, {"approvals", type text}, {"login", type text}, {"fullname", type text}, {"updated-by-login", type text}, {"updated-by-name", type text}})

in
    #"Changed column type";
shared bronze_invoice_header_incremental_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "cb65a0a4-f1b1-4adf-aaed-7cbff0148ef8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "2dcbfe8e-d9e8-4137-95fe-44a4db0795aa"]}[Data],
  TableNavigation = Navigation_2{[Id = "bronze_invoice_header_incremental", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
