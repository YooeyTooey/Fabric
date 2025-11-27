CREATE   PROCEDURE dbo.sp_COUPA_upsert_invoice_header
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @updated INT = 0, @inserted INT = 0;

    BEGIN TRY
        BEGIN TRAN;

        /* 1) update existing rows by invoice_id */
        UPDATE d
        SET
            d.invoice_id                         = s.invoice_id,
            d.created_date                       = TRY_CONVERT(DATETIME2(6), s.created_date),
            d.updated_date                       = TRY_CONVERT(DATETIME2(6), s.updated_date),
            d.delivery_method                    = s.delivery_method,
            d.invoice_date                       = TRY_CONVERT(DATETIME2(6), s.invoice_date),
            d.invoice_number                     = s.invoice_number,
            d.invoice_status                     = CASE
                                                    WHEN s.invoice_status = 'new' THEN 'New'
                                                    WHEN s.invoice_status = 'voided' THEN 'Voided'
                                                    ELSE s.invoice_status
                                                    END,
            d.total                              = TRY_CONVERT(DECIMAL(28,2), s.total),
            d.net_due_date                       = TRY_CONVERT(DATETIME2(6), s.net_due_date),
            d.paid                               = TRY_CONVERT(BIT, s.paid),
            d.invoice_receipt_date_from_supplier = TRY_CONVERT(DATETIME2(6), s.invoice_receipt_date_from_supplier),
            d.gpo_processing_fee                 = s.gpo_processing_fee,
            d.gl_date                            = TRY_CONVERT(DATETIME2(6), s.gl_date),
            d.printer_location                   = s.printer_location,
            d.national_office_po                 = s.national_office_po,
            d.invoice_description                = s.invoice_description,
            d.lco_t8_code                        = s.lco_t8_code,
            d.currency                           = s.currency,
            d.payment_term                       = s.payment_term,
            d.supplier_number                    = s.supplier_number,
            d.created_by_login                   = s.created_by_login,
            d.created_by_name                    = s.created_by_name,
            d.updated_by_login                   = s.updated_by_login,
            d.updated_by_name                    = s.updated_by_name,
            d.time_stamp                         = s.time_stamp  --CURRENT_TIMESTAMP
        FROM gold_finance.dbo.gold_coupa_invoice_header AS d
        JOIN silver_coupa_test.dbo.silver_invoice_header_incremental AS s
          ON d.invoice_id = s.invoice_id
        WHERE s.invoice_id IS NOT NULL;

        SET @updated = @@ROWCOUNT;

        /* 2) insert new rows */
        INSERT INTO gold_finance.dbo.gold_coupa_invoice_header (
            invoice_id, created_date, updated_date, delivery_method, invoice_date,
            invoice_number, invoice_status, total, net_due_date, paid,
            invoice_receipt_date_from_supplier, gpo_processing_fee, gl_date, printer_location,
            national_office_po, invoice_description, lco_t8_code, currency, payment_term,
            supplier_number, created_by_login, created_by_name, updated_by_login, updated_by_name, time_stamp
        )
        SELECT
            s.invoice_id,
            TRY_CONVERT(DATETIME2(6), s.created_date),
            TRY_CONVERT(DATETIME2(6), s.updated_date),
            s.delivery_method,
            TRY_CONVERT(DATETIME2(6), s.invoice_date),
            s.invoice_number,
            CASE
                WHEN s.invoice_status = 'new' THEN 'New'
                WHEN s.invoice_status = 'voided' THEN 'Voided'
                ELSE s.invoice_status
            END AS invoice_status,
            TRY_CONVERT(DECIMAL(28,2), s.total),
            TRY_CONVERT(DATETIME2(6), s.net_due_date),
            TRY_CONVERT(BIT, s.paid),
            TRY_CONVERT(DATETIME2(6), s.invoice_receipt_date_from_supplier),
            s.gpo_processing_fee,
            TRY_CONVERT(DATETIME2(6), s.gl_date),
            s.printer_location,
            s.national_office_po,
            s.invoice_description,
            s.lco_t8_code,
            s.currency,
            s.payment_term,
            s.supplier_number,
            s.created_by_login,
            s.created_by_name,
            s.updated_by_login,
            s.updated_by_name,
            s.time_stamp
        FROM silver_coupa_test.dbo.silver_invoice_header_incremental AS s
        WHERE s.invoice_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM gold_finance.dbo.gold_coupa_invoice_header AS d
              WHERE d.invoice_id = s.invoice_id
          );

        SET @inserted = @@ROWCOUNT;

        COMMIT;

        SELECT @updated AS updated_rows, @inserted AS inserted_rows;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        SELECT
            ERROR_NUMBER()    AS error_number,
            ERROR_SEVERITY()  AS error_severity,
            ERROR_STATE()     AS error_state,
            ERROR_PROCEDURE() AS error_procedure,
            ERROR_MESSAGE()   AS error_message;
        RETURN;
    END CATCH
END;