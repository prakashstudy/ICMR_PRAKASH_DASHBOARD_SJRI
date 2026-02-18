/**
 * Google Apps Script to receive data from Dash and append/update a Google Sheet.
 * 
 * Instructions:
 * 1. Open Google Sheet.
 * 2. Extensions > Apps Script.
 * 3. Paste this code.
 * 4. Deploy > New Deployment > Web App.
 * 5. Set "Who has access" to "Anyone".
 * 6. Copy the URL and paste it into app.py as EXCEL_WRITE_URL.
 */

function doPost(e) {
    try {
        var data = JSON.parse(e.postData.contents);
        var ss = SpreadsheetApp.getActiveSpreadsheet();
        var sheet = ss.getSheetByName("Dashboard_Sync") || ss.insertSheet("Dashboard_Sync");

        if (data.length === 0) {
            return ContentService.createTextOutput(JSON.stringify({ "status": "success", "message": "No data to process" })).setMimeType(ContentService.MimeType.JSON);
        }

        // 1. Get existing Sheet structure and headers
        var lastRow = sheet.getLastRow();
        var rawHeaders = lastRow > 0 ? sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0] : [];
        var sheetHeaders = rawHeaders.map(function (h) { return String(h).trim(); });

        // --- NEW: Smart Header Expansion ---
        // If incoming data has keys not in our sheet, add them automatically
        if (data.length > 0) {
            var incomingKeys = Object.keys(data[0]);
            var headersAdded = false;
            incomingKeys.forEach(function (key) {
                if (sheetHeaders.indexOf(key) === -1) {
                    sheetHeaders.push(key);
                    rawHeaders.push(key);
                    headersAdded = true;
                }
            });

            if (headersAdded || lastRow === 0) {
                // Write updated (or initial) headers to Row 1
                sheet.getRange(1, 1, 1, sheetHeaders.length).setValues([rawHeaders]);
                if (lastRow === 0) lastRow = 1;
            }
        }

        // 2. Identify "ID" column index in the sheet
        var idColIndex = sheetHeaders.indexOf("ID");
        if (idColIndex === -1) {
            return ContentService.createTextOutput(JSON.stringify({ "status": "error", "message": "ID column not found in Sheet headers" })).setMimeType(ContentService.MimeType.JSON);
        }

        // 3. Map existing IDs to row numbers for fast lookup
        var existingIds = {};
        if (lastRow > 1) {
            var idValues = sheet.getRange(2, idColIndex + 1, lastRow - 1, 1).getValues();
            for (var i = 0; i < idValues.length; i++) {
                var val = idValues[i][0];
                if (val !== undefined && val !== "") {
                    existingIds[String(val).trim()] = i + 2;
                }
            }
        }

        var updateCount = 0;
        var addCount = 0;
        var newRows = [];

        // 4. Process each incoming record
        for (var i = 0; i < data.length; i++) {
            var item = data[i];
            var id = item["ID"] ? String(item["ID"]).trim() : null;

            // Construct row based on SHEET headers (mapped to incoming keys)
            var rowValues = rawHeaders.map(function (h) {
                var cleanHeader = String(h).trim();
                return (item[cleanHeader] !== undefined) ? item[cleanHeader] : "";
            });

            if (id && existingIds[id]) {
                // UPDATE existing patient
                sheet.getRange(existingIds[id], 1, 1, rawHeaders.length).setValues([rowValues]);
                updateCount++;
            } else {
                // NEW patient (Append later)
                newRows.push(rowValues);
                addCount++;
            }
        }

        // 5. Batch append new rows
        if (newRows.length > 0) {
            sheet.getRange(sheet.getLastRow() + 1, 1, newRows.length, sheetHeaders.length).setValues(newRows);
        }

        return ContentService.createTextOutput(JSON.stringify({
            "status": "success",
            "message": "Processed " + data.length + " records. Added: " + addCount + ", Updated: " + updateCount
        })).setMimeType(ContentService.MimeType.JSON);

    } catch (err) {
        return ContentService.createTextOutput(JSON.stringify({ "status": "error", "message": err.toString() }))
            .setMimeType(ContentService.MimeType.JSON);
    }
}

function doGet(e) {
    return ContentService.createTextOutput("Script is active. Send a POST request to sync data.");
}

const TEMPLATE_ID = "12tTQ2iHKjRtuA9jAldgx5QzZXSU-hENv5jbQTB13HH8";
const OUTPUT_FOLDER_ID = "13pIetfXk91AOnJtbdzKpFbY0rwKz8viB";

/**
 * Triggered manually or by a time trigger to send PDF reports.
 */
function processAndSendEmails() {
    // 1. Get a script lock to prevent concurrent executions
    const lock = LockService.getScriptLock();
    try {
        // Wait for up to 30 seconds for the lock
        lock.waitLock(30000);
    } catch (e) {
        Logger.log("Could not obtain lock: " + e.toString());
        return;
    }

    try {
        const ss = SpreadsheetApp.getActiveSpreadsheet();
        const sheet = ss.getSheetByName("Dashboard_Sync");
        if (!sheet) return;

        // Use getValues() to get fresh data from the sheet
        const dataRange = sheet.getDataRange();
        const data = dataRange.getDisplayValues();
        const headers = data[0];

        // Find necessary column indices by name
        const statusCol = headers.indexOf("Status") + 1;
        const emailCol = headers.indexOf("Email");
        const nameCol = headers.indexOf("Name");
        const idCol = headers.indexOf("ID");

        if (statusCol === 0 || emailCol === -1 || nameCol === -1 || idCol === -1) {
            Logger.log("Critical columns (Status, Email, Name, or ID) missing.");
            return;
        }

        const template = DriveApp.getFileById(TEMPLATE_ID);
        const folder = DriveApp.getFolderById(OUTPUT_FOLDER_ID);

        // Map existing files in output folder to avoid re-generating
        const existingFiles = {};
        const files = folder.getFiles();
        while (files.hasNext()) {
            const file = files.next();
            existingFiles[file.getName()] = file;
        }

        for (let i = 1; i < data.length; i++) {
            // RE-READ the status for this specific row to catch updates from previous locked runs
            const currentStatus = sheet.getRange(i + 1, statusCol).getValue();
            if (currentStatus === "SENT" || !data[i][emailCol]) continue;

            const row = data[i];
            let record = {};
            headers.forEach((h, j) => record[h] = row[j]);

            const fileName = record.ID + "_Report";
            let pdf = null;

            try {
                // 2. Check if PDF already exists in the folder
                const existingPdfName = fileName + ".pdf";
                let reportFile;

                if (existingFiles[fileName]) {
                    Logger.log("Report Doc already exists for ID: " + record.ID + ". Using existing.");
                    reportFile = existingFiles[fileName];
                } else {
                    // Create report
                    Logger.log("Generating new report for ID: " + record.ID);
                    const copy = template.makeCopy(fileName, folder);
                    const doc = DocumentApp.openById(copy.getId());
                    const body = doc.getBody();

                    headers.forEach(h => {
                        body.replaceText("{{" + h + "}}", record[h] || "");
                    });

                    doc.saveAndClose();
                    reportFile = copy;
                    // Add to our map so we don't try to make it again in same run if ID repeats
                    existingFiles[fileName] = reportFile;
                }

                // Get PDF
                pdf = reportFile.getAs(MimeType.PDF);

                // Send email
                GmailApp.sendEmail(
                    record.Email,
                    "Lab Report - " + record.Name,
                    "Dear " + record.Name + ",\n\nPlease find your lab report attached.\n\nRegards,\nLab Team",
                    { attachments: [pdf] }
                );

                // Mark as sent
                sheet.getRange(i + 1, statusCol).setValue("SENT");
                Logger.log("Successfully sent email for ID: " + record.ID);

            } catch (e) {
                Logger.log("Error sending email for ID " + record.ID + ": " + e.toString());
            }
        }
    } finally {
        // Release the lock
        lock.releaseLock();
    }
}
